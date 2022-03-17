#define DUCKDB_BUILD_LOADABLE_EXTENSION
#include "duckdb.hpp"

#include <libpq-fe.h>

#include "duckdb/function/table_function.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parallel/parallel_state.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/common/types/cast_helpers.hpp"

using namespace duckdb;

struct PostgresColumnInfo {
  string attname;
  idx_t attlen;
  char attalign;
  bool attnotnull;
  int atttypmod;
  string typname;
};

static constexpr uint32_t POSTGRES_TID_MAX = 4294967295;

struct PostgresBindData : public FunctionData {
  string schema_name;
  string table_name;
  idx_t oid = 0;
  idx_t cardinality = 0;
  idx_t pages = 0;

  idx_t page_size = 0;
  idx_t txid = 0;

  vector<PostgresColumnInfo> columns;
  vector<string> names;
  vector<LogicalType> types;

  idx_t pages_per_task = 1000;
  string dsn;

  string snapshot;

  PGconn *conn = nullptr;
  ~PostgresBindData() {
    if (conn) {
      PQfinish(conn);
      conn = nullptr;
    }
  }
};

struct PGQueryResult {

  PGQueryResult(PGresult *res_p) : res(res_p) {}
  ~PGQueryResult() {
    if (res) {
      PQclear(res);
    }
  }
  PGresult *res = nullptr;

  string GetString(idx_t row, idx_t col) {
    D_ASSERT(res);
    return string(PQgetvalue(res, row, col));
  }

  int32_t GetInt32(idx_t row, idx_t col) {
    return atoi(PQgetvalue(res, row, col));
  }
  int64_t GetInt64(idx_t row, idx_t col) {
    return atoll(PQgetvalue(res, row, col));
  }
};

struct PostgresOperatorData : public FunctionOperatorData {
  bool done = false;
  bool exec = false;
  string sql;
  vector<column_t> column_ids;
  string col_names;
  PGconn *conn = nullptr;
  ~PostgresOperatorData() {
    if (conn) {
      PQfinish(conn);
      conn = nullptr;
    }
  }
};

struct PostgresParallelState : public ParallelState {
  PostgresParallelState() : page_idx(0) {}
  mutex lock;
  idx_t page_idx;
};

static LogicalType DuckDBType(const string &pgtypename, const int atttypmod) {
  if (pgtypename == "int4") {
    return LogicalType::INTEGER;
  } else if (pgtypename == "int8") {
    return LogicalType::BIGINT;
  } else if (pgtypename == "numeric") {
    auto width = ((atttypmod - sizeof(int32_t)) >> 16) & 0xffff;
    auto scale = (((atttypmod - sizeof(int32_t)) & 0x7ff) ^ 1024) - 1024;
    return LogicalType::DECIMAL(width, scale);
  } else if (pgtypename == "bpchar" || pgtypename == "varchar") {
    return LogicalType::VARCHAR;
  } else if (pgtypename == "date") {
    return LogicalType::DATE;
  } else {
    throw IOException("Unsupported Postgres type %s", pgtypename);
  }
}

static unique_ptr<PGQueryResult>
PGQuery(PGconn *conn, string q,
        ExecStatusType response_code = PGRES_TUPLES_OK) {
  auto res = make_unique<PGQueryResult>(PQexec(conn, q.c_str()));
  if (!res->res || PQresultStatus(res->res) != response_code) {
    throw IOException("Unable to query Postgres: %s %s",
                      string(PQerrorMessage(conn)),
                      string(PQresultErrorMessage(res->res)));
  }
  return res;
}

static void PGExec(PGconn *conn, string q) {
  PGQuery(conn, q, PGRES_COMMAND_OK);
}

static unique_ptr<FunctionData>
PostgresBind(ClientContext &context, vector<Value> &inputs,
             named_parameter_map_t &named_parameters,
             vector<LogicalType> &input_table_types,
             vector<string> &input_table_names,
             vector<LogicalType> &return_types, vector<string> &names) {

  auto bind_data = make_unique<PostgresBindData>();

  bind_data->dsn = inputs[0].GetValue<string>();
  bind_data->schema_name = inputs[1].GetValue<string>();
  bind_data->table_name = inputs[2].GetValue<string>();

  bind_data->conn = PQconnectdb(bind_data->dsn.c_str());

  if (PQstatus(bind_data->conn) == CONNECTION_BAD) {
    throw IOException("Unable to connect to Postgres at %s", bind_data->dsn);
  }

  // TODO disabled since tpcds needs too many connections
  PGExec(bind_data->conn,
         "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY");

  // find the id of the table in question to simplify below queries and avoid
  // complex joins (ha)
  auto res =
      PGQuery(bind_data->conn, StringUtil::Format(R"(
SELECT pg_class.oid, reltuples, relpages
FROM pg_class JOIN pg_namespace ON relnamespace = pg_namespace.oid
WHERE nspname='%s' AND relname='%s'
)",
                                                  bind_data->schema_name,
                                                  bind_data->table_name));
  bind_data->oid = res->GetInt64(0, 0);
  bind_data->cardinality = res->GetInt64(0, 1);
  bind_data->pages = res->GetInt64(0, 2);

  res.reset();

  // query the table schema so we can interpret the bits in the pages
  // fun fact: this query also works in DuckDB ^^
  res = PGQuery(bind_data->conn, StringUtil::Format(
                                     R"(
SELECT attname, attlen, attalign, attnotnull, atttypmod, typname
FROM pg_attribute
    JOIN pg_type ON atttypid=pg_type.oid
WHERE attrelid=%d AND attnum > 0
ORDER BY attnum
)",
                                     bind_data->oid));

  for (idx_t row = 0; row < PQntuples(res->res); row++) {
    PostgresColumnInfo info;
    info.attname = res->GetString(row, 0);
    info.attlen = res->GetInt64(row, 1);
    info.attalign = res->GetString(row, 2)[0];
    info.attnotnull = res->GetString(row, 3) == "t";
    info.atttypmod = res->GetInt32(row, 4);
    info.typname = res->GetString(row, 5);

    bind_data->names.push_back(info.attname);
    bind_data->types.push_back(DuckDBType(info.typname, info.atttypmod));

    bind_data->columns.push_back(info);
  }
  res.reset();

  res = PGQuery(bind_data->conn, "SELECT pg_export_snapshot()");
  bind_data->snapshot = res->GetString(0, 0);

  return_types = bind_data->types;
  names = bind_data->names;

  return move(bind_data);
}

static void PostgresInitInternal(ClientContext &context,
                                 const PostgresBindData *bind_data_p,
                                 PostgresOperatorData *local_state,
                                 idx_t task_min, idx_t task_max) {
  D_ASSERT(bind_data_p);
  D_ASSERT(local_state);
  D_ASSERT(task_min <= task_max);

  auto bind_data = (const PostgresBindData *)bind_data_p;

  auto col_names = StringUtil::Join(
      local_state->column_ids.data(), local_state->column_ids.size(), ", ",
      [&](const idx_t column_id) {
        return column_id == COLUMN_IDENTIFIER_ROW_ID
                   ? '"' + bind_data->names[0] + '"'
                   : '"' + bind_data->names[column_id] + '"';
      });

  local_state->sql = StringUtil::Format(
      R"(
COPY (SELECT %s FROM "%s"."%s" WHERE ctid BETWEEN '(%d,0)'::tid AND '(%d,0)'::tid) TO STDOUT (FORMAT binary);
)",
      col_names, bind_data->schema_name, bind_data->table_name, task_min,
      task_max);

  local_state->exec = false;
  local_state->done = false;
}

static PGconn *PostgresScanConnect(string dsn, string snapshot) {
  auto conn = PQconnectdb(dsn.c_str());
  if (!conn || PQstatus(conn) == CONNECTION_BAD) {
    throw IOException("Unable to connect to Postgres at %s", dsn);
  }

  PGExec(conn, "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY");
  PGExec(conn, StringUtil::Format("SET TRANSACTION SNAPSHOT '%s'", snapshot));
  return conn;
}

static unique_ptr<FunctionOperatorData>
PostgresInit(ClientContext &context, const FunctionData *bind_data_p,
             const vector<column_t> &column_ids, TableFilterCollection *) {
  D_ASSERT(bind_data_p);
  auto bind_data = (const PostgresBindData *)bind_data_p;
  auto local_state = make_unique<PostgresOperatorData>();
  local_state->column_ids = column_ids;
  local_state->conn = PostgresScanConnect(bind_data->dsn, bind_data->snapshot);
  PostgresInitInternal(context, bind_data, local_state.get(), 0,
                       POSTGRES_TID_MAX);
  return move(local_state);
}

#define POSTGRES_EPOCH_JDATE 2451545 /* == date2j(2000, 1, 1) */

#define NBASE 10000
#define DEC_DIGITS 4 /* decimal digits per NBASE digit */

/*
 * Interpretation of high bits.
 */

#define NUMERIC_SIGN_MASK 0xC000
#define NUMERIC_POS 0x0000
#define NUMERIC_NEG 0x4000
#define NUMERIC_SHORT 0x8000
#define NUMERIC_SPECIAL 0xC000

/*
 * Definitions for special values (NaN, positive infinity, negative infinity).
 *
 * The two bits after the NUMERIC_SPECIAL bits are 00 for NaN, 01 for positive
 * infinity, 11 for negative infinity.  (This makes the sign bit match where
 * it is in a short-format value, though we make no use of that at present.)
 * We could mask off the remaining bits before testing the active bits, but
 * currently those bits must be zeroes, so masking would just add cycles.
 */
#define NUMERIC_EXT_SIGN_MASK 0xF000 /* high bits plus NaN/Inf flag bits */
#define NUMERIC_NAN 0xC000
#define NUMERIC_PINF 0xD000
#define NUMERIC_NINF 0xF000
#define NUMERIC_INF_SIGN_MASK 0x2000

#define NUMERIC_EXT_FLAGBITS(n) ((n)->choice.n_header & NUMERIC_EXT_SIGN_MASK)
#define NUMERIC_IS_NAN(n) ((n)->choice.n_header == NUMERIC_NAN)
#define NUMERIC_IS_PINF(n) ((n)->choice.n_header == NUMERIC_PINF)
#define NUMERIC_IS_NINF(n) ((n)->choice.n_header == NUMERIC_NINF)
#define NUMERIC_IS_INF(n)                                                      \
  (((n)->choice.n_header & ~NUMERIC_INF_SIGN_MASK) == NUMERIC_PINF)

/*
 * Short format definitions.
 */

#define NUMERIC_DSCALE_MASK 0x3FFF
#define NUMERIC_SHORT_SIGN_MASK 0x2000
#define NUMERIC_SHORT_DSCALE_MASK 0x1F80
#define NUMERIC_SHORT_DSCALE_SHIFT 7
#define NUMERIC_SHORT_DSCALE_MAX                                               \
  (NUMERIC_SHORT_DSCALE_MASK >> NUMERIC_SHORT_DSCALE_SHIFT)
#define NUMERIC_SHORT_WEIGHT_SIGN_MASK 0x0040
#define NUMERIC_SHORT_WEIGHT_MASK 0x003F
#define NUMERIC_SHORT_WEIGHT_MAX NUMERIC_SHORT_WEIGHT_MASK
#define NUMERIC_SHORT_WEIGHT_MIN (-(NUMERIC_SHORT_WEIGHT_MASK + 1))

#define NUMERIC_SIGN(is_short, header1)                                        \
  (is_short                                                                    \
       ? ((header1 & NUMERIC_SHORT_SIGN_MASK) ? NUMERIC_NEG : NUMERIC_POS)     \
       : (header1 & NUMERIC_SIGN_MASK))
#define NUMERIC_DSCALE(is_short, header1)                                      \
  (is_short                                                                    \
       ? (header1 & NUMERIC_SHORT_DSCALE_MASK) >> NUMERIC_SHORT_DSCALE_SHIFT   \
       : (header1 & NUMERIC_DSCALE_MASK))
#define NUMERIC_WEIGHT(is_short, header1, header2)                             \
  (is_short ? ((header1 & NUMERIC_SHORT_WEIGHT_SIGN_MASK                       \
                    ? ~NUMERIC_SHORT_WEIGHT_MASK                               \
                    : 0) |                                                     \
               (header1 & NUMERIC_SHORT_WEIGHT_MASK))                          \
            : (header2))

template <class T>
static void ReadDecimal(idx_t scale, int32_t ndigits, int32_t weight,
                        bool is_negative, const uint16_t *digit_ptr,
                        Vector &output, idx_t output_offset) {
  // this is wild
  auto out_ptr = FlatVector::GetData<T>(output);
  auto scale_POWER = NumericHelper::POWERS_OF_TEN[scale];

  if (ndigits == 0) {
    out_ptr[output_offset] = 0;
    return;
  }
  T integral_part = 0, fractional_part = 0;

  if (weight >= 0) {
    D_ASSERT(weight <= ndigits);
    integral_part = digit_ptr[0];
    for (auto i = 1; i <= weight; i++) {
      integral_part *= NBASE;
      if (i < ndigits) {
        integral_part += digit_ptr[i];
      }
    }
    integral_part *= scale_POWER;
  }

  if (ndigits > weight + 1) {
    fractional_part = digit_ptr[weight + 1];
    for (auto i = weight + 2; i < ndigits; i++) {
      fractional_part *= NBASE;
      if (i < ndigits) {
        fractional_part += digit_ptr[i];
      }
    }

    // we need to find out how large the fractional part is in terms of powers
    // of ten this depends on how many times we multiplied with NBASE
    // if that is different from scale, we need to divide the extra part away
    // again
    auto fractional_power = ((ndigits - weight - 1) * DEC_DIGITS);
    D_ASSERT(fractional_power >= scale);
    auto fractional_power_correction = fractional_power - scale;
    D_ASSERT(fractional_power_correction < 20);
    fractional_part /=
        NumericHelper::POWERS_OF_TEN[fractional_power_correction];
  }

  // finally

  auto base_res = (integral_part + fractional_part);

  out_ptr[output_offset] = (is_negative ? -base_res : base_res);
}

static void ProcessValue(data_ptr_t value_ptr, idx_t value_len,
                         const PostgresBindData *bind_data, idx_t col_idx,
                         bool skip, DataChunk &output, idx_t output_idx,
                         idx_t output_offset) {
  auto &type = bind_data->types[col_idx];
  auto &info = bind_data->columns[col_idx];
  auto &out_vec = output.data[output_idx];

  switch (type.id()) {
  case LogicalTypeId::INTEGER: {
    D_ASSERT(info.attlen == sizeof(int32_t));

    if (!skip) {
      auto out_ptr = FlatVector::GetData<int32_t>(out_vec);
      out_ptr[output_offset] = ntohl(Load<int32_t>(value_ptr));
    }
    break;
  }

  case LogicalTypeId::VARCHAR: {
    D_ASSERT(info.attlen == -1);

    if (!skip) {
      auto out_ptr = FlatVector::GetData<string_t>(out_vec);
      out_ptr[output_offset] =
          StringVector::AddString(out_vec, (char *)value_ptr, value_len);
    }
    break;
  }
  case LogicalTypeId::DECIMAL: {
    auto decimal_ptr = (uint16_t *)value_ptr;
    // we need at least 8 bytes here
    D_ASSERT(value_len >=
             sizeof(uint16_t) * 4); // TODO this should probably be an exception

    // convert everything to little endian
    for (int i = 0; i < value_len / sizeof(uint16_t); i++) {
      decimal_ptr[i] = ntohs(decimal_ptr[i]);
    }

    auto ndigits = decimal_ptr[0];
    D_ASSERT(value_len ==
             sizeof(uint16_t) *
                 (4 + ndigits)); // TODO this should probably be an exception
    auto weight = (int16_t)decimal_ptr[1];
    auto sign = decimal_ptr[2];

    if (!(sign == NUMERIC_POS || sign == NUMERIC_NAN || sign == NUMERIC_PINF ||
          sign == NUMERIC_NINF || sign == NUMERIC_NEG)) {
      D_ASSERT(0);
      // TODO complain
    }
    auto dscale = decimal_ptr[3];
    auto is_negative = sign == NUMERIC_NEG;

    D_ASSERT(dscale == DecimalType::GetScale(type));
    auto digit_ptr = (const uint16_t *)decimal_ptr + 4;

    switch (type.InternalType()) {
    case PhysicalType::INT16:
      ReadDecimal<int16_t>(DecimalType::GetScale(type), ndigits, weight,
                           is_negative, digit_ptr, out_vec, output_offset);
      break;
    case PhysicalType::INT32:
      ReadDecimal<int32_t>(DecimalType::GetScale(type), ndigits, weight,
                           is_negative, digit_ptr, out_vec, output_offset);
      break;
    case PhysicalType::INT64:
      ReadDecimal<int64_t>(DecimalType::GetScale(type), ndigits, weight,
                           is_negative, digit_ptr, out_vec, output_offset);
      break;

    default:
      throw InternalException("Unsupported decimal storage type");
    }
    break;
  }

  case LogicalTypeId::DATE: {
    if (!skip) {
      auto jd = ntohl(Load<int32_t>(value_ptr));
      auto out_ptr = FlatVector::GetData<date_t>(out_vec);
      out_ptr[output_offset].days =
          jd + POSTGRES_EPOCH_JDATE - 2440588; // magic!
    }
    break;
  }
  default:
    throw InternalException("Unsupported Type %s", type.ToString());
  }
}

static void PostgresScan(ClientContext &context,
                         const FunctionData *bind_data_p,
                         FunctionOperatorData *operator_state, DataChunk *,
                         DataChunk &output) {

  D_ASSERT(operator_state);
  D_ASSERT(bind_data_p);

  auto bind_data = (const PostgresBindData *)bind_data_p;
  auto local_state = (PostgresOperatorData *)operator_state;

  if (local_state->done) {
    return;
  }

  // TODO clean up buffer handling here its a mess

  char *buffer = nullptr, *buffer_org = nullptr;

  if (!local_state->exec) {
    PGQuery(local_state->conn, local_state->sql, PGRES_COPY_OUT);

    // first we take the header
    auto len = PQgetCopyData(local_state->conn, &buffer, 0);
    buffer_org = buffer;

    if (!buffer || len == -2) { // -2 means an error occured
      throw IOException("Unable to query Postgres: %s",
                        string(PQerrorMessage(local_state->conn)));
    }

    if (len == -1) { // -1 means COPY is done, e.g. no tuples
      local_state->done = true;
      return;
    }

    if (!memcmp(buffer, "PGCOPY\\n\\377\\r\\n\\0", 11)) {
      throw IOException("Expected Postgres binary header, got something else");
    }
    buffer += 11;
    D_ASSERT(buffer);

    buffer += 8; // as far as i can tell the "Flags field" and the "Header
                 // extension area length" do not contain anything interesting

    local_state->exec = true;
  }

  // then we take berlin

  char tuple_header_buf[5];

  auto len = 0;

  idx_t output_offset = 0;
  while (true) {
    output.SetCardinality(output_offset);

    if (output_offset == STANDARD_VECTOR_SIZE) {
      break;
    }
    if (!buffer) {

      auto len = PQgetCopyData(local_state->conn, &buffer, 0);

      buffer_org = buffer;
      if (!buffer || len == -2) { // -2 means an error occured
        throw IOException("Unable to query Postgres: %s",
                          string(PQerrorMessage(local_state->conn)));
      }

      if (len == -1) { // -1 means COPY is done, e.g. no tuples
        local_state->done = true;
        break;
      }
    }

    auto tuple_count = (int16_t)ntohs(Load<uint16_t>((const_data_ptr_t)buffer));
    buffer += sizeof(int16_t);

    // TODO we could possibly skip this whole read and check and abort if len ==
    // 2
    if (tuple_count == -1) { // done
      local_state->done = true;
      break;
    }

    if (tuple_count != local_state->column_ids.size()) {
      throw IOException("Expected %d fields, got %d",
                        local_state->column_ids.size(), tuple_count);
    }

    for (idx_t output_idx = 0; output_idx < output.ColumnCount();
         output_idx++) {
      auto col_idx = local_state->column_ids[output_idx];
      if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
        col_idx = 0;
      }

      auto raw_len = ntohl(Load<int32_t>((const_data_ptr_t)buffer));
      // TODO handle -1 len, indicates NULL
      buffer += sizeof(int32_t);
      auto raw_val = (data_ptr_t)buffer;
      buffer += raw_len;

      ProcessValue(raw_val, raw_len, bind_data, col_idx, false, output,
                   output_idx, output_offset);
    }

    PQfreemem(buffer_org);
    buffer = nullptr;

    output_offset++;
  }
}

static idx_t PostgresMaxThreads(ClientContext &context,
                                const FunctionData *bind_data_p) {
  D_ASSERT(bind_data_p);

  auto bind_data = (const PostgresBindData *)bind_data_p;
  return bind_data->pages / bind_data->pages_per_task;
}

static unique_ptr<ParallelState>
PostgresInitParallelState(ClientContext &context, const FunctionData *,
                          const vector<column_t> &column_ids,
                          TableFilterCollection *) {
  return make_unique<PostgresParallelState>();
}

static bool PostgresParallelStateNext(ClientContext &context,
                                      const FunctionData *bind_data_p,
                                      FunctionOperatorData *local_state_p,
                                      ParallelState *parallel_state_p) {
  D_ASSERT(bind_data_p);
  D_ASSERT(local_state_p);
  D_ASSERT(parallel_state_p);

  auto bind_data = (const PostgresBindData *)bind_data_p;
  auto &parallel_state = (PostgresParallelState &)*parallel_state_p;
  auto local_state = (PostgresOperatorData *)local_state_p;

  lock_guard<mutex> parallel_lock(parallel_state.lock);

  if (parallel_state.page_idx < bind_data->pages) {
    auto page_max = parallel_state.page_idx + bind_data->pages_per_task;
    if (parallel_state.page_idx + bind_data->pages_per_task >
        bind_data->pages) {
      // the relpages entry is not the real max, so make the last task bigger
      page_max = POSTGRES_TID_MAX;
    }
    PostgresInitInternal(context, bind_data, local_state,
                         parallel_state.page_idx, page_max);
    parallel_state.page_idx += bind_data->pages_per_task;
    return true;
  }
  return false;
}

static unique_ptr<FunctionOperatorData>
PostgresParallelInit(ClientContext &context, const FunctionData *bind_data_p,
                     ParallelState *parallel_state_p,
                     const vector<column_t> &column_ids,
                     TableFilterCollection *) {
  D_ASSERT(bind_data_p);
  auto bind_data = (const PostgresBindData *)bind_data_p;

  auto local_state = make_unique<PostgresOperatorData>();
  local_state->column_ids = column_ids;
  local_state->conn = PostgresScanConnect(bind_data->dsn, bind_data->snapshot);
  if (!PostgresParallelStateNext(context, bind_data_p, local_state.get(),
                                 parallel_state_p)) {
    local_state->done = true;
  }
  return move(local_state);
}

static string PostgresScanToString(const FunctionData *bind_data_p) {
  D_ASSERT(bind_data_p);

  auto bind_data = (const PostgresBindData *)bind_data_p;
  return bind_data->table_name;
}

static unique_ptr<NodeStatistics>
PostgresCardinality(ClientContext &context, const FunctionData *bind_data_p) {
  auto bind_data = (const PostgresBindData *)bind_data_p;
  if (bind_data->cardinality == -1) {
    return make_unique<NodeStatistics>();
  }
  return make_unique<NodeStatistics>(bind_data->cardinality);
}

struct AttachFunctionData : public TableFunctionData {
  AttachFunctionData() {}

  bool finished = false;
  string source_schema = "public";
  string target_schema = DEFAULT_SCHEMA;
  string suffix = "";
  bool overwrite = false;
  string dsn = "";
};

static unique_ptr<FunctionData>
AttachBind(ClientContext &context, vector<Value> &inputs,
           named_parameter_map_t &named_parameters,
           vector<LogicalType> &input_table_types,
           vector<string> &input_table_names, vector<LogicalType> &return_types,
           vector<string> &names) {

  auto result = make_unique<AttachFunctionData>();
  result->dsn = inputs[0].GetValue<string>();

  for (auto &kv : named_parameters) {
    if (kv.first == "source_schema") {
      result->source_schema = StringValue::Get(kv.second);
    } else if (kv.first == "target_schema") {
      result->target_schema = StringValue::Get(kv.second);
    } else if (kv.first == "overwrite") {
      result->overwrite = BooleanValue::Get(kv.second);
    }
  }

  return_types.push_back(LogicalType::BOOLEAN);
  names.emplace_back("Success");
  return move(result);
}

static void AttachFunction(ClientContext &context,
                           const FunctionData *bind_data,
                           FunctionOperatorData *operator_state,
                           DataChunk *input, DataChunk &output) {
  auto &data = (AttachFunctionData &)*bind_data;
  if (data.finished) {
    return;
  }

  auto conn = PQconnectdb(data.dsn.c_str());

  if (PQstatus(conn) == CONNECTION_BAD) {
    throw IOException("Unable to connect to Postgres at %s", data.dsn);
  }

  // create a template create view info that is filled in the loop below
  CreateViewInfo view_info;
  view_info.schema = data.target_schema;
  view_info.temporary = true;
  view_info.on_conflict = data.overwrite ? OnCreateConflict::REPLACE_ON_CONFLICT
                                         : OnCreateConflict::ERROR_ON_CONFLICT;

  vector<unique_ptr<ParsedExpression>> parameters;
  parameters.push_back(make_unique<ConstantExpression>(Value(data.dsn)));
  parameters.push_back(
      make_unique<ConstantExpression>(Value(data.source_schema)));
  // push an empty parameter for the table name but keep a pointer so we can
  // fill it below
  parameters.push_back(make_unique<ConstantExpression>(Value()));
  auto *table_name_ptr = (ConstantExpression *)parameters.back().get();
  auto table_function = make_unique<TableFunctionRef>();
  table_function->function =
      make_unique<FunctionExpression>("postgres_scan", move(parameters));

  auto select_node = make_unique<SelectNode>();
  select_node->select_list.push_back(make_unique<StarExpression>());
  select_node->from_table = move(table_function);

  view_info.query = make_unique<SelectStatement>();
  view_info.query->node = move(select_node);

  auto res = PGQuery(conn, StringUtil::Format(
                               R"(
SELECT table_name
FROM information_schema.tables
WHERE table_schema='%s'
AND table_type='BASE TABLE'
)",
                               data.source_schema)
                               .c_str());

  for (idx_t row = 0; row < PQntuples(res->res); row++) {

    auto table_name = res->GetString(row, 0);
    view_info.view_name = table_name;
    table_name_ptr->value = Value(table_name);
    // CREATE VIEW AS SELECT * FROM sqlite_scan()
    auto binder = Binder::CreateBinder(context);
    auto bound_statement = binder->Bind(*view_info.query->Copy());
    view_info.types = bound_statement.types;
    auto view_info_copy = view_info.Copy();
    context.db->GetCatalog().CreateView(context,
                                        (CreateViewInfo *)view_info_copy.get());
  }
  res.reset();
  PQfinish(conn);

  data.finished = true;
}

class PostgresScanFunction : public TableFunction {
public:
  PostgresScanFunction()
      : TableFunction(
            "postgres_scan",
            {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
            PostgresScan, PostgresBind, PostgresInit, nullptr, nullptr, nullptr,
            PostgresCardinality, nullptr, PostgresScanToString,
            PostgresMaxThreads, PostgresInitParallelState, nullptr,
            PostgresParallelInit, PostgresParallelStateNext, true, false,
            nullptr) {}
};

extern "C" {
DUCKDB_EXTENSION_API void postgres_scanner_init(duckdb::DatabaseInstance &db) {
  Connection con(db);
  con.BeginTransaction();
  auto &context = *con.context;
  auto &catalog = Catalog::GetCatalog(context);

  PostgresScanFunction postgres_fun;
  CreateTableFunctionInfo postgres_info(postgres_fun);
  catalog.CreateTableFunction(context, &postgres_info);

  TableFunction attach_func("postgres_attach", {LogicalType::VARCHAR},
                            AttachFunction, AttachBind);
  attach_func.named_parameters["overwrite"] = LogicalType::BOOLEAN;
  attach_func.named_parameters["source_schema"] = LogicalType::VARCHAR;
  attach_func.named_parameters["target_schema"] = LogicalType::VARCHAR;
  attach_func.named_parameters["suffix"] = LogicalType::VARCHAR;

  CreateTableFunctionInfo attach_info(attach_func);
  catalog.CreateTableFunction(context, &attach_info);

  con.Commit();
}

DUCKDB_EXTENSION_API const char *postgres_scanner_version() {
  return DuckDB::LibraryVersion();
}
}
