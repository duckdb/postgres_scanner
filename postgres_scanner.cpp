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

// simplified from
// https://github.com/postgres/postgres/blob/master/src/include/access/htup_details.h
// and elsewhere

struct ItemIdData {
  uint32_t lp_off : 15, lp_flags : 2, lp_len : 15;
};

struct PageHeaderData {
  uint64_t pd_lsn;
  uint16_t pd_checksum;
  uint16_t pd_flags;
  uint16_t pd_lower;
  uint16_t pd_upper;
  uint16_t pd_special;
  uint16_t pd_pagesize_version;
  uint32_t pd_prune_xid;
};

struct HeapTupleHeaderData {
  uint32_t t_xmin;
  uint32_t t_xmax;
  uint32_t t_cid_t_xvac;
  uint32_t ip_blkid;
  uint16_t ip_posid;
  uint16_t t_infomask2;
  uint16_t t_infomask;
  uint8_t t_hoff;
  /* ^ - 23 bytes - ^ */
};

struct PostgresColumnInfo {
  string attname;
  idx_t attlen;
  char attalign;
  bool attnotnull;
  int atttypmod;
  string typname;
};

struct ScanTask {
  string file_name;
  idx_t page_min;
  idx_t page_max;
};

struct PostgresBindData : public FunctionData {
  string table_name;
  idx_t oid = 0;
  idx_t cardinality = 0;
  idx_t page_size = 0;
  idx_t txid = 0;

  vector<ScanTask> tasks;
  vector<PostgresColumnInfo> columns;
  vector<string> names;
  vector<LogicalType> types;

  PGconn *conn = nullptr;
  ~PostgresBindData() {
    if (conn) {
      PQexec(conn, "ROLLBACK");
      PQfinish(conn);
      conn = nullptr;
    }
  }
};

struct PostgresOperatorData : public FunctionOperatorData {
  bool done = false;
  vector<idx_t> scan_columns;
  idx_t max_bound_column_id;
  unique_ptr<FileHandle> file_handle;
  idx_t task_offset;
  idx_t task_max;
  ScanTask current_task;
  idx_t page_offset;
  unique_ptr<data_t[]> page_buffer;
  idx_t item_count;
  idx_t item_offset;
  ItemIdData *item_ptr;

  void InitScanColumns(idx_t ncol, const vector<column_t> &column_ids) {
    idx_t col_idx = 0;
    max_bound_column_id = 0;
    scan_columns.resize(ncol, -1);
    for (auto &col : column_ids) {
      if (col == COLUMN_IDENTIFIER_ROW_ID) {
        continue;
      }
      scan_columns[col] = col_idx;
      col_idx++;
      if (col > max_bound_column_id) {
        max_bound_column_id = col;
      }
    }
  }
};

struct PostgresParallelState : public ParallelState {
  mutex lock;
  idx_t task_idx;
};

static constexpr idx_t PAGES_PER_TASK = 10000;

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

struct PGQueryResult {
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

static void PGExec(PGconn *conn, string q) {
  auto res = make_unique<PGQueryResult>();
  res->res = PQexec(conn, q.c_str());

  if (!res->res) {
    throw IOException("Unable to query Postgres");
  }
  if (PQresultStatus(res->res) != PGRES_COMMAND_OK) {
    throw IOException("Unable to query Postgres: %s",
                      string(PQresultErrorMessage(res->res)));
  }
}

static unique_ptr<PGQueryResult> PGQuery(PGconn *conn, string q) {
  auto res = make_unique<PGQueryResult>();
  res->res = PQexec(conn, q.c_str());

  if (!res->res) {
    throw IOException("Unable to query Postgres");
  }
  if (PQresultStatus(res->res) != PGRES_TUPLES_OK) {
    throw IOException("Unable to query Postgres: %s",
                      string(PQresultErrorMessage(res->res)));
  }
  return res;
}

unique_ptr<FunctionData>
PostgresBind(ClientContext &context, vector<Value> &inputs,
             unordered_map<string, Value> &named_parameters,
             vector<LogicalType> &input_table_types,
             vector<string> &input_table_names,
             vector<LogicalType> &return_types, vector<string> &names) {

  auto result = make_unique<PostgresBindData>();

  auto dsn = inputs[0].GetValue<string>();
  auto schema_name = inputs[1].GetValue<string>();
  auto table_name = inputs[2].GetValue<string>();

  result->conn = PQconnectdb(dsn.c_str());

  if (PQstatus(result->conn) == CONNECTION_BAD) {
    throw IOException("Unable to connect to Postgres at %s", dsn);
  }

  // TODO disabled since tpcds needs too many connections
  //  PGExec(result->conn, "BEGIN TRANSACTION");
  //  // get a read lock on lineitem so we can be sure nobody deletes the pages
  //  // under our asses
  //  PGExec(result->conn,
  //         StringUtil::Format("LOCK TABLE \"%s\".\"%s\" IN ACCESS SHARE
  //         MODE;",
  //                            schema_name, table_name)
  //             .c_str());

  // find the id of the table in question to simplify below queries and avoid
  // complex joins (ha)
  auto res = PGQuery(result->conn, StringUtil::Format(R"(
SELECT pg_class.oid, reltuples
FROM pg_class JOIN pg_namespace ON relnamespace = pg_namespace.oid
WHERE nspname='%s' AND relname='%s'
)",
                                                      schema_name, table_name));
  result->oid = res->GetInt64(0, 0);
  result->cardinality = res->GetInt64(0, 1);

  // somewhat hacky query to find out where the data file in question is
  res = PGQuery(result->conn, StringUtil::Format(R"(
SELECT setting || '/' || pg_relation_filepath(%d) table_path,
    current_setting('block_size') block_size,
    txid_current()
FROM pg_settings
WHERE name = 'data_directory'
)",
                                                 result->oid));

  result->table_name = table_name;
  result->page_size = res->GetInt32(0, 1);
  result->txid = res->GetInt32(0, 2);
  auto base_filename = res->GetString(0, 0);
  res.reset();

  // find all the data files
  vector<string> file_list;
  file_list.push_back(base_filename);
  auto &file_system = FileSystem::GetFileSystem(context);
  auto additional_files = file_system.Glob(base_filename + ".*");
  file_list.insert(file_list.end(), additional_files.begin(),
                   additional_files.end());

  for (auto &file_name : file_list) {
    auto file_handle =
        file_system.OpenFile(file_name, FileFlags::FILE_FLAGS_READ);
    auto file_size = file_handle->GetFileSize();

    if (file_size % result->page_size != 0) {
      throw IOException(
          "Postgres data file length %s not a multiple of page size",
          file_name);
    }

    auto pages_in_file = file_size / result->page_size;
    for (idx_t page_idx = 0; page_idx < pages_in_file;
         page_idx += PAGES_PER_TASK) {
      ScanTask task;
      task.file_name = file_name;
      task.page_min = page_idx;
      task.page_max = MinValue(page_idx + PAGES_PER_TASK, pages_in_file) - 1;
      result->tasks.push_back(move(task));
    }
  }

  // query the table schema so we can interpret the bits in the pages
  // fun fact: this query also works in DuckDB ^^
  res = PGQuery(result->conn, StringUtil::Format(
                                  R"(
SELECT attname, attlen, attalign, attnotnull, atttypmod, typname
FROM pg_attribute
    JOIN pg_type ON atttypid=pg_type.oid
WHERE attrelid=%d AND attnum > 0
ORDER BY attnum
)",
                                  result->oid));

  for (idx_t row = 0; row < PQntuples(res->res); row++) {
    PostgresColumnInfo info;
    info.attname = res->GetString(row, 0);
    info.attlen = res->GetInt64(row, 1);
    info.attalign = res->GetString(row, 2)[0];
    info.attnotnull = res->GetString(row, 3) == "t";
    info.atttypmod = res->GetInt32(row, 4);
    info.typname = res->GetString(row, 5);

    result->names.push_back(info.attname);
    result->types.push_back(DuckDBType(info.typname, info.atttypmod));

    result->columns.push_back(info);
  }
  res.reset();

  // TODO TEMP
  PQfinish(result->conn);
  result->conn = nullptr;

  return_types = result->types;
  names = result->names;

  return move(result);
}

static void PostgresInitInternal(ClientContext &context,
                                 const PostgresBindData *bind_data,
                                 PostgresOperatorData *local_state,
                                 idx_t task_min, idx_t task_max) {
  D_ASSERT(bind_data);
  D_ASSERT(local_state);
  D_ASSERT(task_min <= task_max);

  local_state->done = false;
  local_state->task_max = task_max;
  local_state->task_offset = task_min;

  local_state->page_buffer =
      unique_ptr<data_t[]>(new data_t[bind_data->page_size]);
  local_state->item_offset = 0;
  local_state->item_count = 0;
}

static unique_ptr<FunctionOperatorData>
PostgresInit(ClientContext &context, const FunctionData *bind_data_p,
             const vector<column_t> &column_ids, TableFilterCollection *) {
  D_ASSERT(bind_data_p);
  auto bind_data = (const PostgresBindData *)bind_data_p;
  auto result = make_unique<PostgresOperatorData>();

  PostgresInitInternal(context, bind_data, result.get(), 0,
                       bind_data->tasks.size());
  result->InitScanColumns(bind_data->types.size(), column_ids);
  return move(result);
}

//
// 188  * xxxxxx00 4-byte length word, aligned, uncompressed data (up to 1G)
// 189  * xxxxxx10 4-byte length word, aligned, *compressed* data (up to 1G)
// 190  * 00000001 1-byte length word, unaligned, TOAST pointer
// 191  * xxxxxxx1 1-byte length word, unaligned, uncompressed data (up to 126b)
// 192  *
// 193  * The "xxx" bits are the length field (which includes itself in all
// cases). 194  * In the big-endian case we mask to extract the length, in the
// little-endian 195  * case we shift.  Note that in both cases the flag bits
// are in the physically 196  * first byte.  Also, it is not possible for a
// 1-byte length word to be zero; 197  * this lets us disambiguate alignment
// padding bytes from the start of an 198  * unaligned datum.  (We now *require*
// pad bytes to be filled with zero!) 199  *

static idx_t GetAttributeLength(data_ptr_t tuple_ptr, idx_t page_size,
                                idx_t &length_length_out) {
  auto first_varlen_byte = Load<uint8_t>(tuple_ptr);
  idx_t len = 0;
  if (first_varlen_byte == 0x80) {
    // 1 byte external, unsupported
    // TODO this check is wrong!
    // throw IOException("No external values");
  }
  // one byte length varlen
  if ((first_varlen_byte & 0x01) == 0x01) {
    length_length_out = 1;
    len = (first_varlen_byte >> 1) & 0x7F;

  } else {
    auto four_byte_len = Load<uint32_t>(tuple_ptr);
    length_length_out = 4;
    if ((four_byte_len & 0x3) == 0x2) {
      throw IOException("No compressed values");
    }
    len = (four_byte_len >> 2) & 0x3FFFFFFF;
  }
  if (len > page_size) {
    throw IOException(
        "Can't have attribute length of %lld on page of size %lld", len,
        page_size);
  }
  return len;
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
  out_ptr[output_offset] =
      (integral_part + fractional_part) * (is_negative ? -1 : 1);
}

static bool PrepareTuple(ClientContext &context,
                         const PostgresBindData *bind_data,
                         PostgresOperatorData &state) {
  if (state.item_offset >= state.item_count) {
    // we need to move on to a new page or a new task
    if (!state.file_handle || state.page_offset > state.current_task.page_max) {
      // we ran out of task, do we have another one?
      if (state.task_offset >= state.task_max) {
        state.done = true;
        return false;
      }
      state.current_task = bind_data->tasks[state.task_offset];
      state.file_handle = FileSystem::GetFileSystem(context).OpenFile(
          state.current_task.file_name, FileFlags::FILE_FLAGS_READ);
      state.page_offset = state.current_task.page_min;
      state.item_count = 0;
      state.task_offset++;
    }

    auto page_ptr = state.page_buffer.get();
    state.file_handle->Read(page_ptr, bind_data->page_size,
                            state.page_offset * bind_data->page_size);
    // parse page header
    auto page_header = Load<PageHeaderData>(page_ptr);
    page_ptr += sizeof(PageHeaderData);

    if (page_header.pd_lower > bind_data->page_size ||
        page_header.pd_upper > bind_data->page_size) {
      throw IOException("Page upper/lower offsets exceed page size");
    }
    state.item_ptr = (ItemIdData *)page_ptr;
    state.item_count =
        (page_header.pd_lower - sizeof(PageHeaderData)) / sizeof(ItemIdData);
    state.item_offset = 0;
    state.page_offset++;
  }

  return true;
}

static idx_t ProcessValue(data_ptr_t tuple_ptr,
                          const PostgresBindData *bind_data, idx_t &col_idx,
                          bool skip, Vector &output, idx_t output_offset) {
  idx_t length_length;
  auto &type = bind_data->types[col_idx];
  auto &info = bind_data->columns[col_idx];

  switch (type.id()) {
  case LogicalTypeId::INTEGER: {
    D_ASSERT(info.attlen == sizeof(int32_t));

    if (!skip) {
      auto out_ptr = FlatVector::GetData<int32_t>(output);
      out_ptr[output_offset] = Load<int32_t>(tuple_ptr);
    }
    return sizeof(int32_t);
  }

  case LogicalTypeId::VARCHAR: {
    D_ASSERT(info.attlen == -1);

    auto len =
        GetAttributeLength(tuple_ptr, bind_data->page_size, length_length);

    if (!skip) {
      auto out_ptr = FlatVector::GetData<string_t>(output);
      out_ptr[output_offset] = StringVector::AddString(
          output, (char *)tuple_ptr + length_length, len - length_length);
    }
    return len;
  }
  case LogicalTypeId::DECIMAL: {
    D_ASSERT(info.attlen == -1);

    auto len =
        GetAttributeLength(tuple_ptr, bind_data->page_size, length_length);
    if (!skip) {
      auto numeric_header = Load<uint16_t>(tuple_ptr + length_length);
      if ((numeric_header & NUMERIC_SIGN_MASK) == NUMERIC_SPECIAL) {
        throw IOException(
            "'Special' numerics not supported. Please insert more coin.");
      }

      auto is_short = (numeric_header & NUMERIC_SIGN_MASK) == NUMERIC_SHORT;
      auto total_header_length =
          length_length + (is_short ? sizeof(uint16_t) : sizeof(uint32_t));
      int ndigits = (len - total_header_length) / sizeof(int16_t);

      int16_t long_header = 0;
      if (!is_short) {
        long_header =
            Load<uint16_t>(tuple_ptr + length_length + sizeof(uint16_t));
      }

      int weight = NUMERIC_WEIGHT(is_short, numeric_header, long_header);
      auto is_negative = NUMERIC_SIGN(is_short, numeric_header) == NUMERIC_NEG;
      int dscale = NUMERIC_DSCALE(is_short, numeric_header);
      auto digit_ptr = (uint16_t *)(tuple_ptr + total_header_length);
      D_ASSERT(dscale == DecimalType::GetScale(type));

      switch (type.InternalType()) {
      case PhysicalType::INT16:
        ReadDecimal<int16_t>(DecimalType::GetScale(type), ndigits, weight,
                             is_negative, digit_ptr, output, output_offset);
        break;
      case PhysicalType::INT32:
        ReadDecimal<int32_t>(DecimalType::GetScale(type), ndigits, weight,
                             is_negative, digit_ptr, output, output_offset);
        break;
      case PhysicalType::INT64:
        ReadDecimal<int64_t>(DecimalType::GetScale(type), ndigits, weight,
                             is_negative, digit_ptr, output, output_offset);
        break;

      default:
        throw InternalException("Unsupported decimal storage type");
      }
    }
    return len;
  }
  case LogicalTypeId::DATE: {
    D_ASSERT(info.attlen == sizeof(int32_t));
    if (!skip) {
      auto jd = Load<int32_t>(tuple_ptr);
      auto out_ptr = FlatVector::GetData<date_t>(output);
      out_ptr[output_offset].days = jd + POSTGRES_EPOCH_JDATE - 2440588;
    }
    return sizeof(int32_t);
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
  auto &state = (PostgresOperatorData &)*operator_state;

  if (state.done) {
    return;
  }

  idx_t output_offset = 0;
  while (output_offset < STANDARD_VECTOR_SIZE) {
    if (!PrepareTuple(context, bind_data, state)) {
      return;
    }

    // maybe its faster to read the items in one go, but perhaps not since
    // the page is rather small and fits in cache move to next page item
    auto item = state.item_ptr[state.item_offset++];
#ifdef DEBUG // this check is somewhat optional
    if (item.lp_off + item.lp_len > bind_data->page_size) {
      throw IOException("Item pointer and length exceed page size");
    }
#endif
    // unused or dead
    if (item.lp_flags == 0 || item.lp_flags == 3) {
      continue;
    }
    //  redirect
    if (item.lp_flags == 2) {
      throw IOException("REDIRECT tuples are not supported");
    }
    // normal
    if (item.lp_flags != 1 || item.lp_len == 0) {
      throw IOException("Expected NORMAL tuple with non-zero length but got "
                        "something else");
    }

    // read tuple header
    auto tuple_start_ptr = state.page_buffer.get() + item.lp_off;
    auto tuple_ptr = tuple_start_ptr;
    auto tuple_header = Load<HeapTupleHeaderData>(tuple_ptr);
    auto null_ptr = tuple_ptr + 23;

    if (tuple_header.t_xmin > bind_data->txid ||
        (tuple_header.t_xmax != 0 && tuple_header.t_xmax < bind_data->txid)) {
      // tuple was deleted or updated elsewhere
      continue;
    }
    tuple_ptr += tuple_header.t_hoff;

    // if we are done with the last column that the query actually wants
    // we can completely skip ahead to the next tupl
    for (idx_t col_idx = 0; col_idx <= state.max_bound_column_id; col_idx++) {

      auto null_byte = *(null_ptr + col_idx / 8);
      auto null_column = ((null_byte >> col_idx % 8) & 0x1) == 0x1;
      // TODO interpret NULL flags here, this seems still wrong?
      // D_ASSERT(!null_column);

      auto &info = bind_data->columns[col_idx];

      // TODO clean this up, its uuugly and we dont have to start at
      // tuple_start_ptr every time
      if (info.attlen != -1) {
        if (info.attalign == 'i') {
          auto offset = tuple_ptr - tuple_start_ptr;
          auto real_offset = ((offset + 3) / 4) * 4;
          tuple_ptr = tuple_start_ptr + real_offset;
        } else {
          throw IOException("Unknown alignment %c", info.attalign);
        }
      }

      bool skip_column = state.scan_columns[col_idx] == -1;
      auto output_idx = skip_column ? -1 : state.scan_columns[col_idx];

      tuple_ptr += ProcessValue(tuple_ptr, bind_data, col_idx, skip_column,
                                output.data[output_idx], output_offset);
    }
    output.SetCardinality(++output_offset);
  }
}
static idx_t PostgresMaxThreads(ClientContext &context,
                                const FunctionData *bind_data_p) {
  D_ASSERT(bind_data_p);

  auto bind_data = (const PostgresBindData *)bind_data_p;
  return bind_data->tasks.size();
}

static unique_ptr<ParallelState>
PostgresInitParallelState(ClientContext &context, const FunctionData *,
                          const vector<column_t> &column_ids,
                          TableFilterCollection *) {
  auto result = make_unique<PostgresParallelState>();
  result->task_idx = 0;
  return move(result);
}

static bool PostgresParallelStateNext(ClientContext &context,
                                      const FunctionData *bind_data_p,
                                      FunctionOperatorData *state_p,
                                      ParallelState *parallel_state_p) {
  D_ASSERT(bind_data_p);
  D_ASSERT(state_p);
  D_ASSERT(parallel_state_p);

  auto bind_data = (const PostgresBindData *)bind_data_p;
  auto &parallel_state = (PostgresParallelState &)*parallel_state_p;
  auto local_state = (PostgresOperatorData *)state_p;

  lock_guard<mutex> parallel_lock(parallel_state.lock);

  if (parallel_state.task_idx < bind_data->tasks.size()) {
    PostgresInitInternal(context, bind_data, local_state,
                         parallel_state.task_idx, parallel_state.task_idx + 1);
    parallel_state.task_idx++;
    return true;
  }
  return false;
}

static unique_ptr<FunctionOperatorData>
PostgresParallelInit(ClientContext &context, const FunctionData *bind_data_p,
                     ParallelState *parallel_state_p,
                     const vector<column_t> &column_ids,
                     TableFilterCollection *) {
  auto result = make_unique<PostgresOperatorData>();
  if (!PostgresParallelStateNext(context, bind_data_p, result.get(),
                                 parallel_state_p)) {
    result->done = true;
  }
  auto bind_data = (const PostgresBindData *)bind_data_p;

  result->InitScanColumns(bind_data->types.size(), column_ids);
  return move(result);
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
           unordered_map<string, Value> &named_parameters,
           vector<LogicalType> &input_table_types,
           vector<string> &input_table_names, vector<LogicalType> &return_types,
           vector<string> &names) {
  auto result = make_unique<AttachFunctionData>();
  result->dsn = inputs[0].GetValue<string>();

  for (auto &kv : named_parameters) {
    if (kv.first == "source_schema") {
      result->source_schema = kv.second.str_value;
    } else if (kv.first == "target_schema") {
      result->target_schema = kv.second.str_value;
    } else if (kv.first == "overwrite") {
      result->overwrite = kv.second.value_.boolean;
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
