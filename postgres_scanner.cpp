#define DUCKDB_BUILD_LOADABLE_EXTENSION
#include "duckdb.hpp"

#include <libpq-fe.h>

#include "duckdb/function/table_function.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parallel/parallel_state.hpp"
#include "duckdb/storage/statistics/validity_statistics.hpp"
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
  vector<ScanTask> tasks;
  idx_t page_size;
  idx_t txid;
  vector<PostgresColumnInfo> columns;
  vector<string> names;
  vector<LogicalType> types;
};

struct PostgresOperatorData : public FunctionOperatorData {
  bool done = false;
  // TODO support projection pushdown
  vector<column_t> column_ids;
  unique_ptr<FileHandle> file_handle;
  idx_t task_offset;
  idx_t task_max;
  ScanTask current_task;
  idx_t page_offset;
  unique_ptr<data_t[]> page_buffer;
  idx_t item_count;
  idx_t item_offset;
  ItemIdData *item_ptr;
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

unique_ptr<FunctionData>
PostgresBind(ClientContext &context, vector<Value> &inputs,
             unordered_map<string, Value> &named_parameters,
             vector<LogicalType> &input_table_types,
             vector<string> &input_table_names,
             vector<LogicalType> &return_types, vector<string> &names) {

  auto result = make_unique<PostgresBindData>();

  auto dsn = inputs[0].GetValue<string>();
  auto table_name = inputs[1].GetValue<string>();

  PGconn *conn;
  PGresult *res;

  conn = PQconnectdb(dsn.c_str());

  if (PQstatus(conn) == CONNECTION_BAD) {
    throw IOException("Unable to connect to Postgres at %s", dsn);
  }

  // TODO make this optional? do this at all?
  PQexec(conn, "CHECKPOINT");
  PQexec(conn, "SYNC");

  // TODO check if those queries succeeded
  // TODO maybe we should hold an open transaction so we have a transaction id
  // as reference

  // find out where the data file in question is
  res = PQexec(conn, StringUtil::Format(
                         "SELECT setting || '/' || pg_relation_filepath('%s') "
                         "table_path, current_setting('block_size') "
                         "block_size, txid_current() "
                         "FROM pg_settings WHERE name = 'data_directory'",
                         table_name)
                         .c_str());

  result->table_name = table_name;
  result->page_size = atoi(PQgetvalue(res, 0, 1));
  result->txid = atoi(PQgetvalue(res, 0, 2));
  auto base_filename = string(PQgetvalue(res, 0, 0));
  PQclear(res);

  // find all the data files
  vector<string> file_list;
  file_list.push_back(base_filename);
  auto &file_system = FileSystem::GetFileSystem(context);
  auto additional_files = file_system.Glob(base_filename + ".*");
  file_list.insert(file_list.end(), additional_files.begin(),
                   additional_files.end());

  // TODO should we maybe construct those tasks later?
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

  // TODO support declaring a schema here ("default")
  // query the table schema so we can interpret the bits in the pages
  // fun fact: this query also works in DuckDB ^^
  res = PQexec(conn,
               StringUtil::Format(
                   "SELECT attname, attlen, attalign, attnotnull, atttypmod, "
                   "typname FROM pg_attribute JOIN pg_class ON "
                   "attrelid=pg_class.oid JOIN pg_type ON atttypid=pg_type.oid "
                   "WHERE relname='%s' AND attnum > 0 ORDER BY attnum",
                   table_name)
                   .c_str());

  for (idx_t row = 0; row < PQntuples(res); row++) {
    PostgresColumnInfo info;
    info.attname = string(PQgetvalue(res, row, 0));
    info.attlen = atol(PQgetvalue(res, row, 1));
    info.attalign = PQgetvalue(res, row, 2)[0];
    info.attnotnull = string(PQgetvalue(res, row, 3)) == "t";
    info.atttypmod = atoi(PQgetvalue(res, row, 4));
    info.typname = string(PQgetvalue(res, row, 5));

    result->names.push_back(info.attname);
    result->types.push_back(DuckDBType(info.typname, info.atttypmod));

    result->columns.push_back(info);
  }
  PQclear(res);
  PQfinish(conn);

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
  result->column_ids = column_ids;

  PostgresInitInternal(context, bind_data, result.get(), 0,
                       bind_data->tasks.size());
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

static idx_t GetAttributeLength(data_ptr_t tuple_ptr,
                                idx_t &length_length_out) {
  auto first_varlen_byte = Load<uint8_t>(tuple_ptr);
  if (first_varlen_byte == 0x80) {
    // 1 byte external, unsupported
    throw IOException("No external values");
  }
  // one byte length varlen
  if ((first_varlen_byte & 0x01) == 0x01) {
    length_length_out = 1;
    return (first_varlen_byte >> 1) & 0x7F;
  } else {
    auto four_byte_len = Load<uint32_t>(tuple_ptr);
    length_length_out = 4;
    return (four_byte_len >> 2) & 0x3FFFFFFF;
    // TODO check if not TOAST/external
  }
}

#define POSTGRES_EPOCH_JDATE 2451545 /* == date2j(2000, 1, 1) */

#define NBASE 10000
#define HALF_NBASE 5000
#define DEC_DIGITS 4       /* decimal digits per NBASE digit */
#define MUL_GUARD_DIGITS 2 /* these are measured in NBASE digits */
#define DIV_GUARD_DIGITS 4

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
    if (state.item_offset >= state.item_count) {
      // we need to move on to a new page or a new task
      if (!state.file_handle ||
          state.page_offset > state.current_task.page_max) {
        // we ran out of task, do we have another one?
        if (state.task_offset > state.task_max) {
          state.done = true;
          return;
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

    // TODO maybe its faster to read the items in one go, but perhaps not since
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
    // TODO decode the NULL bitmask here, future work

    if (tuple_header.t_xmin > bind_data->txid ||
        (tuple_header.t_xmax != 0 && tuple_header.t_xmax < bind_data->txid)) {
      // tuple was deleted or updated elsewhere
      continue;
    }

    idx_t length_length;

    tuple_ptr += tuple_header.t_hoff;
    for (idx_t col_idx = 0; col_idx < bind_data->columns.size(); col_idx++) {
      // TODO handle NULLs here, they are not in the data
      auto &type = bind_data->types[col_idx];
      auto &info = bind_data->columns[col_idx];

      switch (type.id()) {
      case LogicalTypeId::INTEGER: {
        D_ASSERT(info.attlen == sizeof(int32_t));

        auto out_ptr = FlatVector::GetData<int32_t>(output.data[col_idx]);
        out_ptr[output_offset] = Load<int32_t>(tuple_ptr);
        tuple_ptr += sizeof(int32_t);
        break;
      }

      case LogicalTypeId::VARCHAR: {
        D_ASSERT(info.attlen == -1);

        auto len = GetAttributeLength(tuple_ptr, length_length);
        auto out_ptr = FlatVector::GetData<string_t>(output.data[col_idx]);
        out_ptr[output_offset] = StringVector::AddString(
            output.data[col_idx], (char *)tuple_ptr + length_length,
            len - length_length);
        tuple_ptr += len;
        break;
      }
      case LogicalTypeId::DECIMAL: {
        D_ASSERT(info.attlen == -1);

        auto len = GetAttributeLength(tuple_ptr, length_length);
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
        auto is_negative =
            NUMERIC_SIGN(is_short, numeric_header) == NUMERIC_NEG;
        int dscale = NUMERIC_DSCALE(is_short, numeric_header);
        auto digit_ptr = (uint16_t *)(tuple_ptr + total_header_length);

        switch (type.InternalType()) {
        case PhysicalType::INT64: {
          auto out_ptr = FlatVector::GetData<int64_t>(output.data[col_idx]);

          if (ndigits == 0) {
            out_ptr[output_offset] = 0;
            break;
          }
          out_ptr[output_offset] = *digit_ptr;
          if (weight > 0) {
            for (int i = 1; i <= weight; i++) {
              out_ptr[output_offset] *= NBASE;
              if (i < ndigits) {
                // TODO bounds check on i
                out_ptr[output_offset] += digit_ptr[i];
              }
            }
          }
          out_ptr[output_offset] *=
              NumericHelper::POWERS_OF_TEN[DecimalType::GetScale(type)];

          // TODO weight == 0?
          if (weight + 1 < ndigits) {
            for (int i = 1; i <= ndigits - (weight + 1); i++) {
              out_ptr[output_offset] /= NBASE;
              if (i < ndigits) {
                // TODO bounds check on i
                out_ptr[output_offset] += digit_ptr[i];
              }
            }
          }
          break;
        }
        default:
          throw InternalException("Unsupported decimal storage type");
        }

        // data is at tuple_ptr + length_length;
        tuple_ptr += len;
        break;
      }
      case LogicalTypeId::DATE: {
        // TODO compute this alignment nicer
        // TODO compute alignment also for ints etc, should probably be generic
        D_ASSERT(info.attlen == sizeof(int32_t));
        auto offset = tuple_ptr - tuple_start_ptr;
        tuple_ptr = tuple_start_ptr + ((offset + 3) / 4) * 4;
        auto jd = Load<int32_t>(tuple_ptr);
        auto out_ptr = FlatVector::GetData<date_t>(output.data[col_idx]);
        out_ptr[output_offset].days = jd + POSTGRES_EPOCH_JDATE - 2440588;
        tuple_ptr += sizeof(int32_t);
        break;
      }
      default:
        throw InternalException("Unsupported Type %s", type.ToString());
      }
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
                         parallel_state.task_idx, parallel_state.task_idx);
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
  result->column_ids = column_ids;
  if (!PostgresParallelStateNext(context, bind_data_p, result.get(),
                                 parallel_state_p)) {
    result->done = true;
  }
  return move(result);
}

static string PostgresScanToString(const FunctionData *bind_data_p) {
  D_ASSERT(bind_data_p);

  auto bind_data = (const PostgresBindData *)bind_data_p;
  return bind_data->table_name;
}

class PostgresScanFunction : public TableFunction {
public:
  PostgresScanFunction()
      : TableFunction(
            "postgres_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR},
            PostgresScan, PostgresBind, PostgresInit, nullptr, nullptr, nullptr,
            nullptr, nullptr, PostgresScanToString, PostgresMaxThreads,
            PostgresInitParallelState, nullptr, PostgresParallelInit,
            PostgresParallelStateNext, false, false, nullptr) {}
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

  con.Commit();
}

DUCKDB_EXTENSION_API const char *postgres_scanner_version() {
  return DuckDB::LibraryVersion();
}
}
