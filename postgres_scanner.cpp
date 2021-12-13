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
  idx_t attnum;
  string attname;
  idx_t attlen;
  char attalign;
  bool attnotnull;
  string typname;
  idx_t typlen;
};

struct ScanTask {
  string file_name;
  idx_t file_size;

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

static LogicalType DuckDBType(const string &pgtypename) {
  if (pgtypename == "int4") {
    return LogicalType::INTEGER;
  } else if (pgtypename == "int8") {
    return LogicalType::BIGINT;
  } else if (pgtypename == "numeric") {
      return LogicalType::DECIMAL(15, 2);
  }  else if (pgtypename == "bpchar" || pgtypename == "varchar") {
      return LogicalType::VARCHAR;
  } else if (pgtypename == "date") {
      return LogicalType::DATE;
  }  else {
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
      task.file_size = file_size;
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
                   "SELECT attnum, attname, attlen, attalign, attnotnull, "
                   "typname, typlen FROM pg_attribute JOIN pg_class ON "
                   "attrelid=pg_class.oid JOIN pg_type ON atttypid=pg_type.oid "
                   "WHERE relname='%s' AND attnum > 0 ORDER BY attnum",
                   table_name)
                   .c_str());

  for (idx_t row = 0; row < PQntuples(res); row++) {
    PostgresColumnInfo info;
    info.attnum = atol(PQgetvalue(res, row, 0));
    info.attname = string(PQgetvalue(res, row, 1));
    info.attlen = atol(PQgetvalue(res, row, 2));
    info.attalign = PQgetvalue(res, row, 3)[0];
    info.attnotnull = string(PQgetvalue(res, row, 4)) == "t";
    info.typname = string(PQgetvalue(res, row, 5));
    info.typlen = atol(PQgetvalue(res, row, 6));

    result->names.push_back(info.attname);
    result->types.push_back(DuckDBType(info.typname));

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
//188  * xxxxxx00 4-byte length word, aligned, uncompressed data (up to 1G)
//189  * xxxxxx10 4-byte length word, aligned, *compressed* data (up to 1G)
//190  * 00000001 1-byte length word, unaligned, TOAST pointer
//191  * xxxxxxx1 1-byte length word, unaligned, uncompressed data (up to 126b)
//192  *
//193  * The "xxx" bits are the length field (which includes itself in all cases).
//194  * In the big-endian case we mask to extract the length, in the little-endian
//195  * case we shift.  Note that in both cases the flag bits are in the physically
//196  * first byte.  Also, it is not possible for a 1-byte length word to be zero;
//197  * this lets us disambiguate alignment padding bytes from the start of an
//198  * unaligned datum.  (We now *require* pad bytes to be filled with zero!)
//199  *
//
//typedef union
//149 {
//150     struct                      /* Normal varlena (4-byte length) */
//151     {
//152         uint32      va_header;
//153         char        va_data[FLEXIBLE_ARRAY_MEMBER];
//154     }           va_4byte;
//155     struct                      /* Compressed-in-line format */
//156     {
//157         uint32      va_header;
//158         uint32      va_tcinfo;  /* Original data size (excludes header) and
//  159                                  * compression method; see va_extinfo */
//160         char        va_data[FLEXIBLE_ARRAY_MEMBER]; /* Compressed data */
//161     }           va_compressed;
//162 } varattrib_4b;
//163
//
//typedef struct
//165 {
//166     uint8       va_header;
//167     char        va_data[FLEXIBLE_ARRAY_MEMBER]; /* Data begins here */
//168 } varattrib_1b;
//169

//typedef struct
//172 {
//173     uint8       va_header;      /* Always 0x80 or 0x01 */
//174     uint8       va_tag;         /* Type of datum */
//175     char        va_data[FLEXIBLE_ARRAY_MEMBER]; /* Type-specific data */
//176 } varattrib_1b_e;

// from postgres.h

//
//00146 #define VARATT_IS_4B(PTR) \
//00147     ((((varattrib_1b *) (PTR))->va_header & 0x80) == 0x00)
//00148 #define VARATT_IS_4B_U(PTR) \
//00149     ((((varattrib_1b *) (PTR))->va_header & 0xC0) == 0x00)
//00150 #define VARATT_IS_4B_C(PTR) \
//00151     ((((varattrib_1b *) (PTR))->va_header & 0xC0) == 0x40)
//00152 #define VARATT_IS_1B(PTR) \
//00153     ((((varattrib_1b *) (PTR))->va_header & 0x80) == 0x80)
//00154 #define VARATT_IS_1B_E(PTR) \
//00155     ((((varattrib_1b *) (PTR))->va_header) == 0x80)
//00156 #define VARATT_NOT_PAD_BYTE(PTR) \
//00157     (*((uint8 *) (PTR)) != 0)
//00158
//00159 /* VARSIZE_4B() should only be used on known-aligned data */
//00160 #define VARSIZE_4B(PTR) \
//00161     (((varattrib_4b *) (PTR))->va_4byte.va_header & 0x3FFFFFFF)
//00162 #define VARSIZE_1B(PTR) \
//00163     (((varattrib_1b *) (PTR))->va_header & 0x7F)
//00164 #define VARSIZE_1B_E(PTR) \
//00165     (((varattrib_1b_e *) (PTR))->va_len_1be)
//00166

//00178 #define VARATT_IS_4B(PTR) \
//00179     ((((varattrib_1b *) (PTR))->va_header & 0x01) == 0x00)
//00180 #define VARATT_IS_4B_U(PTR) \
//00181     ((((varattrib_1b *) (PTR))->va_header & 0x03) == 0x00)
//00182 #define VARATT_IS_4B_C(PTR) \
//00183     ((((varattrib_1b *) (PTR))->va_header & 0x03) == 0x02)
//00184 #define VARATT_IS_1B(PTR) \
//00185     ((((varattrib_1b *) (PTR))->va_header & 0x01) == 0x01)
//00186 #define VARATT_IS_1B_E(PTR) \
//00187     ((((varattrib_1b *) (PTR))->va_header) == 0x01)

//00190
//00218 #define VARHDRSZ_EXTERNAL       2
//00219
//00220 #define VARDATA_4B(PTR)     (((varattrib_4b *) (PTR))->va_4byte.va_data)
//00221 #define VARDATA_4B_C(PTR)   (((varattrib_4b *) (PTR))->va_compressed.va_data)
//00222 #define VARDATA_1B(PTR)     (((varattrib_1b *) (PTR))->va_data)
//00223 #define VARDATA_1B_E(PTR)   (((varattrib_1b_e *) (PTR))->va_data)
//00224
//00225 #define VARRAWSIZE_4B_C(PTR) \
//00226     (((varattrib_4b *) (PTR))->va_compressed.va_rawsize)
//00227
//00228 /* Externally visible macros */
//00229
//00230 /*
//00231  * VARDATA, VARSIZE, and SET_VARSIZE are the recommended API for most code
//00232  * for varlena datatypes.  Note that they only work on untoasted,
//00233  * 4-byte-header Datums!
//00234  *
//00235  * Code that wants to use 1-byte-header values without detoasting should
//00236  * use VARSIZE_ANY/VARSIZE_ANY_EXHDR/VARDATA_ANY.  The other macros here
//00237  * should usually be used only by tuple assembly/disassembly code and
//00238  * code that specifically wants to work with still-toasted Datums.
//00239  *
//00240  * WARNING: It is only safe to use VARDATA_ANY() -- typically with
//00241  * PG_DETOAST_DATUM_PACKED() -- if you really don't care about the alignment.
//00242  * Either because you're working with something like text where the alignment
//00243  * doesn't matter or because you're not going to access its constituent parts
//00244  * and just use things like memcpy on it anyways.
//00245  */
//00246 #define VARDATA(PTR)                        VARDATA_4B(PTR)
//00247 #define VARSIZE(PTR)                        VARSIZE_4B(PTR)
//00248
//00249 #define VARSIZE_SHORT(PTR)                  VARSIZE_1B(PTR)
//00250 #define VARDATA_SHORT(PTR)                  VARDATA_1B(PTR)
//00251
//00252 #define VARSIZE_EXTERNAL(PTR)               VARSIZE_1B_E(PTR)
//00253 #define VARDATA_EXTERNAL(PTR)               VARDATA_1B_E(PTR)
//00254
//00255 #define VARATT_IS_COMPRESSED(PTR)           VARATT_IS_4B_C(PTR)
//00256 #define VARATT_IS_EXTERNAL(PTR)             VARATT_IS_1B_E(PTR)
//00257 #define VARATT_IS_SHORT(PTR)                VARATT_IS_1B(PTR)
//00258 #define VARATT_IS_EXTENDED(PTR)             (!VARATT_IS_4B_U(PTR))
//00259
//00260 #define SET_VARSIZE(PTR, len)               SET_VARSIZE_4B(PTR, len)
//00261 #define SET_VARSIZE_SHORT(PTR, len)         SET_VARSIZE_1B(PTR, len)
//00262 #define SET_VARSIZE_COMPRESSED(PTR, len)    SET_VARSIZE_4B_C(PTR, len)
//00263 #define SET_VARSIZE_EXTERNAL(PTR, len)      SET_VARSIZE_1B_E(PTR, len)
//00264
//00265 #define VARSIZE_ANY(PTR) \
//00266     (VARATT_IS_1B_E(PTR) ? VARSIZE_1B_E(PTR) : \
//00267      (VARATT_IS_1B(PTR) ? VARSIZE_1B(PTR) : \
//00268       VARSIZE_4B(PTR)))
//00269

void PostgresScan(ClientContext &context, const FunctionData *bind_data_p,
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
    auto tuple_ptr = state.page_buffer.get() + item.lp_off;
    auto tuple_header = Load<HeapTupleHeaderData>(tuple_ptr);
    // TODO decode the NULL bitmask here, future work

    if (tuple_header.t_xmin > bind_data->txid ||
        (tuple_header.t_xmax != 0 && tuple_header.t_xmax < bind_data->txid)) {
      // tuple was deleted or updated elsewhere
      continue;
    }

    tuple_ptr += tuple_header.t_hoff;
    for (idx_t col_idx = 0; col_idx < bind_data->columns.size(); col_idx++) {
      // TODO handle NULLs here, they are not in the data
      auto &type = bind_data->types[col_idx];
      switch (type.id()) {
      case LogicalTypeId::INTEGER: {
        auto out_ptr = FlatVector::GetData<int32_t>(output.data[col_idx]);
        out_ptr[output_offset] = Load<int32_t>(tuple_ptr);
        tuple_ptr += sizeof(int32_t);
        break;
      }
//      case LogicalTypeId::BIGINT: {
//        // TODO this is not correct yet
//        auto out_ptr = FlatVector::GetData<int64_t>(output.data[col_idx]);
//        out_ptr[output_offset] = Load<int64_t>(tuple_ptr);
//        tuple_ptr += sizeof(int64_t);
//        break;
//      }
          case LogicalTypeId::VARCHAR: {
              throw std::runtime_error("eek");
//
//              // TODO this is not correct yet
//              auto out_ptr = FlatVector::GetData<string_t>(output.data[col_idx]);
//              auto len = Load<uint32_t>(tuple_ptr);
//
//              // TODO interpret 2 high bits for TOASTedness
//              out_ptr[output_offset] = StringVector::AddString(output.data[col_idx], (char*) tuple_ptr + 4, len - 4);
//              tuple_ptr += len;
//              break;
          }
          case LogicalTypeId::DECIMAL: {
              auto first_varlen_byte = Load<uint8_t>(tuple_ptr);
              if (first_varlen_byte == 0x80) {
                  // 1 byte external, unsupported
                  throw IOException("No external values");
              }
              idx_t len = 0;
              // one byte length varlen
              if ((first_varlen_byte & 0x80) == 0x80) {
                  len = first_varlen_byte & 0x7F;
              } else {
                  auto four_byte_len = Load<uint32_t>(tuple_ptr);
                  len = four_byte_len & 0x3FFFFFFF;
              }
              printf("len=%llu\n", len);

              tuple_ptr += len;
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
