#include "duckdb.hpp"

#include <libpq-fe.h>

#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "postgres_filter_pushdown.hpp"
#include "postgres_scanner.hpp"
#include "postgres_result.hpp"
#include "postgres_binary_reader.hpp"
#include "storage/postgres_catalog.hpp"
#include "storage/postgres_transaction.hpp"
#include "storage/postgres_table_set.hpp"

namespace duckdb {

static constexpr uint32_t POSTGRES_TID_MAX = 4294967295;

struct PostgresGlobalState;

struct PostgresLocalState : public LocalTableFunctionState {
	bool done = false;
	bool exec = false;
	bool no_connection = false;
	string sql;
	vector<column_t> column_ids;
	TableFilterSet *filters;
	string col_names;
	PostgresConnection connection;
	idx_t batch_idx = 0;
	PostgresPoolConnection pool_connection;

	void ScanChunk(ClientContext &context, const PostgresBindData &bind_data, PostgresGlobalState &gstate,
	               DataChunk &output);
};

struct PostgresGlobalState : public GlobalTableFunctionState {
	explicit PostgresGlobalState(idx_t max_threads) : page_idx(0), batch_idx(0), max_threads(max_threads) {
	}

	mutable mutex lock;
	idx_t page_idx;
	idx_t batch_idx;
	idx_t max_threads;
	unique_ptr<ColumnDataCollection> collection;
	ColumnDataScanState scan_state;
	bool used_main_thread = false;
	string snapshot;

	PostgresConnection &GetConnection();
	void SetConnection(PostgresConnection connection);
	void SetConnection(shared_ptr<OwnedPostgresConnection> connection);

	bool TryOpenNewConnection(ClientContext &context, PostgresLocalState &lstate, const PostgresBindData &bind_data);
	idx_t MaxThreads() const override {
		return max_threads;
	}

private:
	PostgresConnection connection;
};

static void PostgresGetSnapshot(const PostgresBindData &bind_data, PostgresGlobalState &gstate) {
	unique_ptr<PostgresResult> result;
	// by default disable snapshotting
	gstate.snapshot = string();
	if (gstate.max_threads <= 1) {
		return;
	}

	// reader threads can use the same snapshot
	auto &con = gstate.GetConnection();

	result =
	    con.TryQuery("SELECT pg_export_snapshot()");
	if (result) {
		gstate.snapshot = result->GetString(0, 0);
	}
}

void PostgresScanFunction::PrepareBind(PostgresVersion version, ClientContext &context, PostgresBindData &bind_data,
                                       idx_t approx_num_pages) {
	Value pages_per_task;
	if (context.TryGetCurrentSetting("pg_pages_per_task", pages_per_task)) {
		bind_data.pages_per_task = UBigIntValue::Get(pages_per_task);
		if (bind_data.pages_per_task == 0) {
			bind_data.pages_per_task = PostgresBindData::DEFAULT_PAGES_PER_TASK;
		}
	}
	bool use_ctid_scan = true;
	Value pg_use_ctid_scan;
	if (context.TryGetCurrentSetting("pg_use_ctid_scan", pg_use_ctid_scan)) {
		use_ctid_scan = BooleanValue::Get(pg_use_ctid_scan);
	}
	if (version.major_v < 14) {
		// Disable parallel CTID scan on older Postgres versions since it is not efficient
		// see https://github.com/duckdb/postgres_scanner/issues/186
		use_ctid_scan = false;
	}
	if (!use_ctid_scan) {
		approx_num_pages = 0;
	}
	bind_data.SetTablePages(approx_num_pages);
	bind_data.version = version;
}

void PostgresBindData::SetTablePages(idx_t approx_num_pages) {
	this->pages_approx = approx_num_pages;
	if (!read_only) {
		max_threads = 1;
	} else {
		max_threads = MaxValue<idx_t>(pages_approx / pages_per_task, 1);
	}
}

PostgresConnection &PostgresGlobalState::GetConnection() {
	return connection;
}

void PostgresGlobalState::SetConnection(PostgresConnection connection) {
	this->connection = std::move(connection);
}

void PostgresGlobalState::SetConnection(shared_ptr<OwnedPostgresConnection> connection) {
	this->connection = PostgresConnection(std::move(connection));
}

void PostgresBindData::SetCatalog(PostgresCatalog &catalog) {
	this->pg_catalog = &catalog;
}

static unique_ptr<FunctionData> PostgresBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<PostgresBindData>();

	bind_data->dsn = input.inputs[0].GetValue<string>();
	bind_data->schema_name = input.inputs[1].GetValue<string>();
	bind_data->table_name = input.inputs[2].GetValue<string>();

	auto con = PostgresConnection::Open(bind_data->dsn);
	auto version = con.GetPostgresVersion();
	// query the table schema so we can interpret the bits in the pages
	auto info = PostgresTableSet::GetTableInfo(con, bind_data->schema_name, bind_data->table_name);

	bind_data->postgres_types = info->postgres_types;
	for (auto &col : info->create_info->columns.Logical()) {
		names.push_back(col.GetName());
		return_types.push_back(col.GetType());
	}
	bind_data->names = info->postgres_names;
	bind_data->types = return_types;
	bind_data->can_use_main_thread = true;
	bind_data->requires_materialization = false;

	PostgresScanFunction::PrepareBind(version, context, *bind_data, info->approx_num_pages);
	return std::move(bind_data);
}

static bool ContainsCastToVarchar(const PostgresType &type) {
	if (type.info == PostgresTypeAnnotation::CAST_TO_VARCHAR) {
		return true;
	}
	for (auto &child : type.children) {
		if (ContainsCastToVarchar(child)) {
			return true;
		}
	}
	return false;
}

static void PostgresInitInternal(ClientContext &context, const PostgresBindData *bind_data_p,
                                 PostgresLocalState &lstate, idx_t task_min, idx_t task_max) {
	D_ASSERT(bind_data_p);
	D_ASSERT(task_min <= task_max);

	auto bind_data = (const PostgresBindData *)bind_data_p;

	string col_names;
	for (auto &column_id : lstate.column_ids) {
		if (!col_names.empty()) {
			col_names += ", ";
		}
		if (column_id == COLUMN_IDENTIFIER_ROW_ID) {
			if (bind_data->table_name.empty() || !bind_data->emit_ctid) {
				// count(*) over postgres_query
				col_names += "NULL";
			} else {
				col_names += "ctid";
			}
		} else {
			col_names += KeywordHelper::WriteQuoted(bind_data->names[column_id], '"');
			if (bind_data->postgres_types[column_id].info == PostgresTypeAnnotation::CAST_TO_VARCHAR) {
				col_names += "::VARCHAR";
			} else if (bind_data->types[column_id].id() == LogicalTypeId::LIST) {
				if (bind_data->postgres_types[column_id].info != PostgresTypeAnnotation::STANDARD) {
					continue;
				}
				if (bind_data->postgres_types[column_id].children[0].info == PostgresTypeAnnotation::CAST_TO_VARCHAR) {
					col_names += "::VARCHAR[]";
				}
			} else {
				if (ContainsCastToVarchar(bind_data->postgres_types[column_id])) {
					throw NotImplementedException("Error reading table \"%s\" - cast to varchar not implemented for "
					                              "composite column \"%s\" (type %s)",
					                              bind_data->table_name, bind_data->names[column_id],
					                              bind_data->types[column_id].ToString());
				}
			}
		}
	}

	string filter_string =
	    PostgresFilterPushdown::TransformFilters(lstate.column_ids, lstate.filters, bind_data->names);

	string filter;
	if (bind_data->pages_approx > 0) {
		filter = StringUtil::Format("WHERE ctid BETWEEN '(%d,0)'::tid AND '(%d,0)'::tid", task_min, task_max);
	}
	if (!filter_string.empty()) {
		if (filter.empty()) {
			filter += "WHERE ";
		} else {
			filter += " AND ";
		}
		filter += filter_string;
	}
	if (bind_data->table_name.empty()) {
		D_ASSERT(!bind_data->sql.empty());
		lstate.sql = StringUtil::Format(
		    R"(
	COPY (SELECT %s FROM (%s) AS __unnamed_subquery %s) TO STDOUT (FORMAT "binary");
	)",
		    col_names, bind_data->sql, filter);

	} else {
		lstate.sql = StringUtil::Format(
		    R"(
	COPY (SELECT %s FROM %s.%s %s) TO STDOUT (FORMAT "binary");
	)",
		    col_names, KeywordHelper::WriteQuoted(bind_data->schema_name, '"'),
		    KeywordHelper::WriteQuoted(bind_data->table_name, '"'), filter);
	}
	lstate.exec = false;
	lstate.done = false;
}

static idx_t PostgresMaxThreads(ClientContext &context, const FunctionData *bind_data_p) {
	D_ASSERT(bind_data_p);
	auto &bind_data = bind_data_p->Cast<PostgresBindData>();
	if (bind_data.requires_materialization) {
		return 1;
	}
	return bind_data.max_threads;
}

static unique_ptr<LocalTableFunctionState> GetLocalState(ClientContext &context, TableFunctionInitInput &input,
                                                         PostgresGlobalState &gstate);

static void PostgresScanConnect(PostgresConnection &conn, string snapshot) {
	conn.Execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY");
	if (!snapshot.empty()) {
		conn.Query(StringUtil::Format("SET TRANSACTION SNAPSHOT '%s'", snapshot));
	}
}

static unique_ptr<GlobalTableFunctionState> PostgresInitGlobalState(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<PostgresBindData>();
	auto result = make_uniq<PostgresGlobalState>(PostgresMaxThreads(context, input.bind_data.get()));
	auto pg_catalog = bind_data.GetCatalog();
	if (pg_catalog) {
		auto &transaction = Transaction::Get(context, *pg_catalog).Cast<PostgresTransaction>();
		auto &con = transaction.GetConnection();
		result->SetConnection(con.GetConnection());
	} else {
		auto con = PostgresConnection::Open(bind_data.dsn);
		PostgresScanConnect(con, string());
		result->SetConnection(std::move(con));
	}
	if (bind_data.requires_materialization) {
		// if requires_materialization is enabled we scan and materialize the table in its entirety up-front
		vector<LogicalType> types;
		for (auto column_id : input.column_ids) {
			types.push_back(column_id == COLUMN_IDENTIFIER_ROW_ID ? LogicalType::BIGINT : bind_data.types[column_id]);
		}
		auto materialized = make_uniq<ColumnDataCollection>(Allocator::Get(context), types);
		DataChunk scan_chunk;
		scan_chunk.Initialize(Allocator::Get(context), types);

		auto local_state = GetLocalState(context, input, *result);
		auto &lstate = local_state->Cast<PostgresLocalState>();
		ColumnDataAppendState append_state;
		materialized->InitializeAppend(append_state);
		while (true) {
			scan_chunk.Reset();
			lstate.ScanChunk(context, bind_data, *result, scan_chunk);
			if (scan_chunk.size() == 0) {
				break;
			}
			materialized->Append(append_state, scan_chunk);
		}
		result->collection = std::move(materialized);
		result->collection->InitializeScan(result->scan_state);
	} else {
		// we create a transaction here, and get the snapshot id to enable transaction-safe parallelism
		PostgresGetSnapshot(bind_data, *result);
	}
	return std::move(result);
}

static bool PostgresParallelStateNext(ClientContext &context, const FunctionData *bind_data_p,
                                      PostgresLocalState &lstate, PostgresGlobalState &gstate) {
	D_ASSERT(bind_data_p);
	auto bind_data = (const PostgresBindData *)bind_data_p;

	lock_guard<mutex> parallel_lock(gstate.lock);
	lstate.batch_idx = gstate.batch_idx++;
	if (gstate.page_idx < bind_data->pages_approx) {
		auto page_max = gstate.page_idx + bind_data->pages_per_task;
		if (page_max >= bind_data->pages_approx) {
			// the relpages entry is not the real max, so make the last task bigger
			page_max = POSTGRES_TID_MAX;
		}

		PostgresInitInternal(context, bind_data, lstate, gstate.page_idx, page_max);
		gstate.page_idx = page_max;
		return true;
	}
	lstate.done = true;
	return false;
}

bool PostgresGlobalState::TryOpenNewConnection(ClientContext &context, PostgresLocalState &lstate,
                                               const PostgresBindData &bind_data) {
	auto pg_catalog = bind_data.GetCatalog();
	{
		lock_guard<mutex> parallel_lock(lock);
		if (!used_main_thread) {
			if (bind_data.can_use_main_thread) {
				lstate.connection = PostgresConnection(GetConnection().GetConnection());
			} else {
				// we cannot use the main thread but we haven't initiated ANY scan yet
				// we HAVE to open a new connection
				lstate.pool_connection = pg_catalog->GetConnectionPool().ForceGetConnection();
				lstate.connection = PostgresConnection(lstate.pool_connection.GetConnection().GetConnection());
			}
			used_main_thread = true;
			return true;
		}
	}

	if (pg_catalog) {
		if (!pg_catalog->GetConnectionPool().TryGetConnection(lstate.pool_connection)) {
			return false;
		}
		lstate.connection = PostgresConnection(lstate.pool_connection.GetConnection().GetConnection());
	} else {
		lstate.connection = PostgresConnection::Open(bind_data.dsn);
	}
	PostgresScanConnect(lstate.connection, snapshot);
	return true;
}

static unique_ptr<LocalTableFunctionState> GetLocalState(ClientContext &context, TableFunctionInitInput &input,
                                                         PostgresGlobalState &gstate) {
	auto &bind_data = (PostgresBindData &)*input.bind_data;

	auto local_state = make_uniq<PostgresLocalState>();
	if (gstate.collection) {
		return std::move(local_state);
	}
	local_state->column_ids = input.column_ids;

	local_state->filters = input.filters.get();
	if (!gstate.TryOpenNewConnection(context, *local_state, bind_data)) {
		// if the connection pool is exhausted we bail-out
		local_state->no_connection = true;
		return std::move(local_state);
	}
	if (bind_data.pages_approx == 0 || bind_data.requires_materialization) {
		PostgresInitInternal(context, &bind_data, *local_state, 0, POSTGRES_TID_MAX);
		gstate.page_idx = POSTGRES_TID_MAX;
	} else if (!PostgresParallelStateNext(context, input.bind_data.get(), *local_state, gstate)) {
		local_state->done = true;
	}
	return std::move(local_state);
}

static unique_ptr<LocalTableFunctionState> PostgresInitLocalState(ExecutionContext &context,
                                                                  TableFunctionInitInput &input,
                                                                  GlobalTableFunctionState *global_state) {
	auto &gstate = global_state->Cast<PostgresGlobalState>();
	return GetLocalState(context.client, input, gstate);
}

void PostgresLocalState::ScanChunk(ClientContext &context, const PostgresBindData &bind_data,
                                   PostgresGlobalState &gstate, DataChunk &output) {
	idx_t output_offset = 0;
	PostgresBinaryReader reader(connection);
	while (true) {
		if (done && !PostgresParallelStateNext(context, &bind_data, *this, gstate)) {
			return;
		}
		if (!exec) {
			connection.BeginCopyFrom(reader, sql);
			exec = true;
		}

		output.SetCardinality(output_offset);
		if (output_offset == STANDARD_VECTOR_SIZE) {
			return;
		}

		while (!reader.Ready()) {
			if (!reader.Next()) {
				// finished this batch
				reader.CheckResult();
				done = true;
				continue;
			}
		}

		auto tuple_count = reader.ReadInteger<int16_t>();
		if (tuple_count <= 0) { // done here, lets try to get more
			reader.Reset();
			done = true;
			continue;
		}

		D_ASSERT(tuple_count == column_ids.size());

		for (idx_t output_idx = 0; output_idx < output.ColumnCount(); output_idx++) {
			auto col_idx = column_ids[output_idx];
			auto &out_vec = output.data[output_idx];
			if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
				// row id
				// ctid in postgres are a composite type of (page_index, tuple_in_page)
				// the page index is a 4-byte integer, the tuple_in_page a 2-byte integer
				PostgresType ctid_type;
				ctid_type.info = PostgresTypeAnnotation::CTID;
				reader.ReadValue(LogicalType::BIGINT, ctid_type, out_vec, output_offset);
			} else {
				reader.ReadValue(bind_data.types[col_idx], bind_data.postgres_types[col_idx], out_vec, output_offset);
			}
		}
		reader.Reset();
		output_offset++;
	}
}

static void PostgresScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<PostgresBindData>();
	auto &gstate = data.global_state->Cast<PostgresGlobalState>();

	if (gstate.collection) {
		gstate.collection->Scan(gstate.scan_state, output);
		return;
	}
	auto &local_state = data.local_state->Cast<PostgresLocalState>();
	if (local_state.no_connection) {
		return;
	}
	local_state.ScanChunk(context, bind_data, gstate, output);
}

static idx_t PostgresScanBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
                                    LocalTableFunctionState *local_state_p, GlobalTableFunctionState *global_state) {
	auto &bind_data = bind_data_p->Cast<PostgresBindData>();
	auto &local_state = local_state_p->Cast<PostgresLocalState>();
	return local_state.batch_idx;
}

static string PostgresScanToString(const FunctionData *bind_data_p) {
	D_ASSERT(bind_data_p);
	auto &bind_data = bind_data_p->Cast<PostgresBindData>();
	return bind_data.table_name;
}

unique_ptr<NodeStatistics> PostgresScanCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<PostgresBindData>();
	// see https://www.postgresql.org/docs/current/storage-page-layout.html
	// pages are 8KB
	// every page has ~24 bytes of overhead
	constexpr static idx_t PAGE_METADATA_SIZE = 23;
	constexpr static idx_t POSTGRES_PAGE_SIZE = 8192 - PAGE_METADATA_SIZE;
	// every row has ~23 bytes of overhead in the header
	constexpr static idx_t ROW_META_DATA_SIZE = 23;
	// for simplicity we assume every column is 8 bytes on average
	auto row_size = ROW_META_DATA_SIZE + bind_data.types.size() * 8;
	auto rows_per_page = MaxValue<idx_t>(1, POSTGRES_PAGE_SIZE / row_size);
	auto estimated_cardinality = bind_data.pages_approx * rows_per_page;
	return make_uniq<NodeStatistics>(estimated_cardinality);
}

double PostgresScanProgress(ClientContext &context, const FunctionData *bind_data_p,
                            const GlobalTableFunctionState *global_state) {
	auto &bind_data = bind_data_p->Cast<PostgresBindData>();
	auto &gstate = global_state->Cast<PostgresGlobalState>();

	lock_guard<mutex> parallel_lock(gstate.lock);
	double progress = 100 * double(gstate.page_idx) / double(bind_data.pages_approx);
	return MinValue<double>(100, progress);
}

static void PostgresScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                                  const TableFunction &function) {
	throw NotImplementedException("PostgresScanSerialize");
}

static unique_ptr<FunctionData> PostgresScanDeserialize(Deserializer &deserializer, TableFunction &function) {
	throw NotImplementedException("PostgresScanDeserialize");
}

PostgresScanFunction::PostgresScanFunction()
    : TableFunction("postgres_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, PostgresScan,
                    PostgresBind, PostgresInitGlobalState, PostgresInitLocalState) {
	to_string = PostgresScanToString;
	serialize = PostgresScanSerialize;
	deserialize = PostgresScanDeserialize;
	get_batch_index = PostgresScanBatchIndex;
	cardinality = PostgresScanCardinality;
	table_scan_progress = PostgresScanProgress;
	projection_pushdown = true;
	global_initialization = TableFunctionInitialization::INITIALIZE_ON_SCHEDULE;
}

PostgresScanFunctionFilterPushdown::PostgresScanFunctionFilterPushdown()
    : TableFunction("postgres_scan_pushdown", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                    PostgresScan, PostgresBind, PostgresInitGlobalState, PostgresInitLocalState) {
	to_string = PostgresScanToString;
	serialize = PostgresScanSerialize;
	deserialize = PostgresScanDeserialize;
	get_batch_index = PostgresScanBatchIndex;
	cardinality = PostgresScanCardinality;
	table_scan_progress = PostgresScanProgress;
	projection_pushdown = true;
	filter_pushdown = true;
	global_initialization = TableFunctionInitialization::INITIALIZE_ON_SCHEDULE;
}

} // namespace duckdb
