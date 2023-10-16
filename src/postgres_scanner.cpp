#include "duckdb.hpp"

#include <libpq-fe.h>

#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "postgres_filter_pushdown.hpp"
#include "postgres_scanner.hpp"
#include "postgres_result.hpp"
#include "postgres_binary_reader.hpp"
#include "storage/postgres_transaction.hpp"

namespace duckdb {

static constexpr uint32_t POSTGRES_TID_MAX = 4294967295;

struct PostgresGlobalState;

struct PostgresLocalState : public LocalTableFunctionState {
	bool done = false;
	bool exec = false;
	string sql;
	vector<column_t> column_ids;
	TableFilterSet *filters;
	string col_names;
	PostgresConnection connection;

	void ScanChunk(ClientContext &context, const PostgresBindData &bind_data, PostgresGlobalState &gstate, DataChunk &output);
};

struct PostgresGlobalState : public GlobalTableFunctionState {
	explicit PostgresGlobalState(idx_t max_threads)
	    : page_idx(0), max_threads(max_threads) {
	}

	mutex lock;
	idx_t page_idx;
	idx_t max_threads;
	unique_ptr<ColumnDataCollection> collection;
	ColumnDataScanState scan_state;

	idx_t MaxThreads() const override {
		return max_threads;
	}
};

static LogicalType DuckDBType2(PostgresTypeInfo *type_info, int atttypmod, PostgresTypeInfo *ele_info, PostgresConnection &con,
                               ClientContext &context, PostgresType &postgres_type) {
	auto &pgtypename = type_info->typname;

	// TODO better check, does the typtyp say something here?
	// postgres array types start with an _
	if (StringUtil::StartsWith(pgtypename, "_")) {
		PostgresType child_type;
		auto result = LogicalType::LIST(DuckDBType2(ele_info, atttypmod, nullptr, con, context, child_type));
		postgres_type.children.push_back(std::move(child_type));
		return result;
	}

	if (type_info->typtype == "e") { // ENUM
		auto res = con.Query(
		    StringUtil::Format("SELECT unnest(enum_range(NULL::%s.%s))", type_info->nspname, type_info->typname));
		Vector duckdb_levels(LogicalType::VARCHAR, res->Count());
		for (idx_t row = 0; row < res->Count(); row++) {
			duckdb_levels.SetValue(row, res->GetString(row, 0));
		}
		return LogicalType::ENUM("postgres_enum_" + pgtypename, duckdb_levels, res->Count());
	}

	if (pgtypename == "bool") {
		return LogicalType::BOOLEAN;
	} else if (pgtypename == "int2") {
		return LogicalType::SMALLINT;
	} else if (pgtypename == "int4") {
		return LogicalType::INTEGER;
	} else if (pgtypename == "int8") {
		return LogicalType::BIGINT;
	} else if (pgtypename == "oid") { // "The oid type is currently implemented as an unsigned four-byte integer."
		return LogicalType::UINTEGER;
	} else if (pgtypename == "float4") {
		return LogicalType::FLOAT;
	} else if (pgtypename == "float8") {
		return LogicalType::DOUBLE;
	} else if (pgtypename == "numeric") {
		if (atttypmod == -1) { // unbounded decimal/numeric, will just return as double
			postgres_type.info = PostgresTypeAnnotation::NUMERIC_AS_DOUBLE;
			return LogicalType::DOUBLE;
		}
		auto width = ((atttypmod - sizeof(int32_t)) >> 16) & 0xffff;
		auto scale = (((atttypmod - sizeof(int32_t)) & 0x7ff) ^ 1024) - 1024;
		return LogicalType::DECIMAL(width, scale);
	} else if (pgtypename == "char" || pgtypename == "bpchar" || pgtypename == "varchar" || pgtypename == "text" ||
	           pgtypename == "json") {
		return LogicalType::VARCHAR;
	} else if (pgtypename == "jsonb") {
		postgres_type.info = PostgresTypeAnnotation::JSONB;
		return LogicalType::VARCHAR;
	} else if (pgtypename == "date") {
		return LogicalType::DATE;
	} else if (pgtypename == "bytea") {
		return LogicalType::BLOB;
	} else if (pgtypename == "time") {
		return LogicalType::TIME;
	} else if (pgtypename == "timetz") {
		return LogicalType::TIME_TZ;
	} else if (pgtypename == "timestamp") {
		return LogicalType::TIMESTAMP;
	} else if (pgtypename == "timestamptz") {
		return LogicalType::TIMESTAMP_TZ;
	} else if (pgtypename == "interval") {
		return LogicalType::INTERVAL;
	} else if (pgtypename == "uuid") {
		return LogicalType::UUID;
	} else {
		postgres_type.info = PostgresTypeAnnotation::CAST_TO_VARCHAR;
		return LogicalType::VARCHAR;
	}
}

static LogicalType DuckDBType(PostgresColumnInfo &info, PostgresConnection &con, ClientContext &context, PostgresType &postgres_type) {
	return DuckDBType2(&info.type_info, info.atttypmod, &info.elem_info, con, context, postgres_type);
}

int64_t PostgresScanFunction::PrepareBind(ClientContext &context, PostgresBindData &bind_data) {
	// we create a transaction here, and get the snapshot id so the parallel
	// reader threads can use the same snapshot
	auto &con = bind_data.connection;
	auto result = con.Query("SELECT pg_is_in_recovery(), pg_export_snapshot()");
	bind_data.in_recovery = result->GetBool(0, 0);
	bind_data.snapshot = "";

	if (!bind_data.in_recovery) {
		bind_data.snapshot = result->GetString(0, 1);
	}

	// find the id of the table in question to simplify below queries and avoid
	// complex joins (ha)
	auto res = con.Query(StringUtil::Format(R"(
SELECT pg_class.oid, relpages
FROM pg_class JOIN pg_namespace ON relnamespace = pg_namespace.oid
WHERE nspname=%s AND relname=%s
)", KeywordHelper::WriteQuoted(bind_data.schema_name), KeywordHelper::WriteQuoted(bind_data.table_name)));
	if (res->Count() != 1) {
		throw InvalidInputException("Postgres table \"%s\".\"%s\" not found", bind_data.schema_name,
		                            bind_data.table_name);
	}
	auto oid = res->GetInt64(0, 0);
	bind_data.pages_approx = res->GetInt64(0, 1);
	return oid;
}

static unique_ptr<FunctionData> PostgresBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<PostgresBindData>();

	bind_data->dsn = input.inputs[0].GetValue<string>();
	bind_data->schema_name = input.inputs[1].GetValue<string>();
	bind_data->table_name = input.inputs[2].GetValue<string>();

	bind_data->connection = PostgresConnection::Open(bind_data->dsn);

	bind_data->connection.Execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY");
	auto oid = PostgresScanFunction::PrepareBind(context, *bind_data);

	// query the table schema so we can interpret the bits in the pages
	// fun fact: this query also works in DuckDB ^^
	auto query = StringUtil::Format(R"(
SELECT
    attname, atttypmod, pg_namespace.nspname,
    pg_type.typname, pg_type.typlen, pg_type.typtype, pg_type.typelem,
    pg_type_elem.typname elem_typname, pg_type_elem.typlen elem_typlen, pg_type_elem.typtype elem_typtype
FROM pg_attribute
    JOIN pg_type ON atttypid=pg_type.oid
    LEFT JOIN pg_type pg_type_elem ON pg_type.typelem=pg_type_elem.oid
    LEFT JOIN pg_namespace ON pg_type.typnamespace = pg_namespace.oid
WHERE attrelid=%d AND attnum > 0
ORDER BY attnum;
)", oid);
	auto res = bind_data->connection.Query(query);

	// can't scan a table without columns (yes those exist)
	if (res->Count() == 0) {
		throw InvalidInputException("Table %s does not contain any columns.", bind_data->table_name);
	}

	for (idx_t row = 0; row < res->Count(); row++) {
		PostgresColumnInfo info;
		info.attname = res->GetString(row, 0);
		info.atttypmod = res->GetInt32(row, 1);

		info.type_info.nspname = res->GetString(row, 2);
		info.type_info.typname = res->GetString(row, 3);
		info.type_info.typlen = res->GetInt64(row, 4);
		info.type_info.typtype = res->GetString(row, 5);
		info.typelem = res->GetInt64(row, 6);

		info.elem_info.nspname = res->GetString(row, 2);
		info.elem_info.typname = res->GetString(row, 7);
		info.elem_info.typlen = res->GetInt64(row, 8);
		info.elem_info.typtype = res->GetString(row, 9);

		bind_data->names.push_back(info.attname);
		PostgresType type;
		auto duckdb_type = DuckDBType(info, bind_data->connection, context, type);

		// we cast unsupported types to varchar on read
		auto needs_cast = duckdb_type == LogicalType::INVALID;
		if (!needs_cast) {
			bind_data->types.push_back(std::move(duckdb_type));
		} else {
			bind_data->types.push_back(LogicalType::VARCHAR);
		}
		bind_data->postgres_types.push_back(type);
	}

	return_types = bind_data->types;
	names = bind_data->names;

	return std::move(bind_data);
}

static void PostgresInitInternal(ClientContext &context, const PostgresBindData *bind_data_p,
                                 PostgresLocalState &lstate, idx_t task_min, idx_t task_max) {
	D_ASSERT(bind_data_p);
	D_ASSERT(task_min <= task_max);

	auto bind_data = (const PostgresBindData *)bind_data_p;

	string col_names;
	for(auto &column_id : lstate.column_ids) {
		if (!col_names.empty()) {
			col_names += ", ";
		}
		if (column_id == COLUMN_IDENTIFIER_ROW_ID) {
			col_names += "ctid";
		} else {
			col_names += KeywordHelper::WriteQuoted(bind_data->names[column_id], '"');
			if (bind_data->postgres_types[column_id].info == PostgresTypeAnnotation::CAST_TO_VARCHAR) {
				col_names += "::VARCHAR";
			}
			if (bind_data->types[column_id].id() == LogicalTypeId::LIST) {
				if (bind_data->postgres_types[column_id].children[0].info == PostgresTypeAnnotation::CAST_TO_VARCHAR) {
					col_names += "::VARCHAR[]";
				}
			}
		}
	}

	string filter_string= PostgresFilterPushdown::TransformFilters(lstate.column_ids, lstate.filters, bind_data->names);

	string filter;
	if (bind_data->pages_approx > 0) {
		filter = StringUtil::Format("WHERE ctid BETWEEN '(%d,0)'::tid AND '(%d,0)'::tid", task_min, task_max);
	}
	if (!filter_string.empty()) {
		if (filter.empty()) {
			filter += "WHERE ";
		}
		filter += filter_string;
	}
	lstate.sql = StringUtil::Format(
	    R"(
COPY (SELECT %s FROM %s.%s %s) TO STDOUT (FORMAT binary);
)",
	    col_names, KeywordHelper::WriteQuoted(bind_data->schema_name, '"'), KeywordHelper::WriteQuoted(bind_data->table_name, '"'), filter);

	lstate.exec = false;
	lstate.done = false;
}

static PostgresConnection PostgresScanConnect(string dsn, bool in_recovery, string snapshot) {
	auto conn = PostgresConnection::Open(dsn);
	conn.Execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY");
	if (!in_recovery) {
		conn.Execute(StringUtil::Format("SET TRANSACTION SNAPSHOT '%s'", snapshot));
	}
	return conn;
}

static void ProcessValue(const LogicalType &type, const PostgresType &postgres_type, PostgresBinaryReader &reader, idx_t value_len,
                         Vector &out_vec, idx_t output_offset) {
	switch (type.id()) {
	case LogicalTypeId::SMALLINT:
		D_ASSERT(value_len == sizeof(int16_t));
		FlatVector::GetData<int16_t>(out_vec)[output_offset] = reader.ReadInteger<int16_t>();
		break;

	case LogicalTypeId::INTEGER:
		D_ASSERT(value_len == sizeof(int32_t));
		FlatVector::GetData<int32_t>(out_vec)[output_offset] = reader.ReadInteger<int32_t>();
		break;

	case LogicalTypeId::UINTEGER:
		D_ASSERT(value_len == sizeof(uint32_t));
		FlatVector::GetData<uint32_t>(out_vec)[output_offset] = reader.ReadInteger<uint32_t>();
		break;

	case LogicalTypeId::BIGINT:
		D_ASSERT(value_len == sizeof(int64_t));
		FlatVector::GetData<int64_t>(out_vec)[output_offset] = reader.ReadInteger<int64_t>();
		break;

	case LogicalTypeId::FLOAT: {
		D_ASSERT(value_len == sizeof(float));
		FlatVector::GetData<float>(out_vec)[output_offset] = reader.ReadFloat();
		break;
	}

	case LogicalTypeId::DOUBLE: {
		// this was an unbounded decimal, read params from value and cast to double
		if (postgres_type.info == PostgresTypeAnnotation::NUMERIC_AS_DOUBLE) {
			FlatVector::GetData<double>(out_vec)[output_offset] = reader.ReadDecimal<double, DecimalConversionDouble>();
			break;
		}
		D_ASSERT(value_len == sizeof(double));
		FlatVector::GetData<double>(out_vec)[output_offset] = reader.ReadDouble();
		break;
	}

	case LogicalTypeId::BLOB:
	case LogicalTypeId::VARCHAR: {
		if (postgres_type.info == PostgresTypeAnnotation::JSONB) {
			auto version = reader.ReadInteger<uint8_t>();
			value_len--;
			if (version != 1) {
				throw NotImplementedException("JSONB version number mismatch, expected 1, got %d", version);
			}
		}
		FlatVector::GetData<string_t>(out_vec)[output_offset] =
		    StringVector::AddStringOrBlob(out_vec, reader.ReadString(value_len), value_len);
		break;
	}
	case LogicalTypeId::BOOLEAN:
		D_ASSERT(value_len == sizeof(bool));
		FlatVector::GetData<bool>(out_vec)[output_offset] = reader.ReadBoolean();
		break;
	case LogicalTypeId::DECIMAL: {
		if (value_len < sizeof(uint16_t) * 4) {
			throw InvalidInputException("Need at least 8 bytes to read a Postgres decimal. Got %d", value_len);
		}
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			FlatVector::GetData<int16_t>(out_vec)[output_offset] = reader.ReadDecimal<int16_t>();
			break;
		case PhysicalType::INT32:
			FlatVector::GetData<int32_t>(out_vec)[output_offset] = reader.ReadDecimal<int32_t>();
			break;
		case PhysicalType::INT64:
			FlatVector::GetData<int64_t>(out_vec)[output_offset] = reader.ReadDecimal<int64_t>();
			break;
		case PhysicalType::INT128:
			FlatVector::GetData<hugeint_t>(out_vec)[output_offset] = reader.ReadDecimal<hugeint_t, DecimalConversionHugeint>();
			break;
		default:
			throw InvalidInputException("Unsupported decimal storage type");
		}
		break;
	}

	case LogicalTypeId::DATE: {
		D_ASSERT(value_len == sizeof(int32_t));
		auto out_ptr = FlatVector::GetData<date_t>(out_vec);
		out_ptr[output_offset] = reader.ReadDate();
		break;
	}
	case LogicalTypeId::TIME: {
		D_ASSERT(value_len == sizeof(int64_t));
		FlatVector::GetData<dtime_t>(out_vec)[output_offset] = reader.ReadTime();
		break;
	}
	case LogicalTypeId::TIME_TZ: {
		D_ASSERT(value_len == sizeof(int64_t) + sizeof(int32_t));
		FlatVector::GetData<dtime_tz_t>(out_vec)[output_offset] = reader.ReadTimeTZ();
		break;
	}
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP: {
		D_ASSERT(value_len == sizeof(int64_t));
		FlatVector::GetData<timestamp_t>(out_vec)[output_offset] = reader.ReadTimestamp();
		break;
	}
	case LogicalTypeId::ENUM: {
		auto enum_val = string(reader.ReadString(value_len), value_len);
		auto offset = EnumType::GetPos(type, enum_val);
		if (offset < 0) {
			throw IOException("Could not map ENUM value %s", enum_val);
		}
		switch (type.InternalType()) {
		case PhysicalType::UINT8:
			FlatVector::GetData<uint8_t>(out_vec)[output_offset] = (uint8_t)offset;
			break;
		case PhysicalType::UINT16:
			FlatVector::GetData<uint16_t>(out_vec)[output_offset] = (uint16_t)offset;
			break;

		case PhysicalType::UINT32:
			FlatVector::GetData<uint32_t>(out_vec)[output_offset] = (uint32_t)offset;
			break;

		default:
			throw InternalException("ENUM can only have unsigned integers (except "
			                        "UINT64) as physical types, got %s",
			                        TypeIdToString(type.InternalType()));
		}
		break;
	}
	case LogicalTypeId::INTERVAL: {
		FlatVector::GetData<interval_t>(out_vec)[output_offset] = reader.ReadInterval();
		break;
	}

	case LogicalTypeId::UUID: {
		D_ASSERT(value_len == 2 * sizeof(int64_t));
		FlatVector::GetData<hugeint_t>(out_vec)[output_offset] = reader.ReadUUID();
		break;
	}
	case LogicalTypeId::LIST: {
		auto &list_entry = FlatVector::GetData<list_entry_t>(out_vec)[output_offset];
		auto child_offset = ListVector::GetListSize(out_vec);

		if (value_len < 1) {
			list_entry.offset = child_offset;
			list_entry.length = 0;
			break;
		}
		D_ASSERT(value_len >= 3 * sizeof(uint32_t));
		auto flag_one = reader.ReadInteger<uint32_t>();
		auto flag_two = reader.ReadInteger<uint32_t>();
		auto value_oid = reader.ReadInteger<uint32_t>();
		if (flag_one == 0) {
			list_entry.offset = child_offset;
			list_entry.length = 0;
			return;
		}
		// D_ASSERT(flag_two == 1); // TODO what is this?!
		auto array_length = reader.ReadInteger<uint32_t>();
		auto array_dim = reader.ReadInteger<uint32_t>();
		if (array_dim != 1) {
			throw NotImplementedException("Only one-dimensional Postgres arrays are supported %u %u ", array_length,
			                              array_dim);
		}

		auto &child_vec = ListVector::GetEntry(out_vec);
		ListVector::Reserve(out_vec, child_offset + array_length);
		for (idx_t child_idx = 0; child_idx < array_length; child_idx++) {
			// handle NULLs again (TODO: unify this with scan)
			auto ele_len = reader.ReadInteger<int32_t>();
			if (ele_len == -1) { // NULL
				FlatVector::Validity(child_vec).Set(child_offset + child_idx, false);
				continue;
			}

			ProcessValue(ListType::GetChildType(type), postgres_type.children[0], reader, ele_len, child_vec,
			             child_offset + child_idx);
		}
		ListVector::SetListSize(out_vec, child_offset + array_length);

		list_entry.offset = child_offset;
		list_entry.length = array_length;
		break;
	}
	case LogicalTypeId::STRUCT: {
		auto &child_entries = StructVector::GetEntries(out_vec);
		auto entry_count = reader.ReadInteger<uint32_t>();
		if (entry_count != child_entries.size()) {
			throw InternalException("Mismatch in entry count: expected %d but got %d", child_entries.size(), entry_count);
		}
		for(idx_t c = 0; c < entry_count; c++) {
			auto &child = *child_entries[c];
			auto value_oid = reader.ReadInteger<uint32_t>();
			auto ele_len = reader.ReadInteger<int32_t>();
			if (ele_len == -1) { // NULL
				FlatVector::Validity(child).Set(output_offset, false);
				continue;
			}
			ProcessValue(child.GetType(), postgres_type.children[c], reader, ele_len, child, output_offset);
		}
		break;
	}
	default:
		throw InternalException("Unsupported Type %s", type.ToString());
	}
}

static idx_t PostgresMaxThreads(ClientContext &context, const FunctionData *bind_data_p) {
	D_ASSERT(bind_data_p);

	// FIXME - minor cleanup, should not need to remove const-ness here
	auto bind_data = (PostgresBindData *)bind_data_p;
	if (bind_data->requires_materialization) {
		return 1;
	}
	if (bind_data->transaction && !bind_data->transaction->IsReadOnly()) {
		return 1;
	}
	return MaxValue<idx_t>(bind_data->pages_approx / bind_data->pages_per_task, 1);
}

static unique_ptr<LocalTableFunctionState> GetLocalState(ClientContext &context, TableFunctionInitInput &input, PostgresGlobalState &gstate);

static unique_ptr<GlobalTableFunctionState> PostgresInitGlobalState(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<PostgresBindData>();
	auto result = make_uniq<PostgresGlobalState>(PostgresMaxThreads(context, input.bind_data.get()));
	if (bind_data.requires_materialization) {
		// if requires_materialization is enabled we scan and materialize the table in its entirety up-front
		vector<LogicalType> types;
		for(auto column_id : input.column_ids) {
			types.push_back(column_id == COLUMN_IDENTIFIER_ROW_ID ? LogicalType::BIGINT : bind_data.types[column_id]);
		}
		auto materialized = make_uniq<ColumnDataCollection>(Allocator::Get(context), types);
		DataChunk scan_chunk;
		scan_chunk.Initialize(Allocator::Get(context), types);

		auto local_state = GetLocalState(context, input, *result);
		auto &lstate = local_state->Cast<PostgresLocalState>();
		ColumnDataAppendState append_state;
		materialized->InitializeAppend(append_state);
		while(true) {
			scan_chunk.Reset();
			lstate.ScanChunk(context, bind_data, *result, scan_chunk);
			if (scan_chunk.size() == 0) {
				break;
			}
			materialized->Append(append_state, scan_chunk);
		}
		result->collection = std::move(materialized);
		result->collection->InitializeScan(result->scan_state);
	}
	return result;
}

static bool PostgresParallelStateNext(ClientContext &context, const FunctionData *bind_data_p,
                                      PostgresLocalState &lstate, PostgresGlobalState &gstate) {
	D_ASSERT(bind_data_p);
	auto bind_data = (const PostgresBindData *)bind_data_p;

	lock_guard<mutex> parallel_lock(gstate.lock);
	if (gstate.page_idx < bind_data->pages_approx) {
		auto page_max = gstate.page_idx + bind_data->pages_per_task;
		if (page_max >= bind_data->pages_approx) {
			// the relpages entry is not the real max, so make the last task bigger
			page_max = POSTGRES_TID_MAX;
		}

		PostgresInitInternal(context, bind_data, lstate, gstate.page_idx, page_max);
		gstate.page_idx += bind_data->pages_per_task;
		return true;
	}
	lstate.done = true;
	return false;
}

static unique_ptr<LocalTableFunctionState> GetLocalState(ClientContext &context, TableFunctionInitInput &input, PostgresGlobalState &gstate) {
	auto &bind_data = (PostgresBindData &) *input.bind_data;

	auto local_state = make_uniq<PostgresLocalState>();
	if (gstate.collection) {
		return local_state;
	}
	local_state->column_ids = input.column_ids;

	if (bind_data.transaction && !bind_data.transaction->IsReadOnly()) {
		// if we have made other modifications in this transaction we have to use the main connection
		local_state->connection = PostgresConnection(bind_data.transaction->GetConnection().GetConnection());
	} else {
		local_state->connection = PostgresScanConnect(bind_data.dsn, bind_data.in_recovery, bind_data.snapshot);
	}
	local_state->filters = input.filters.get();
	if (bind_data.pages_approx == 0 || bind_data.requires_materialization) {
		PostgresInitInternal(context, &bind_data, *local_state, 0, POSTGRES_TID_MAX);
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

void PostgresLocalState::ScanChunk(ClientContext &context, const PostgresBindData &bind_data, PostgresGlobalState &gstate, DataChunk &output) {
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
			auto raw_len = reader.ReadInteger<int32_t>();
			if (raw_len == -1) { // NULL
				FlatVector::Validity(out_vec).Set(output_offset, false);
				continue;
			}
			if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
				// row id
				// ctid in postgres are a composite type of (page_index, tuple_in_page)
				// the page index is a 4-byte integer, the tuple_in_page a 2-byte integer
				D_ASSERT(raw_len == 6);
				int64_t page_index = reader.ReadInteger<int32_t>();
				int64_t row_in_page = reader.ReadInteger<int16_t>();
				FlatVector::GetData<int64_t>(out_vec)[output_offset] = (page_index << 16LL) + row_in_page;
			} else {
				ProcessValue(bind_data.types[col_idx], bind_data.postgres_types[col_idx], reader, raw_len, out_vec,
							 output_offset);
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
	local_state.ScanChunk(context, bind_data, gstate, output);
}

static string PostgresScanToString(const FunctionData *bind_data_p) {
	D_ASSERT(bind_data_p);

	auto bind_data = (const PostgresBindData *)bind_data_p;
	return bind_data->table_name;
}

struct AttachFunctionData : public TableFunctionData {
	bool finished = false;
	string source_schema = "public";
	string sink_schema = "main";
	string suffix = "";
	bool overwrite = false;
	bool filter_pushdown = false;

	string dsn = "";
};

static unique_ptr<FunctionData> AttachBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {

	auto result = make_uniq<AttachFunctionData>();
	result->dsn = input.inputs[0].GetValue<string>();

	for (auto &kv : input.named_parameters) {
		if (kv.first == "source_schema") {
			result->source_schema = StringValue::Get(kv.second);
		} else if (kv.first == "sink_schema") {
			result->sink_schema = StringValue::Get(kv.second);
		} else if (kv.first == "overwrite") {
			result->overwrite = BooleanValue::Get(kv.second);
		} else if (kv.first == "filter_pushdown") {
			result->filter_pushdown = BooleanValue::Get(kv.second);
		}
	}

	return_types.push_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");
	return std::move(result);
}

static void AttachFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (AttachFunctionData &)*data_p.bind_data;
	if (data.finished) {
		return;
	}

	auto conn = PostgresConnection::Open(data.dsn);
	auto dconn = Connection(context.db->GetDatabase(context));
	auto res = conn.Query(StringUtil::Format(
	                             R"(
SELECT relname
FROM pg_class JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
JOIN pg_attribute ON pg_class.oid = pg_attribute.attrelid
WHERE relkind = 'r' AND attnum > 0 AND nspname = '%s'
GROUP BY relname
ORDER BY relname;
)",
	                             data.source_schema)
	                             .c_str());

	for (idx_t row = 0; row < PQntuples(res->res); row++) {
		auto table_name = res->GetString(row, 0);

		dconn
		    .TableFunction(data.filter_pushdown ? "postgres_scan_pushdown" : "postgres_scan",
		                   {Value(data.dsn), Value(data.source_schema), Value(table_name)})
		    ->CreateView(data.sink_schema, table_name, data.overwrite, false);
	}
	res.reset();

	data.finished = true;
}

static void PostgresScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
								 const TableFunction &function) {
	throw NotImplementedException("PostgresScanSerialize");
}

static unique_ptr<FunctionData> PostgresScanDeserialize(Deserializer &deserializer, TableFunction &function) {
	throw NotImplementedException("PostgresScanDeserialize");
}

PostgresScanFunction::PostgresScanFunction()
	: TableFunction("postgres_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
					PostgresScan, PostgresBind, PostgresInitGlobalState, PostgresInitLocalState) {
	to_string = PostgresScanToString;
	serialize = PostgresScanSerialize;
	deserialize = PostgresScanDeserialize;
	projection_pushdown = true;
}

PostgresScanFunctionFilterPushdown::PostgresScanFunctionFilterPushdown()
	: TableFunction("postgres_scan_pushdown", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
					PostgresScan, PostgresBind, PostgresInitGlobalState, PostgresInitLocalState) {
	to_string = PostgresScanToString;
	serialize = PostgresScanSerialize;
	deserialize = PostgresScanDeserialize;
	projection_pushdown = true;
	filter_pushdown = true;
}

PostgresAttachFunction::PostgresAttachFunction()
	: TableFunction("postgres_attach", {LogicalType::VARCHAR}, AttachFunction, AttachBind) {
}

}
