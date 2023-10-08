#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/parser.hpp"
#include "storage/postgres_schema_entry.hpp"
#include "postgres_conversion.hpp"
#include "postgres_connection.hpp"
#include "postgres_stmt.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

PostgresConnection::PostgresConnection() : connection(nullptr) {
}

PostgresConnection::~PostgresConnection() {
	Close();
}

PostgresConnection::PostgresConnection(PostgresConnection &&other) noexcept {
	std::swap(connection, other.connection);
	std::swap(dsn, other.dsn);
}

PostgresConnection &PostgresConnection::operator=(PostgresConnection &&other) noexcept {
	std::swap(connection, other.connection);
	std::swap(dsn, other.dsn);
	return *this;
}

PostgresConnection PostgresConnection::Open(const string &connection_string) {
	PostgresConnection result;
	result.connection = PostgresUtils::PGConnect(connection_string);
	result.dsn = connection_string;
	return result;
}

bool PostgresConnection::TryPrepare(const string &query, PostgresStatement &stmt, string &error) {
	stmt.name = "";
	auto result = PQprepare(connection, stmt.name.c_str(), query.c_str(), 0, nullptr);
	if (!result || PQresultStatus(result) != PGRES_COMMAND_OK) {
		error = PQresultErrorMessage(result);
		return false;
	}
	return true;
}

PostgresStatement PostgresConnection::Prepare(const string &query) {
	PostgresStatement stmt;
	stmt.connection = connection;
	string error_msg;
	if (!TryPrepare(query, stmt, error_msg)) {
		string error = "Failed to prepare query \"" + query + "\": " + error_msg;
		throw std::runtime_error(error);
	}
	return stmt;
}

static bool ResultHasError(PGresult *result) {
	if (!result) {
		return true;
	}
	switch(PQresultStatus(result)) {
	case PGRES_COMMAND_OK:
	case PGRES_TUPLES_OK:
		return false;
	default:
		return true;
	}
}

unique_ptr<PostgresResult> PostgresConnection::TryQuery(const string &query) {
	auto result = PQexec(connection, query.c_str());
	if (ResultHasError(result)) {
		return nullptr;
	}
	return make_uniq<PostgresResult>(result);
}

unique_ptr<PostgresResult> PostgresConnection::Query(const string &query) {
	auto result = PQexec(connection, query.c_str());
	if (ResultHasError(result)) {
		throw std::runtime_error("Failed to execute query \"" + query + "\": " + string(PQresultErrorMessage(result)));
	}
	return make_uniq<PostgresResult>(result);
}

void PostgresConnection::Execute(const string &query) {
	Query(query);
}

bool PostgresConnection::IsOpen() {
	return connection;
}

void PostgresConnection::Close() {
	if (!IsOpen()) {
		return;
	}
	PQfinish(connection);
	connection = nullptr;
}

vector<string> PostgresConnection::GetEntries(string entry_type) {
	throw InternalException("Get Entries");
}

void PostgresConnection::GetIndexInfo(const string &index_name, string &sql, string &table_name) {
	throw InternalException("GetIndexInfo");
}

void PostgresConnection::GetViewInfo(const string &view_name, string &sql) {
	throw InternalException("GetViewInfo(");
}

static void AddColumn(PostgresResult &result, idx_t row, CreateTableInfo &table_info, idx_t column_offset = 0) {
	PostgresTypeData type_info;
	idx_t column_index = column_offset;
	auto column_name = result.GetString(row, column_index);
	type_info.type_name = result.GetString(row, column_index + 1);
	auto default_value = result.GetString(row, column_index + 2);
	auto is_nullable = result.GetString(row, column_index + 3);
	type_info.precision = result.IsNull(row, column_index + 4) ? -1 : result.GetInt64(row, column_index + 4);
	type_info.scale = result.IsNull(row, column_index + 5) ? -1 : result.GetInt64(row, column_index + 5);

	auto column_type = PostgresUtils::TypeToLogicalType(type_info);

	ColumnDefinition column(std::move(column_name), std::move(column_type));
	if (!default_value.empty()) {
		auto expressions = Parser::ParseExpressionList(default_value);
		if (expressions.empty()) {
			throw InternalException("Expression list is empty");
		}
		column.SetDefaultValue(std::move(expressions[0]));
	}
	table_info.columns.AddColumn(std::move(column));
	if (is_nullable != "YES") {
		table_info.constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(row)));
	}
}

vector<unique_ptr<CreateTableInfo>> PostgresConnection::GetTables(const PostgresSchemaEntry &schema) {
	auto query = StringUtil::Replace(R"(
SELECT table_name, column_name, udt_name, column_default, is_nullable, numeric_precision, numeric_scale
FROM information_schema.columns
WHERE table_schema=${SCHEMA_NAME}
ORDER BY table_name, ordinal_position;
)", "${SCHEMA_NAME}", KeywordHelper::WriteQuoted(schema.name));

	auto result = Query(query);
	auto rows = result->Count();

	vector<unique_ptr<CreateTableInfo>> tables;
	unique_ptr<CreateTableInfo> info;
	for(idx_t row = 0; row < rows; row++) {
		auto table_name = result->GetString(row, 0);
		if (!info || info->table != table_name) {
			if (info) {
				tables.push_back(std::move(info));
			}
			info = make_uniq<CreateTableInfo>(schema, table_name);
		}
		AddColumn(*result, row, *info, 1);
	}
	if (info) {
		tables.push_back(std::move(info));
	}
	return tables;
}

unique_ptr<CreateTableInfo> PostgresConnection::GetTableInfo(const PostgresSchemaEntry &schema, const string &table_name) {
	// query the columns that belong to this table
	auto query = StringUtil::Replace(StringUtil::Replace(R"(
SELECT column_name, udt_name, column_default, is_nullable, numeric_precision, numeric_scale
FROM information_schema.columns
WHERE table_schema=${SCHEMA_NAME} AND table_name=${TABLE_NAME}
ORDER BY ordinal_position;
)", "${TABLE_NAME}", KeywordHelper::WriteQuoted(table_name)), "${SCHEMA_NAME}", KeywordHelper::WriteQuoted(schema.name));
	auto result = Query(query);
	auto rows = result->Count();
	if (rows == 0) {
		return nullptr;
	}
	auto table_info = make_uniq<CreateTableInfo>(schema, table_name);
	for(idx_t row = 0; row < rows; row++) {
		AddColumn(*result, row, *table_info);
	}
	return table_info;
}

bool PostgresConnection::ColumnExists(const string &table_name, const string &column_name) {
	throw InternalException("ColumnExists");
}

bool PostgresConnection::GetMaxRowId(const string &table_name, idx_t &max_row_id) {
	throw InternalException("GetMaxRowId");
}

vector<IndexInfo> PostgresConnection::GetIndexInfo(const string &table_name) {
	throw InternalException("GetIndexInfo");
}

} // namespace duckdb
