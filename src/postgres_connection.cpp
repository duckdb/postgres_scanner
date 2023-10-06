#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/parser.hpp"
#include "postgres_connection.hpp"
#include "postgres_stmt.hpp"
#include "duckdb/common/types/uuid.hpp"

namespace duckdb {

PostgresConnection::PostgresConnection() : connection(nullptr) {
}

PostgresConnection::PostgresConnection(PGconn *connection_p) :
	connection(connection_p) {
}

PostgresConnection::~PostgresConnection() {
	Close();
}

PostgresConnection::PostgresConnection(PostgresConnection &&other) noexcept {
	std::swap(connection, other.connection);
}

PostgresConnection &PostgresConnection::operator=(PostgresConnection &&other) noexcept {
	std::swap(connection, other.connection);
	return *this;
}

PostgresConnection PostgresConnection::Open(const string &connection_string) {
	PostgresConnection result;
	result.connection = PostgresUtils::PGConnect(connection_string);
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

vector<string> PostgresConnection::GetTables() {
	return GetEntries("table");
}

void PostgresConnection::GetIndexInfo(const string &index_name, string &sql, string &table_name) {
	throw InternalException("GetIndexInfo");
}

void PostgresConnection::GetViewInfo(const string &view_name, string &sql) {
	throw InternalException("GetViewInfo(");
}

bool PostgresConnection::GetTableInfo(const string &table_name, ColumnList &columns, vector<unique_ptr<Constraint>> &constraints) {
	// query the columns that belong to this table
	auto query = StringUtil::Replace(R"(
SELECT column_name, data_type, column_default, is_nullable
FROM information_schema.columns
WHERE table_name='${TABLE_NAME}'
ORDER BY ordinal_position;
)", "${TABLE_NAME}", table_name);
	auto result = Query(query);
	auto rows = result->Count();
	if (rows == 0) {
		return false;
	}
	for(idx_t row = 0; row < rows; row++) {
		auto column_name = result->GetString(row, 0);
		auto data_type = result->GetString(row, 1);
		auto default_value = result->GetString(row, 2);
		auto is_nullable = result->GetString(row, 3);
		auto column_type = PostgresUtils::TypeToLogicalType(data_type);

		ColumnDefinition column(std::move(column_name), std::move(column_type));
		if (!default_value.empty()) {
			auto expressions = Parser::ParseExpressionList(default_value);
			if (expressions.empty()) {
				throw InternalException("Expression list is empty");
			}
			column.SetDefaultValue(std::move(expressions[0]));
		}
		columns.AddColumn(std::move(column));
		if (is_nullable != "YES") {
			constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(row)));
		}
	}
	return true;
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
