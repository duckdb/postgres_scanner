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

unique_ptr<PostgresResult> PostgresConnection::Query(const string &query) {
	auto result = PQexec(connection, query.c_str());
	if (!result || PQresultStatus(result) != PGRES_COMMAND_OK) {
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

CatalogType PostgresConnection::GetEntryType(const string &name) {
	throw InternalException("GetEntryType");
}

void PostgresConnection::GetIndexInfo(const string &index_name, string &sql, string &table_name) {
	throw InternalException("GetIndexInfo");
}

void PostgresConnection::GetViewInfo(const string &view_name, string &sql) {
	throw InternalException("GetViewInfo(");
}

void PostgresConnection::GetTableInfo(const string &table_name, ColumnList &columns, vector<unique_ptr<Constraint>> &constraints) {
	throw InternalException("GetTableInfo");
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
