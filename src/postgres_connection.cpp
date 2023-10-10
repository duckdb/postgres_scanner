#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/parser.hpp"
#include "postgres_connection.hpp"
#include "postgres_stmt.hpp"
#include "duckdb/common/types/uuid.hpp"

namespace duckdb {

PostgresConnection::PostgresConnection(shared_ptr<OwnedPostgresConnection> connection_p)
    : connection(std::move(connection_p)) {
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
	result.connection = make_shared<OwnedPostgresConnection>(PostgresUtils::PGConnect(connection_string));
	result.dsn = connection_string;
	return result;
}

bool PostgresConnection::TryPrepare(const string &query, PostgresStatement &stmt, string &error) {
	stmt.name = "";
	auto result = PQprepare(GetConn(), stmt.name.c_str(), query.c_str(), 0, nullptr);
	if (!result || PQresultStatus(result) != PGRES_COMMAND_OK) {
		error = PQresultErrorMessage(result);
		return false;
	}
	return true;
}

PostgresStatement PostgresConnection::Prepare(const string &query) {
	PostgresStatement stmt;
	stmt.connection = GetConn();
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
	auto result = PQexec(GetConn(), query.c_str());
	if (ResultHasError(result)) {
		return nullptr;
	}
	return make_uniq<PostgresResult>(result);
}

unique_ptr<PostgresResult> PostgresConnection::Query(const string &query) {
	auto result = PQexec(GetConn(), query.c_str());
	if (ResultHasError(result)) {
		throw std::runtime_error("Failed to execute query \"" + query + "\": " + string(PQresultErrorMessage(result)));
	}
	return make_uniq<PostgresResult>(result);
}

void PostgresConnection::Execute(const string &query) {
	Query(query);
}

bool PostgresConnection::IsOpen() {
	return connection.get();
}

void PostgresConnection::Close() {
	if (!IsOpen()) {
		return;
	}
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
