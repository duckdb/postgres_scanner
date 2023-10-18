#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/parser.hpp"
#include "postgres_connection.hpp"
#include "duckdb/common/types/uuid.hpp"

namespace duckdb {

static bool debug_postgres_print_queries = false;

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

PGresult *PostgresConnection::PQExecute(const string &query) {
	if (PostgresConnection::DebugPrintQueries()) {
		Printer::Print(query + "\n");
	}
	return PQexec(GetConn(), query.c_str());
}

unique_ptr<PostgresResult> PostgresConnection::Query(const string &query) {
	auto result = PQExecute(query.c_str());
	if (ResultHasError(result)) {
		throw std::runtime_error("Failed to execute query \"" + query + "\": " + string(PQresultErrorMessage(result)));
	}
	return make_uniq<PostgresResult>(result);
}

void PostgresConnection::Execute(const string &query) {
	Query(query);
}

PostgresVersion PostgresConnection::GetPostgresVersion() {
	auto result = Query("SHOW server_version;");
	auto version = PostgresUtils::ExtractPostgresVersion(result->GetString(0, 0));
	return version;
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

vector<IndexInfo> PostgresConnection::GetIndexInfo(const string &table_name) {
	return vector<IndexInfo>();
}

void PostgresConnection::DebugSetPrintQueries(bool print) {
	debug_postgres_print_queries = print;
}

bool PostgresConnection::DebugPrintQueries() {
	return debug_postgres_print_queries;
}

} // namespace duckdb
