#include "storage/postgres_transaction.hpp"
#include "storage/postgres_catalog.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "postgres_result.hpp"

namespace duckdb {

PostgresTransaction::PostgresTransaction(PostgresCatalog &postgres_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), access_mode(postgres_catalog.access_mode) {
	connection = postgres_catalog.GetConnectionPool().GetConnection();
}

PostgresTransaction::~PostgresTransaction() = default;

void PostgresTransaction::Start() {
	transaction_state = PostgresTransactionState::TRANSACTION_NOT_YET_STARTED;
}
void PostgresTransaction::Commit() {
	if (transaction_state == PostgresTransactionState::TRANSACTION_STARTED) {
		transaction_state = PostgresTransactionState::TRANSACTION_FINISHED;
		GetConnectionRaw().Execute("COMMIT");
	}
}
void PostgresTransaction::Rollback() {
	if (transaction_state == PostgresTransactionState::TRANSACTION_STARTED) {
		transaction_state = PostgresTransactionState::TRANSACTION_FINISHED;
		GetConnectionRaw().Execute("ROLLBACK");
	}
}

PostgresConnection &PostgresTransaction::GetConnection() {
	auto &con = GetConnectionRaw();
	if (transaction_state == PostgresTransactionState::TRANSACTION_NOT_YET_STARTED) {
		transaction_state = PostgresTransactionState::TRANSACTION_STARTED;
		string query = "BEGIN TRANSACTION";
		if (access_mode == AccessMode::READ_ONLY) {
			query += " READ ONLY";
		}
		con.Execute(query);
	}
	return con;
}

PostgresConnection &PostgresTransaction::GetConnectionRaw() {
	return connection.GetConnection();
}

string PostgresTransaction::GetDSN() {
	return GetConnectionRaw().GetDSN();
}

unique_ptr<PostgresResult> PostgresTransaction::Query(const string &query) {
	auto &con = GetConnectionRaw();
	if (transaction_state == PostgresTransactionState::TRANSACTION_NOT_YET_STARTED) {
		transaction_state = PostgresTransactionState::TRANSACTION_STARTED;
		string transaction_start = "BEGIN TRANSACTION";
		if (access_mode == AccessMode::READ_ONLY) {
			transaction_start += " READ ONLY";
		}
		transaction_start += ";\n";
		return con.Query(transaction_start + query);
	}
	return con.Query(query);
}

vector<unique_ptr<PostgresResult>> PostgresTransaction::ExecuteQueries(const string &queries) {
	auto &con = GetConnectionRaw();
	if (transaction_state == PostgresTransactionState::TRANSACTION_NOT_YET_STARTED) {
		transaction_state = PostgresTransactionState::TRANSACTION_STARTED;
		string transaction_start = "BEGIN TRANSACTION";
		if (access_mode == AccessMode::READ_ONLY) {
			transaction_start += " READ ONLY";
		}
		transaction_start += ";\n";
		return con.ExecuteQueries(transaction_start + queries);
	}
	return con.ExecuteQueries(queries);
}


PostgresTransaction &PostgresTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<PostgresTransaction>();
}

} // namespace duckdb
