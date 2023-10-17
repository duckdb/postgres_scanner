#include "storage/postgres_transaction.hpp"
#include "storage/postgres_catalog.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "postgres_result.hpp"

namespace duckdb {

PostgresTransaction::PostgresTransaction(PostgresCatalog &postgres_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), access_mode(postgres_catalog.access_mode) {
	connection = PostgresConnection::Open(postgres_catalog.path);
}

PostgresTransaction::~PostgresTransaction() = default;

void PostgresTransaction::Start() {
	transaction_state = PostgresTransactionState::TRANSACTION_NOT_YET_STARTED;
}
void PostgresTransaction::Commit() {
	if (transaction_state == PostgresTransactionState::TRANSACTION_STARTED) {
		transaction_state = PostgresTransactionState::TRANSACTION_FINISHED;
		connection.Execute("COMMIT");
	}
}
void PostgresTransaction::Rollback() {
	if (transaction_state == PostgresTransactionState::TRANSACTION_STARTED) {
		transaction_state = PostgresTransactionState::TRANSACTION_FINISHED;
		connection.Execute("ROLLBACK");
	}
}

PostgresConnection &PostgresTransaction::GetConnection() {
	if (transaction_state == PostgresTransactionState::TRANSACTION_NOT_YET_STARTED) {
		transaction_state = PostgresTransactionState::TRANSACTION_STARTED;
		string query = "BEGIN TRANSACTION";
		if (access_mode == AccessMode::READ_ONLY) {
			query += " READ ONLY";
		}
		connection.Execute(query);
	}
	return connection;
}

unique_ptr<PostgresResult> PostgresTransaction::Query(const string &query) {
	if (transaction_state == PostgresTransactionState::TRANSACTION_NOT_YET_STARTED) {
		transaction_state = PostgresTransactionState::TRANSACTION_STARTED;
		string transaction_start = "BEGIN TRANSACTION";
		if (access_mode == AccessMode::READ_ONLY) {
			transaction_start += " READ ONLY";
		}
		transaction_start += ";\n";
		return connection.Query(transaction_start + query);
	}
	return connection.Query(query);
}

PostgresTransaction &PostgresTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<PostgresTransaction>();
}

} // namespace duckdb
