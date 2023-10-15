#include "storage/postgres_transaction.hpp"
#include "storage/postgres_catalog.hpp"
#include "storage/postgres_schema_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "postgres_result.hpp"

namespace duckdb {

PostgresTransaction::PostgresTransaction(PostgresCatalog &postgres_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), postgres_catalog(postgres_catalog) {
	connection = PostgresConnection::Open(postgres_catalog.path);
}

PostgresTransaction::~PostgresTransaction() = default;

void PostgresTransaction::Start() {
	string query = "BEGIN TRANSACTION";
	if (postgres_catalog.access_mode == AccessMode::READ_ONLY) {
		query += " READ ONLY";
	}
	connection.Execute(query);
}
void PostgresTransaction::Commit() {
	connection.Execute("COMMIT");
}
void PostgresTransaction::Rollback() {
	connection.Execute("ROLLBACK");
}

PostgresConnection &PostgresTransaction::GetConnection() {
	return connection;
}

PostgresTransaction &PostgresTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<PostgresTransaction>();
}

} // namespace duckdb
