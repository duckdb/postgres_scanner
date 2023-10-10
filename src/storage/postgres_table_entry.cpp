#include "storage/postgres_catalog.hpp"
#include "storage/postgres_table_entry.hpp"
#include "storage/postgres_transaction.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "postgres_scanner.hpp"

namespace duckdb {

PostgresTableEntry::PostgresTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info)
    : TableCatalogEntry(catalog, schema, info) {
}

unique_ptr<BaseStatistics> PostgresTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

void PostgresTableEntry::BindUpdateConstraints(LogicalGet &, LogicalProjection &, LogicalUpdate &, ClientContext &) {
}

TableFunction PostgresTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	auto &transaction = Transaction::Get(context, catalog).Cast<PostgresTransaction>();
	auto &conn = transaction.GetConnection();

	auto result = make_uniq<PostgresBindData>();

	result->schema_name = schema.name;
	result->table_name = name;
	result->dsn = conn.GetDSN();
	result->transaction = &transaction;
	result->connection = PostgresConnection(conn.GetConnection());

	PostgresScanFunction::PrepareBind(context, *result);

	bind_data = std::move(result);
	return PostgresScanFunction();
}

TableStorageInfo PostgresTableEntry::GetStorageInfo(ClientContext &context) {
	auto &transaction = Transaction::Get(context, catalog).Cast<PostgresTransaction>();
	auto &db = transaction.GetConnection();
	TableStorageInfo result;
	result.cardinality = 0;
	result.index_info = db.GetIndexInfo(name);
	return result;
}

} // namespace duckdb
