#include "storage/postgres_catalog.hpp"
#include "storage/postgres_table_entry.hpp"
#include "storage/postgres_transaction.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "postgres_scanner.hpp"

namespace duckdb {

PostgresType PostgresTypeFromType(const LogicalType &type) {
	PostgresType result;
	if (type.id() == LogicalTypeId::LIST) {
		result.children.push_back(PostgresTypeFromType(ListType::GetChildType(type)));
	}
	return result;
}

PostgresTableEntry::PostgresTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info)
	: TableCatalogEntry(catalog, schema, info) {
	for(auto &col : columns.Logical()) {
		postgres_types.push_back(PostgresTypeFromType(col.GetType()));
	}
}

PostgresTableEntry::PostgresTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, PostgresTableInfo &info)
    : TableCatalogEntry(catalog, schema, *info.create_info), postgres_types(std::move(info.postgres_types)) {
	D_ASSERT(postgres_types.size() == columns.LogicalColumnCount());
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
	for(auto &col : columns.Logical()) {
		result->types.push_back(col.GetType());
		result->names.push_back(col.GetName());
	}
	result->postgres_types = postgres_types;

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
