#include "storage/postgres_catalog.hpp"
#include "storage/postgres_table_entry.hpp"
#include "storage/postgres_transaction.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "postgres_scanner.hpp"

namespace duckdb {

PostgresTableEntry::PostgresTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info)
	: TableCatalogEntry(catalog, schema, info) {
	for(idx_t c = 0; c < columns.LogicalColumnCount(); c++) {
		auto &col = columns.GetColumnMutable(LogicalIndex(c));
		if (col.GetType().HasAlias()) {
			col.TypeMutable() = PostgresUtils::RemoveAlias(col.GetType());
		}
		postgres_types.push_back(PostgresUtils::CreateEmptyPostgresType(col.GetType()));
		postgres_names.push_back(col.GetName());
	}
	approx_num_pages = 0;
}

PostgresTableEntry::PostgresTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, PostgresTableInfo &info)
    : TableCatalogEntry(catalog, schema, *info.create_info), postgres_types(std::move(info.postgres_types)),
	postgres_names(std::move(info.postgres_names)) {
	D_ASSERT(postgres_types.size() == columns.LogicalColumnCount());
	approx_num_pages = info.approx_num_pages;
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
	}
	result->names = postgres_names;
	result->postgres_types = postgres_types;
	result->read_only = transaction.IsReadOnly();
	result->SetTablePages(approx_num_pages);

	// check how many threads we can actually use
	if (result->max_threads > 1) {
		auto &connection_pool = catalog.Cast<PostgresCatalog>().GetConnectionPool();
		result->connection_reservation = connection_pool.AllocateConnections(result->max_threads);
		result->max_threads = result->connection_reservation.GetConnectionCount();
	}

	bind_data = std::move(result);
	auto function = PostgresScanFunction();
	Value filter_pushdown;
	if (context.TryGetCurrentSetting("pg_experimental_filter_pushdown", filter_pushdown)) {
		function.filter_pushdown = BooleanValue::Get(filter_pushdown);
	}
	return function;
}

TableStorageInfo PostgresTableEntry::GetStorageInfo(ClientContext &context) {
	auto &transaction = Transaction::Get(context, catalog).Cast<PostgresTransaction>();
	auto &db = transaction.GetConnection();
	TableStorageInfo result;
	result.cardinality = 0;
	result.index_info = db.GetIndexInfo(name);
	return result;
}

static bool CopyRequiresText(const LogicalType &type, const PostgresType &pg_type) {
	if (pg_type.info != PostgresTypeAnnotation::STANDARD) {
		return true;
	}
	switch(type.id()) {
	case LogicalTypeId::LIST: {
		D_ASSERT(pg_type.children.size() == 1);
		auto &child_type = ListType::GetChildType(type);
		if (child_type.id() != LogicalTypeId::LIST && !PostgresUtils::SupportedPostgresOid(child_type)) {
			return true;
		}
		if (CopyRequiresText(child_type, pg_type.children[0])) {
			return true;
		}
		return false;
	}
	case LogicalTypeId::STRUCT: {
		auto &children = StructType::GetChildTypes(type);
		D_ASSERT(children.size() == pg_type.children.size());
		for(idx_t c = 0; c < pg_type.children.size(); c++) {
			if (!PostgresUtils::SupportedPostgresOid(children[c].second)) {
				return true;
			}
			if (CopyRequiresText(children[c].second, pg_type.children[c])) {
				return true;
			}
		}
		return false;
	}
	default:
		return false;
	}
}

PostgresCopyFormat PostgresTableEntry::GetCopyFormat(ClientContext &context) {
	Value use_binary_copy;
	if (context.TryGetCurrentSetting("pg_use_binary_copy", use_binary_copy)) {
		if (!BooleanValue::Get(use_binary_copy)) {
			return PostgresCopyFormat::TEXT;
		}
	}
	D_ASSERT(postgres_types.size() == columns.LogicalColumnCount());
	for(idx_t c = 0; c < postgres_types.size(); c++) {
		if (CopyRequiresText(columns.GetColumn(LogicalIndex(c)).GetType(), postgres_types[c])) {
			return PostgresCopyFormat::TEXT;
		}
	}
	return PostgresCopyFormat::BINARY;
}

} // namespace duckdb
