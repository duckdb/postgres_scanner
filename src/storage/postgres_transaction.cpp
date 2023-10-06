#include "storage/postgres_transaction.hpp"
#include "storage/postgres_catalog.hpp"
#include "storage/postgres_schema_entry.hpp"
#include "storage/postgres_table_entry.hpp"
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

PostgresTransaction::~PostgresTransaction() {
}

void PostgresTransaction::Start() {
	connection.Execute("BEGIN TRANSACTION");
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

optional_ptr<CatalogEntry> PostgresTransaction::GetCatalogEntry(CatalogType type, const string &entry_name) {
	auto entry = catalog_entries.find(entry_name);
	if (entry != catalog_entries.end()) {
		return entry->second.get();
	}
	unique_ptr<CatalogEntry> result;
	switch (type) {
	case CatalogType::TABLE_ENTRY: {
		CreateTableInfo info(postgres_catalog.GetMainSchema(), entry_name);
		auto exists = connection.GetTableInfo(entry_name, info.columns, info.constraints);
		if (!exists) {
			return nullptr;
		}
		D_ASSERT(!info.columns.empty());

		result = make_uniq<PostgresTableEntry>(postgres_catalog, postgres_catalog.GetMainSchema(), info);
		break;
	}
	case CatalogType::VIEW_ENTRY: {
		string sql;
		connection.GetViewInfo(entry_name, sql);

		auto view_info = CreateViewInfo::FromCreateView(*context.lock(), sql);
		view_info->internal = false;
		result = make_uniq<ViewCatalogEntry>(postgres_catalog, postgres_catalog.GetMainSchema(), *view_info);
		break;
	}
	case CatalogType::INDEX_ENTRY: {
		throw InternalException("FIXME: index");
	}
	default:
		throw InternalException("Unrecognized catalog entry type");
	}
	auto result_ptr = result.get();
	catalog_entries[entry_name] = std::move(result);
	return result_ptr;
}

void PostgresTransaction::ClearTableEntry(const string &table_name) {
	catalog_entries.erase(table_name);
}

string GetDropSQL(CatalogType type, const string &table_name, bool cascade) {
	string result;
	result = "DROP ";
	switch (type) {
	case CatalogType::TABLE_ENTRY:
		result += "TABLE ";
		break;
	case CatalogType::VIEW_ENTRY:
		result += "VIEW ";
		break;
	case CatalogType::INDEX_ENTRY:
		result += "INDEX ";
		break;
	default:
		throw InternalException("Unsupported type for drop");
	}
	result += KeywordHelper::WriteOptionallyQuoted(table_name);
	return result;
}

void PostgresTransaction::DropEntry(CatalogType type, const string &table_name, bool cascade) {
	catalog_entries.erase(table_name);
	connection.Execute(GetDropSQL(type, table_name, cascade));
}

} // namespace duckdb
