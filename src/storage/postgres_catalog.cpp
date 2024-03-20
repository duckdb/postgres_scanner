#include "storage/postgres_catalog.hpp"
#include "storage/postgres_schema_entry.hpp"
#include "storage/postgres_transaction.hpp"
#include "postgres_connection.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

PostgresCatalog::PostgresCatalog(AttachedDatabase &db_p, const string &path, AccessMode access_mode)
    : Catalog(db_p), path(path), access_mode(access_mode), schemas(*this), connection_pool(*this) {
	Value connection_limit;
	auto &db_instance = db_p.GetDatabase();
	if (db_instance.TryGetCurrentSetting("pg_connection_limit", connection_limit)) {
		connection_pool.SetMaximumConnections(UBigIntValue::Get(connection_limit));
	}

	auto connection = connection_pool.GetConnection();
	this->version = connection.GetConnection().GetPostgresVersion();
}

PostgresCatalog::~PostgresCatalog() = default;

void PostgresCatalog::Initialize(bool load_builtin) {
}

optional_ptr<CatalogEntry> PostgresCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	auto &postgres_transaction = PostgresTransaction::Get(transaction.GetContext(), *this);
	auto entry = schemas.GetEntry(transaction.GetContext(), info.schema);
	if (entry) {
		switch (info.on_conflict) {
		case OnCreateConflict::REPLACE_ON_CONFLICT: {
			DropInfo try_drop;
			try_drop.type = CatalogType::SCHEMA_ENTRY;
			try_drop.name = info.schema;
			try_drop.if_not_found = OnEntryNotFound::RETURN_NULL;
			try_drop.cascade = false;
			schemas.DropEntry(transaction.GetContext(), try_drop);
			break;
		}
		case OnCreateConflict::IGNORE_ON_CONFLICT:
			return entry;
		case OnCreateConflict::ERROR_ON_CONFLICT:
		default:
			throw BinderException("Failed to create schema \"%s\": schema already exists", info.schema);
		}
	}
	return schemas.CreateSchema(transaction.GetContext(), info);
}

void PostgresCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	return schemas.DropEntry(context, info);
}

void PostgresCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	schemas.Scan(context, [&](CatalogEntry &schema) { callback(schema.Cast<PostgresSchemaEntry>()); });
}

optional_ptr<SchemaCatalogEntry> PostgresCatalog::GetSchema(CatalogTransaction transaction, const string &schema_name,
                                                            OnEntryNotFound if_not_found,
                                                            QueryErrorContext error_context) {
	if (schema_name == DEFAULT_SCHEMA) {
		return GetSchema(transaction, "public", if_not_found, error_context);
	}
	auto &postgres_transaction = PostgresTransaction::Get(transaction.GetContext(), *this);
	auto entry = schemas.GetEntry(transaction.GetContext(), schema_name);
	if (!entry && if_not_found != OnEntryNotFound::RETURN_NULL) {
		throw BinderException("Schema with name \"%s\" not found", schema_name);
	}
	return reinterpret_cast<SchemaCatalogEntry *>(entry.get());
}

bool PostgresCatalog::InMemory() {
	return false;
}

string PostgresCatalog::GetDBPath() {
	return path;
}

DatabaseSize PostgresCatalog::GetDatabaseSize(ClientContext &context) {
	auto &postgres_transaction = PostgresTransaction::Get(context, *this);
	auto result = postgres_transaction.Query("SELECT pg_database_size(current_database());");
	DatabaseSize size;
	size.free_blocks = 0;
	size.total_blocks = 0;
	size.used_blocks = 0;
	size.wal_size = 0;
	size.block_size = 0;
	size.bytes = result->GetInt64(0, 0);
	return size;
}

void PostgresCatalog::ClearCache() {
	schemas.ClearEntries();
}

} // namespace duckdb
