#define DUCKDB_BUILD_LOADABLE_EXTENSION
#include "duckdb.hpp"


#include "postgres_scanner.hpp"
#include "postgres_storage.hpp"
#include "postgres_scanner_extension.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "storage/postgres_catalog.hpp"

using namespace duckdb;

extern "C" {

static void SetPostgresConnectionLimit(ClientContext &context, SetScope scope, Value &parameter) {
	if (scope == SetScope::LOCAL) {
		throw InvalidInputException("pg_connection_limit can only be set globally");
	}
	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for(auto &db_ref : databases) {
		auto &db = db_ref.get();
		auto &catalog = db.GetCatalog();
		if (catalog.GetCatalogType() != "postgres") {
			continue;
		}
		catalog.Cast<PostgresCatalog>().GetConnectionPool().SetMaximumConnections(UBigIntValue::Get(parameter));
	}
	auto &config = DBConfig::GetConfig(context);
	config.SetOption("pg_connection_limit", parameter);
}

static void SetPostgresDebugQueryPrint(ClientContext &context, SetScope scope, Value &parameter) {
	PostgresConnection::DebugSetPrintQueries(BooleanValue::Get(parameter));
}

static void LoadInternal(DatabaseInstance &db) {
	PostgresScanFunction postgres_fun;
	ExtensionUtil::RegisterFunction(db, postgres_fun);

	PostgresScanFunctionFilterPushdown postgres_fun_filter_pushdown;
	ExtensionUtil::RegisterFunction(db, postgres_fun_filter_pushdown);

	PostgresAttachFunction attach_func;
	ExtensionUtil::RegisterFunction(db, attach_func);

	PostgresClearCacheFunction clear_cache_func;
	ExtensionUtil::RegisterFunction(db, clear_cache_func);

	auto &config = DBConfig::GetConfig(db);
	config.storage_extensions["postgres_scanner"] = make_uniq<PostgresStorageExtension>();

	config.AddExtensionOption("pg_use_binary_copy", "Whether or not to use BINARY copy to read data", LogicalType::BOOLEAN, Value::BOOLEAN(true));
	config.AddExtensionOption("pg_pages_per_task", "The amount of pages per task", LogicalType::UBIGINT, Value::UBIGINT(PostgresBindData::DEFAULT_PAGES_PER_TASK));
	config.AddExtensionOption("pg_connection_limit", "The maximum amount of concurrent Postgres connections", LogicalType::UBIGINT, Value::UBIGINT(PostgresConnectionPool::DEFAULT_MAX_CONNECTIONS), SetPostgresConnectionLimit);
	config.AddExtensionOption("pg_array_as_varchar", "Read Postgres arrays as varchar - enables reading mixed dimensional arrays", LogicalType::BOOLEAN, Value::BOOLEAN(false));
	config.AddExtensionOption("pg_experimental_filter_pushdown", "Whether or not to use filter pushdown (currently experimental)", LogicalType::BOOLEAN, Value::BOOLEAN(false));
	config.AddExtensionOption("pg_debug_show_queries", "DEBUG SETTING: print all queries sent to Postgres to stdout", LogicalType::BOOLEAN, Value::BOOLEAN(false), SetPostgresDebugQueryPrint);

}

void PostgresScannerExtension::Load(DuckDB &db) {
        LoadInternal(*db.instance);
}

DUCKDB_EXTENSION_API void postgres_scanner_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *postgres_scanner_version() {
	return DuckDB::LibraryVersion();
}

DUCKDB_EXTENSION_API void postgres_scanner_storage_init(DBConfig &config) {
	config.storage_extensions["postgres_scanner"] = make_uniq<PostgresStorageExtension>();
}

}
