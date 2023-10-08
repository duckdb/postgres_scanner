#include "storage/postgres_catalog.hpp"
#include "storage/postgres_schema_entry.hpp"
#include "storage/postgres_transaction.hpp"
#include "postgres_connection.hpp"
#include "duckdb/storage/database_size.hpp"

namespace duckdb {

PostgresCatalog::PostgresCatalog(AttachedDatabase &db_p, const string &path, AccessMode access_mode)
    : Catalog(db_p), path(path), access_mode(access_mode) {
}

PostgresCatalog::~PostgresCatalog() {
}

void PostgresCatalog::Initialize(bool load_builtin) {
	main_schema = make_uniq<PostgresSchemaEntry>(*this);
}

optional_ptr<CatalogEntry> PostgresCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	throw BinderException("Postgres databases do not support creating new schemas");
}

void PostgresCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	callback(*main_schema);
}

optional_ptr<SchemaCatalogEntry> PostgresCatalog::GetSchema(CatalogTransaction transaction, const string &schema_name,
                                                          OnEntryNotFound if_not_found,
                                                          QueryErrorContext error_context) {
	if (schema_name == "public" || schema_name == DEFAULT_SCHEMA || schema_name == INVALID_SCHEMA) {
		return main_schema.get();
	}
	if (if_not_found == OnEntryNotFound::RETURN_NULL) {
		return nullptr;
	}
	throw BinderException("Postgres databases only have a single schema - \"%s\"", DEFAULT_SCHEMA);
}

bool PostgresCatalog::InMemory() {
	return false;
}

string PostgresCatalog::GetDBPath() {
	return string();
}

void PostgresCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	throw BinderException("Postgres databases do not support dropping schemas");
}

DatabaseSize PostgresCatalog::GetDatabaseSize(ClientContext &context) {
	throw InternalException("GetDatabaseSize");
}

unique_ptr<PhysicalOperator> PostgresCatalog::PlanUpdate(ClientContext &context, LogicalUpdate &op,
										unique_ptr<PhysicalOperator> plan) {
	throw InternalException("FIXME: PostgresCatalog::PlanUpdate");
}

unique_ptr<LogicalOperator> PostgresCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
											unique_ptr<LogicalOperator> plan) {
	throw InternalException("FIXME: PostgresCatalog::BindCreateIndex");
}


} // namespace duckdb
