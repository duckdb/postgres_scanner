//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_catalog.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "postgres_connection.hpp"
#include "storage/postgres_schema_set.hpp"
#include "storage/postgres_connection_pool.hpp"

namespace duckdb {
class PostgresSchemaEntry;

class PostgresCatalog : public Catalog {
public:
	explicit PostgresCatalog(AttachedDatabase &db_p, const string &path, AccessMode access_mode);
	~PostgresCatalog();

	string path;
	AccessMode access_mode;

public:
	void Initialize(bool load_builtin) override;
	string GetCatalogType() override {
		return "postgres";
	}

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;

	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

	optional_ptr<SchemaCatalogEntry> GetSchema(CatalogTransaction transaction, const string &schema_name,
	                                           OnEntryNotFound if_not_found,
	                                           QueryErrorContext error_context = QueryErrorContext()) override;

	unique_ptr<PhysicalOperator> PlanInsert(ClientContext &context, LogicalInsert &op,
	                                        unique_ptr<PhysicalOperator> plan) override;
	unique_ptr<PhysicalOperator> PlanCreateTableAs(ClientContext &context, LogicalCreateTable &op,
	                                               unique_ptr<PhysicalOperator> plan) override;
	unique_ptr<PhysicalOperator> PlanDelete(ClientContext &context, LogicalDelete &op,
	                                        unique_ptr<PhysicalOperator> plan) override;
	unique_ptr<PhysicalOperator> PlanUpdate(ClientContext &context, LogicalUpdate &op,
	                                        unique_ptr<PhysicalOperator> plan) override;
	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
	                                            unique_ptr<LogicalOperator> plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;

	//! Label all postgres scans in the sub-tree as requiring materialization
	//! This is used for e.g. insert queries that have both (1) a scan from a postgres table, and (2) a sink into one
	static void MaterializePostgresScans(PhysicalOperator &op);

	//! Whether or not this is an in-memory Postgres database
	bool InMemory() override;
	string GetDBPath() override;

	PostgresConnectionPool &GetConnectionPool() {
		return connection_pool;
	}

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;

private:
	PostgresSchemaSet schemas;
	PostgresConnectionPool connection_pool;
};

} // namespace duckdb
