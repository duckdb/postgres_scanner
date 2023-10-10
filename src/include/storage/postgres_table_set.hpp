//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_table_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/postgres_catalog_set.hpp"
#include "storage/postgres_table_entry.hpp"

namespace duckdb {
class PostgresSchemaEntry;

class PostgresTableSet : public PostgresCatalogSet {
public:
	explicit PostgresTableSet(PostgresSchemaEntry &schema, PostgresTransaction &transaction);

public:
	optional_ptr<CatalogEntry> CreateTable(BoundCreateTableInfo &info);

	static unique_ptr<CreateTableInfo> GetTableInfo(PostgresResult &result, const string &table_name);

protected:
	void LoadEntries() override;

protected:
	PostgresSchemaEntry &schema;
};

} // namespace duckdb
