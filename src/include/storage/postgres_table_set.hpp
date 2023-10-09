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

protected:
	void LoadEntries() override;
	string EntryName() override {
		return "TABLE";
	}

protected:
	PostgresSchemaEntry &schema;
};

} // namespace duckdb
