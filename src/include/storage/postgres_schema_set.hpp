//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_schema_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/postgres_catalog_set.hpp"
#include "storage/postgres_schema_entry.hpp"

namespace duckdb {

class PostgresSchemaSet : public PostgresCatalogSet {
public:
	explicit PostgresSchemaSet(Catalog &catalog, PostgresTransaction &transaction);

protected:
	void LoadEntries() override;
	string EntryName() override {
		return "SCHEMA";
	}
};

} // namespace duckdb
