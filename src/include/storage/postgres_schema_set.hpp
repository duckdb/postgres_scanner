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
struct CreateSchemaInfo;

class PostgresSchemaSet : public PostgresCatalogSet {
public:
	explicit PostgresSchemaSet(Catalog &catalog);

public:
	optional_ptr<CatalogEntry> CreateSchema(ClientContext &context, CreateSchemaInfo &info);

	static string GetInitializeQuery();

protected:
	void LoadEntries(ClientContext &context) override;
};

} // namespace duckdb
