//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_index_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/postgres_catalog_set.hpp"
#include "storage/postgres_index_entry.hpp"

namespace duckdb {
class PostgresSchemaEntry;

class PostgresIndexSet : public PostgresCatalogSet {
public:
	PostgresIndexSet(PostgresSchemaEntry &schema);

public:
	void Initialize(PostgresResultSlice &indexes);

	static string GetInitializeQuery();

	optional_ptr<CatalogEntry> CreateIndex(ClientContext &context, CreateIndexInfo &info, TableCatalogEntry &table);

protected:
	void LoadEntries(ClientContext &context) override;

protected:
	PostgresSchemaEntry &schema;
};

} // namespace duckdb
