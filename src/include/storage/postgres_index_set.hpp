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
class TableCatalogEntry;

class PostgresIndexSet : public PostgresInSchemaSet {
public:
	PostgresIndexSet(PostgresSchemaEntry &schema, unique_ptr<PostgresResultSlice> index_result = nullptr);

public:
	static string GetInitializeQuery();

	optional_ptr<CatalogEntry> CreateIndex(ClientContext &context, CreateIndexInfo &info, TableCatalogEntry &table);

protected:
	void LoadEntries(ClientContext &context) override;

protected:
	unique_ptr<PostgresResultSlice> index_result;
};

} // namespace duckdb
