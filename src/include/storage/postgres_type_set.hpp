//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_type_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/postgres_catalog_set.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"

namespace duckdb {
struct CreateTableInfo;
class PostgresResult;
class PostgresSchemaEntry;

class PostgresTypeSet : public PostgresCatalogSet {
public:
	explicit PostgresTypeSet(PostgresSchemaEntry &schema, PostgresTransaction &transaction);

public:
	optional_ptr<CatalogEntry> CreateType(CreateTypeInfo &info);

protected:
	void LoadEntries() override;

	LogicalType GetEnumType(const string &type_name);

protected:
	PostgresSchemaEntry &schema;
};

} // namespace duckdb
