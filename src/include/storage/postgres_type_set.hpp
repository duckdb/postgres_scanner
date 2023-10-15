//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_type_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/postgres_catalog_set.hpp"
#include "storage/postgres_type_entry.hpp"

namespace duckdb {
struct CreateTableInfo;
class PostgresResult;
class PostgresSchemaEntry;
struct PGTypeInfo;

class PostgresTypeSet : public PostgresCatalogSet {
public:
	explicit PostgresTypeSet(PostgresSchemaEntry &schema, PostgresTransaction &transaction);

public:
	optional_ptr<CatalogEntry> CreateType(CreateTypeInfo &info);

protected:
	void LoadEntries() override;

	void LoadEnumTypes(vector<PGTypeInfo> &enum_info);
	void CreateEnum(PostgresResult &result, idx_t start_row, idx_t end_row);
	void LoadCompositeTypes(vector<PGTypeInfo> &composite_info);
	void CreateCompositeType(PostgresResult &result, idx_t start_row, idx_t end_row, unordered_map<idx_t, string> &name_map);

protected:
	PostgresSchemaEntry &schema;
};

} // namespace duckdb
