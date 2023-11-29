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
	explicit PostgresTypeSet(PostgresSchemaEntry &schema);

public:
	optional_ptr<CatalogEntry> CreateType(ClientContext &context, CreateTypeInfo &info);

	void Initialize(PostgresTransaction &transaction, PostgresResultSlice &enums, PostgresResultSlice &composite_types);

	static string GetInitializeEnumsQuery();
	static string GetInitializeCompositesQuery();

protected:
	void LoadEntries(ClientContext &context) override;

	void CreateEnum(PostgresResult &result, idx_t start_row, idx_t end_row);
	void CreateCompositeType(PostgresTransaction &transaction, PostgresResult &result, idx_t start_row, idx_t end_row);

	void InitializeEnums(PostgresResultSlice &enums);
	void InitializeCompositeTypes(PostgresTransaction &transaction, PostgresResultSlice &composite_types);


protected:
	PostgresSchemaEntry &schema;
};

} // namespace duckdb
