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
struct CreateTableInfo;
class PostgresResult;
class PostgresSchemaEntry;

class PostgresTableSet : public PostgresCatalogSet {
public:
	explicit PostgresTableSet(PostgresSchemaEntry &schema, PostgresTransaction &transaction);

public:
	optional_ptr<CatalogEntry> CreateTable(BoundCreateTableInfo &info);

	unique_ptr<PostgresTableInfo> GetTableInfo(PostgresResult &result, const string &table_name);

	void AlterTable(ClientContext &context, AlterTableInfo &info);

protected:
	void LoadEntries() override;

	void AlterTable(RenameTableInfo &info);
	void AlterTable(RenameColumnInfo &info);
	void AlterTable(AddColumnInfo &info);
	void AlterTable(RemoveColumnInfo &info);

	void AddColumn(PostgresResult &result, idx_t row, PostgresTableInfo &table_info, idx_t column_offset = 0);

protected:
	PostgresSchemaEntry &schema;
};

} // namespace duckdb
