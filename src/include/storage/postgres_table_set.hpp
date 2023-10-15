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
	explicit PostgresTableSet(PostgresSchemaEntry &schema);

public:
	optional_ptr<CatalogEntry> CreateTable(ClientContext &context, BoundCreateTableInfo &info);

	optional_ptr<CatalogEntry> RefreshTable(ClientContext &context, const string &table_name);

	void AlterTable(ClientContext &context, AlterTableInfo &info);

protected:
	void LoadEntries(ClientContext &context) override;

	void AlterTable(ClientContext &context, RenameTableInfo &info);
	void AlterTable(ClientContext &context, RenameColumnInfo &info);
	void AlterTable(ClientContext &context, AddColumnInfo &info);
	void AlterTable(ClientContext &context, RemoveColumnInfo &info);

	void AddColumn(PostgresTransaction &transaction, PostgresResult &result, idx_t row, PostgresTableInfo &table_info, idx_t column_offset = 0);

protected:
	PostgresSchemaEntry &schema;
};

} // namespace duckdb
