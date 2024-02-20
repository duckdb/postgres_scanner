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
class PostgresConnection;
class PostgresResult;
class PostgresSchemaEntry;

class PostgresTableSet : public PostgresInSchemaSet {
public:
	explicit PostgresTableSet(PostgresSchemaEntry &schema, unique_ptr<PostgresResultSlice> tables = nullptr);

public:
	optional_ptr<CatalogEntry> CreateTable(ClientContext &context, BoundCreateTableInfo &info);

	static unique_ptr<PostgresTableInfo> GetTableInfo(PostgresTransaction &transaction, PostgresSchemaEntry &schema,
	                                                  const string &table_name);
	static unique_ptr<PostgresTableInfo> GetTableInfo(PostgresConnection &connection, const string &schema_name,
	                                                  const string &table_name);
	optional_ptr<CatalogEntry> ReloadEntry(ClientContext &context, const string &table_name) override;

	void AlterTable(ClientContext &context, AlterTableInfo &info);

	static string GetInitializeQuery();

protected:
	void LoadEntries(ClientContext &context) override;
	bool SupportReload() const override {
		return true;
	}

	void AlterTable(ClientContext &context, RenameTableInfo &info);
	void AlterTable(ClientContext &context, RenameColumnInfo &info);
	void AlterTable(ClientContext &context, AddColumnInfo &info);
	void AlterTable(ClientContext &context, RemoveColumnInfo &info);

	static void AddColumn(optional_ptr<PostgresTransaction> transaction, optional_ptr<PostgresSchemaEntry> schema,
	                      PostgresResult &result, idx_t row, PostgresCreateInfo &pg_create_info, idx_t column_offset = 0);

	void CreateEntries(PostgresTransaction &transaction, PostgresResult &result, idx_t start, idx_t end);

protected:
	unique_ptr<PostgresResultSlice> table_result;
};

} // namespace duckdb
