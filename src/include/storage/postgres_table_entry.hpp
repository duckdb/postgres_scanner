//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_table_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "postgres_utils.hpp"

namespace duckdb {

struct PostgresTableInfo {
	PostgresTableInfo() {
		create_info = make_uniq<CreateTableInfo>();
	}
	PostgresTableInfo(const string &schema, const string &table) {
		create_info = make_uniq<CreateTableInfo>(string(), schema, table);
	}
	PostgresTableInfo(const SchemaCatalogEntry &schema, const string &table) {
		create_info = make_uniq<CreateTableInfo>(schema, table);
	}

	const string &GetTableName() {
		return create_info->table;
	}

	unique_ptr<CreateTableInfo> create_info;
	vector<PostgresType> postgres_types;
	idx_t approx_num_pages;
};

class PostgresTableEntry : public TableCatalogEntry {
public:
	PostgresTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info);
	PostgresTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, PostgresTableInfo &info);

public:
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;

	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;

	TableStorageInfo GetStorageInfo(ClientContext &context) override;

	void BindUpdateConstraints(LogicalGet &get, LogicalProjection &proj, LogicalUpdate &update,
	                           ClientContext &context) override;

	//! Get the copy format (text or binary) that should be used when writing data to this table
	PostgresCopyFormat GetCopyFormat(ClientContext &context);

public:
	vector<PostgresType> postgres_types;
	idx_t approx_num_pages;
};

} // namespace duckdb
