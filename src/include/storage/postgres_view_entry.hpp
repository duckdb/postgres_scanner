//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_view_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "storage/postgres_create_info.hpp"

namespace duckdb {

struct PostgresViewInfo : public PostgresCreateInfo {
public:
	PostgresViewInfo() {
		create_info = make_uniq<CreateViewInfo>();
		// create_info->columns.SetAllowDuplicates(true);
	}
	PostgresViewInfo(const string &schema, const string &view) {
		create_info = make_uniq<CreateViewInfo>(string(), schema, view);
		// create_info->columns.SetAllowDuplicates(true);
	}
	PostgresViewInfo(const SchemaCatalogEntry &schema, const string &view) {
		create_info = make_uniq<CreateViewInfo>((SchemaCatalogEntry &)schema, view);
		// create_info->columns.SetAllowDuplicates(true);
	}
	~PostgresViewInfo() override {
	}

public:
	const string &GetName() const override {
		return create_info->view_name;
	}

	CreateInfo &GetCreateInfo() override {
		return *create_info;
	}

	void AddColumn(ColumnDefinition def, PostgresType pg_type, const string &pg_name) override {
		postgres_types.push_back(std::move(pg_type));
		D_ASSERT(!pg_name.empty());
		postgres_names.push_back(pg_name);
		create_info->types.push_back(def.Type());
		create_info->names.push_back(def.Name());
	}

	void GetColumnNamesAndTypes(vector<string> &names, vector<LogicalType> &types) override {
		names = create_info->names;
		types = create_info->types;
	}

public:
	unique_ptr<CreateViewInfo> create_info;
};

class PostgresViewEntry : public ViewCatalogEntry {
public:
	PostgresViewEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateViewInfo &info);
	PostgresViewEntry(Catalog &catalog, SchemaCatalogEntry &schema, PostgresViewInfo &info);
	~PostgresViewEntry() override;

public:
	//! Postgres type annotations
	vector<PostgresType> postgres_types;
	//! Column names as they are within Postgres
	//! We track these separately because of case sensitivity - Postgres allows e.g. the columns "ID" and "id" together
	//! We would in this case remap them to "ID" and "id_1", while postgres_names store the original names
	vector<string> postgres_names;
	//! The approximate number of pages a table consumes in Postgres
	idx_t approx_num_pages;
};

} // namespace duckdb
