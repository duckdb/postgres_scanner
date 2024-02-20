//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_create_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "postgres_utils.hpp"

namespace duckdb {

struct PostgresCreateInfo {
public:
	PostgresCreateInfo() {
	}
	virtual ~PostgresCreateInfo() {
	}

public:
	virtual CreateInfo &GetCreateInfo() = 0;
	virtual const string &GetName() const = 0;
	virtual void AddColumn(ColumnDefinition def, PostgresType pg_type, const string &pg_name) = 0;

public:
	template <class TARGET>
	TARGET &Cast() {
		D_ASSERT(dynamic_cast<TARGET *>(this));
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		return reinterpret_cast<const TARGET &>(*this);
	}

public:
	idx_t approx_num_pages = 0;
};

} // namespace duckdb
