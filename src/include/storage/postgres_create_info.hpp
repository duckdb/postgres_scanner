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

enum class PostgresCreateInfoType : uint8_t { TABLE, VIEW };

struct PostgresCreateInfo {
public:
	PostgresCreateInfo(PostgresCreateInfoType type) : type(type) {
	}
	virtual ~PostgresCreateInfo() {
	}

public:
	virtual CreateInfo &GetCreateInfo() = 0;
	virtual const string &GetName() const = 0;
	virtual void AddColumn(ColumnDefinition def, PostgresType pg_type, const string &pg_name) = 0;
	virtual void GetColumnNamesAndTypes(vector<string> &names, vector<LogicalType> &types) = 0;
	virtual idx_t PhysicalColumnCount() const = 0;
	virtual void AddConstraint(unique_ptr<Constraint> constraint) = 0;
	PostgresCreateInfoType GetType() const {
		return type;
	}

public:
	template <class TARGET>
	TARGET &Cast() {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast PostgresCreateInfo to type - PostgresCreateInfoType mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast PostgresCreateInfo to type - PostgresCreateInfoType mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}

public:
	idx_t approx_num_pages = 0;
	vector<PostgresType> postgres_types;
	vector<string> postgres_names;

protected:
	PostgresCreateInfoType type;
};

} // namespace duckdb
