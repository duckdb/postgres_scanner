//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "postgres_utils.hpp"
#include "postgres_connection.hpp"

namespace duckdb {
class PostgresTransaction;

struct PostgresTypeInfo {
	string typname;
	int64_t typlen;
	string typtype;
	string nspname;
};

struct PostgresColumnInfo {
	string attname;
	int atttypmod;
	PostgresTypeInfo type_info;
	int64_t typelem; // OID pointer for arrays
	PostgresTypeInfo elem_info;
};

struct PostgresBindData : public FunctionData {
	string schema_name;
	string table_name;
	idx_t pages_approx = 0;

	vector<PostgresType> postgres_types;
	vector<string> names;
	vector<LogicalType> types;
	vector<bool> needs_cast;

	idx_t pages_per_task = 1000;
	string dsn;

	string snapshot;
	bool in_recovery;
	bool requires_materialization = false;

	PostgresConnection connection;
	optional_ptr<PostgresTransaction> transaction;

public:
	unique_ptr<FunctionData> Copy() const override {
		throw NotImplementedException("");
	}
	bool Equals(const FunctionData &other_p) const override {
		return false;
	}
};

class PostgresAttachFunction : public TableFunction {
public:
	PostgresAttachFunction();
};

class PostgresScanFunction : public TableFunction {
public:
	PostgresScanFunction();

	static void PrepareBind(ClientContext &context, PostgresBindData &bind);
};

class PostgresScanFunctionFilterPushdown : public TableFunction {
public:
	PostgresScanFunctionFilterPushdown();
};

} // namespace duckdb
