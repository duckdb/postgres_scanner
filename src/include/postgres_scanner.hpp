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

namespace duckdb {

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
	PostgresBindData() : owns_connection(true) {
	}
	PostgresBindData(PGconn *conn) : conn(conn), owns_connection(false) {
	}
	~PostgresBindData() {
		if (owns_connection && conn) {
			PQfinish(conn);
			conn = nullptr;
		}
	}

	string schema_name;
	string table_name;
	idx_t pages_approx = 0;

	vector<PostgresColumnInfo> columns;
	vector<string> names;
	vector<LogicalType> types;
	vector<bool> needs_cast;

	idx_t pages_per_task = 1000;
	string dsn;

	string snapshot;
	bool in_recovery;

	PGconn *conn = nullptr;
	bool owns_connection;

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
