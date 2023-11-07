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
#include "storage/postgres_connection_pool.hpp"

namespace duckdb {
class PostgresTransaction;

struct PostgresBindData : public FunctionData {
	static constexpr const idx_t DEFAULT_PAGES_PER_TASK = 1000;

	string schema_name;
	string table_name;
	string sql;
	idx_t pages_approx = 0;

	vector<PostgresType> postgres_types;
	vector<string> names;
	vector<LogicalType> types;

	idx_t pages_per_task = DEFAULT_PAGES_PER_TASK;
	string dsn;

	string snapshot;
	bool in_recovery;
	bool requires_materialization = false;
	bool read_only = true;
	idx_t max_threads = 1;

	PostgresConnection connection;
	optional_ptr<PostgresTransaction> transaction;
	PostgresConnectionReservation connection_reservation;

public:
	void SetTablePages(idx_t approx_num_pages);

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

	static void PrepareBind(PostgresVersion version, ClientContext &context, PostgresBindData &bind);
};

class PostgresScanFunctionFilterPushdown : public TableFunction {
public:
	PostgresScanFunctionFilterPushdown();
};

class PostgresClearCacheFunction : public TableFunction {
public:
	PostgresClearCacheFunction();
};

class PostgresQueryFunction : public TableFunction {
public:
	PostgresQueryFunction();
};

} // namespace duckdb
