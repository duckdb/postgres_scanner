//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include <libpq-fe.h>

namespace duckdb {

struct PostgresTypeData {
	string type_name;
	int32_t precision;
	int32_t scale;
};

class PostgresUtils {
public:
	static PGconn *PGConnect(const string &dsn);

	static LogicalType ToPostgresType(const LogicalType &input);
	static LogicalType TypeToLogicalType(const PostgresTypeData &input);
	static string TypeToString(const LogicalType &input);
};

} // namespace duckdb
