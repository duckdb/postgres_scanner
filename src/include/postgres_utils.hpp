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
class PostgresSchemaEntry;
class PostgresTransaction;

struct PostgresTypeData {
	string type_name;
	int32_t precision;
	int32_t scale;
};

enum class PostgresTypeAnnotation {
	STANDARD,
	CAST_TO_VARCHAR,
	NUMERIC_AS_DOUBLE,
	CTID,
	JSONB
};

struct PostgresType {
	idx_t oid = 0;
	PostgresTypeAnnotation info = PostgresTypeAnnotation::STANDARD;
	vector<PostgresType> children;
};

enum class PostgresCopyFormat {
	AUTO = 0,
	BINARY = 1,
	TEXT = 2
};

class PostgresUtils {
public:
	static PGconn *PGConnect(const string &dsn);

	static LogicalType ToPostgresType(const LogicalType &input);
	static LogicalType TypeToLogicalType(PostgresTransaction &transaction, PostgresSchemaEntry &schema, const PostgresTypeData &input, PostgresType &postgres_type);
	static string TypeToString(const LogicalType &input);
	static uint32_t ToPostgresOid(const LogicalType &input);
	static bool SupportedPostgresOid(const LogicalType &input);
	static LogicalType RemoveAlias(const LogicalType &type);
	static PostgresType CreateEmptyPostgresType(const LogicalType &type);
};

} // namespace duckdb
