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
#include "postgres_version.hpp"

namespace duckdb {
class PostgresSchemaEntry;
class PostgresTransaction;

struct PostgresTypeData {
	int64_t type_modifier = 0;
	string type_name;
	idx_t array_dimensions = 0;
};

enum class PostgresTypeAnnotation { STANDARD, CAST_TO_VARCHAR, NUMERIC_AS_DOUBLE, CTID, JSONB, FIXED_LENGTH_CHAR };

struct PostgresType {
	idx_t oid = 0;
	PostgresTypeAnnotation info = PostgresTypeAnnotation::STANDARD;
	vector<PostgresType> children;
};

enum class PostgresCopyFormat { AUTO = 0, BINARY = 1, TEXT = 2 };

class PostgresUtils {
public:
	static PGconn *PGConnect(const string &dsn);

	static LogicalType ToPostgresType(const LogicalType &input);
	static LogicalType TypeToLogicalType(optional_ptr<PostgresTransaction> transaction,
	                                     optional_ptr<PostgresSchemaEntry> schema, const PostgresTypeData &input,
	                                     PostgresType &postgres_type);
	static string TypeToString(const LogicalType &input);
	static uint32_t ToPostgresOid(const LogicalType &input);
	static bool SupportedPostgresOid(const LogicalType &input);
	static LogicalType RemoveAlias(const LogicalType &type);
	static PostgresType CreateEmptyPostgresType(const LogicalType &type);

	static PostgresVersion ExtractPostgresVersion(const string &version);
};

} // namespace duckdb
