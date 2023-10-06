#include "postgres_utils.hpp"

namespace duckdb {

PGconn *PostgresUtils::PGConnect(const string &dsn) {
	PGconn *conn = PQconnectdb(dsn.c_str());

	// both PQStatus and PQerrorMessage check for nullptr
	if (PQstatus(conn) == CONNECTION_BAD) {
		throw IOException("Unable to connect to Postgres at %s: %s", dsn, string(PQerrorMessage(conn)));
	}
	return conn;
}

LogicalType PostgresUtils::TypeToLogicalType(const string &input) {
	if (input == "integer") {
		return LogicalType::INTEGER;
	}
	if (input == "bigint") {
		return LogicalType::BIGINT;
	}
	if (input == "date") {
		return LogicalType::DATE;
	}
	if (input == "character varying") {
		return LogicalType::VARCHAR;
	}
	throw NotImplementedException("Unsupported type for PostgresUtils::TypeToLogicalType: %s", input);
}

LogicalType PostgresUtils::ToPostgresType(const LogicalType &input) {
	switch (input.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
		return LogicalType::BIGINT;
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		return LogicalType::DOUBLE;
	case LogicalTypeId::BLOB:
		return LogicalType::BLOB;
	default:
		return LogicalType::VARCHAR;
	}
}

}
