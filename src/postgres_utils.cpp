#include "postgres_utils.hpp"

namespace duckdb {

static void
PGNoticeProcessor(void *arg, const char *message) {
}

PGconn *PostgresUtils::PGConnect(const string &dsn) {
	PGconn *conn = PQconnectdb(dsn.c_str());

	// both PQStatus and PQerrorMessage check for nullptr
	if (PQstatus(conn) == CONNECTION_BAD) {
		throw IOException("Unable to connect to Postgres at %s: %s", dsn, string(PQerrorMessage(conn)));
	}
	PQsetNoticeProcessor(conn, PGNoticeProcessor, nullptr);
	return conn;
}

string PostgresUtils::TypeToString(const LogicalType &input) {
	switch(input.id()) {
	case LogicalTypeId::FLOAT:
		return "REAL";
	case LogicalTypeId::DOUBLE:
		return "FLOAT";
	case LogicalTypeId::BLOB:
		return "BYTEA";
	case LogicalTypeId::LIST:
		return PostgresUtils::TypeToString(ListType::GetChildType(input)) + "[]";
	default:
		return input.ToString();
	}
}

LogicalType PostgresUtils::TypeToLogicalType(const PostgresTypeData &type_info) {
	auto &pgtypename = type_info.type_name;
	if (StringUtil::StartsWith(pgtypename, "_")) {
		PostgresTypeData child_type_info;
		child_type_info.type_name = pgtypename.substr(1);
		return LogicalType::LIST(TypeToLogicalType(child_type_info));
	}

//	if (type_info->typtype == "e") { // ENUM
//		throw NotImplementedException("FIXME: enum");
//	}

	if (pgtypename == "bool") {
		return LogicalType::BOOLEAN;
	} else if (pgtypename == "int2") {
		return LogicalType::SMALLINT;
	} else if (pgtypename == "int4") {
		return LogicalType::INTEGER;
	} else if (pgtypename == "int8") {
		return LogicalType::BIGINT;
	} else if (pgtypename == "oid") { // "The oid type is currently implemented as an unsigned four-byte integer."
		return LogicalType::UINTEGER;
	} else if (pgtypename == "float4") {
		return LogicalType::FLOAT;
	} else if (pgtypename == "float8") {
		return LogicalType::DOUBLE;
	} else if (pgtypename == "numeric") {
		if (type_info.precision < 0 || type_info.scale < 0 || type_info.precision > 38) {
			// fallback to double
			return LogicalType::DOUBLE;
		}
		return LogicalType::DECIMAL(type_info.precision, type_info.scale);
	} else if (pgtypename == "char" || pgtypename == "bpchar" || pgtypename == "varchar" || pgtypename == "text" ||
	           pgtypename == "jsonb" || pgtypename == "json") {
		return LogicalType::VARCHAR;
	} else if (pgtypename == "date") {
		return LogicalType::DATE;
	} else if (pgtypename == "bytea") {
		return LogicalType::BLOB;
	} else if (pgtypename == "time") {
		return LogicalType::TIME;
	} else if (pgtypename == "timetz") {
		return LogicalType::TIME_TZ;
	} else if (pgtypename == "timestamp") {
		return LogicalType::TIMESTAMP;
	} else if (pgtypename == "timestamptz") {
		return LogicalType::TIMESTAMP_TZ;
	} else if (pgtypename == "interval") {
		return LogicalType::INTERVAL;
	} else if (pgtypename == "uuid") {
		return LogicalType::UUID;
	} else {
		// unsupported so fallback to varchar
		return LogicalType::VARCHAR;
	}
}

LogicalType PostgresUtils::ToPostgresType(const LogicalType &input) {
	switch (input.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::DATE:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::UUID:
	case LogicalTypeId::VARCHAR:
		return input;
	case LogicalTypeId::LIST:
		return LogicalType::LIST(ToPostgresType(ListType::GetChildType(input)));
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		return LogicalType::TIMESTAMP;
	case LogicalTypeId::TINYINT:
		return LogicalType::SMALLINT;
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
		return LogicalType::BIGINT;
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
		return LogicalType::DOUBLE;
	default:
		return LogicalType::VARCHAR;
	}
}

// taken from pg_type_d.h
#define BOOLOID 16
#define BYTEAOID 17
#define INT8OID 20
#define INT2OID 21
#define INT4OID 23
#define FLOAT4OID 700
#define FLOAT8OID 701
#define VARCHAROID 1043
#define DATEOID 1082
#define TIMEOID 1083
#define TIMESTAMPOID 1114
#define TIMESTAMPTZOID 1184
#define INTERVALOID 1186
#define TIMETZOID 1266
#define BITOID 1560
#define UUIDOID 2950

uint32_t PostgresUtils::ToPostgresOid(const LogicalType &input) {
	switch(input.id()) {
	case LogicalTypeId::BOOLEAN:
		return BOOLOID;
	case LogicalTypeId::SMALLINT:
		return INT2OID;
	case LogicalTypeId::INTEGER:
		return INT4OID;
	case LogicalTypeId::BIGINT:
		return INT8OID;
	case LogicalTypeId::FLOAT:
		return FLOAT4OID;
	case LogicalTypeId::DOUBLE:
		return FLOAT8OID;
	case LogicalTypeId::VARCHAR:
		return VARCHAROID;
	case LogicalTypeId::BLOB:
		return BYTEAOID;
	case LogicalTypeId::DATE:
		return DATEOID;
	case LogicalTypeId::TIME:
		return TIMEOID;
	case LogicalTypeId::TIMESTAMP:
		return TIMESTAMPOID;
	case LogicalTypeId::INTERVAL:
		return INTERVALOID;
	case LogicalTypeId::TIME_TZ:
		return TIMETZOID;
	case LogicalTypeId::TIMESTAMP_TZ:
		return TIMESTAMPTZOID;
	case LogicalTypeId::BIT:
		return BITOID;
	case LogicalTypeId::UUID:
		return UUIDOID;
	default:
		throw NotImplementedException("Unsupported type for Postgres array copy: %s", input.ToString());
	}
}

}
