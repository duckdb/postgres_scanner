#include "postgres_utils.hpp"
#include "storage/postgres_schema_entry.hpp"
#include "storage/postgres_transaction.hpp"

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
	if (input.HasAlias()) {
		return input.GetAlias();
	}
	switch(input.id()) {
	case LogicalTypeId::FLOAT:
		return "REAL";
	case LogicalTypeId::DOUBLE:
		return "FLOAT";
	case LogicalTypeId::BLOB:
		return "BYTEA";
	case LogicalTypeId::LIST:
		return PostgresUtils::TypeToString(ListType::GetChildType(input)) + "[]";
	case LogicalTypeId::ENUM:
		throw NotImplementedException("Enums in Postgres must be named - unnamed enums are not supported. Use CREATE TYPE to create a named enum.");
	case LogicalTypeId::STRUCT:
		throw NotImplementedException("Composite types in Postgres must be named - unnamed composite types are not supported. Use CREATE TYPE to create a named composite type.");
	case LogicalTypeId::MAP:
		throw NotImplementedException("MAP type not supported in Postgres");
	case LogicalTypeId::UNION:
		throw NotImplementedException("UNION type not supported in Postgres");
	default:
		return input.ToString();
	}
}

LogicalType PostgresUtils::RemoveAlias(const LogicalType &type) {
	switch(type.id()) {
	case LogicalTypeId::STRUCT: {
		auto child_types = StructType::GetChildTypes(type);
		return LogicalType::STRUCT(std::move(child_types));
	}
	case LogicalTypeId::ENUM: {
		auto &enum_vector = EnumType::GetValuesInsertOrder(type);
		Vector new_vector(LogicalType::VARCHAR);
		new_vector.Reference(enum_vector);
		return LogicalType::ENUM(new_vector, EnumType::GetSize(type));
	}
	default:
		throw InternalException("Unsupported logical type for RemoveAlias");
	}
}

LogicalType PostgresUtils::TypeToLogicalType(optional_ptr<PostgresTransaction> transaction, optional_ptr<PostgresSchemaEntry> schema, const PostgresTypeData &type_info, PostgresType &postgres_type) {
	auto &pgtypename = type_info.type_name;

	// TODO better check, does the typtyp say something here?
	// postgres array types start with an _
	if (StringUtil::StartsWith(pgtypename, "_")) {
		PostgresTypeData child_type_info;
		child_type_info.type_name = pgtypename.substr(1);
		PostgresType child_type;
		auto result = LogicalType::LIST(PostgresUtils::TypeToLogicalType(transaction, schema, child_type_info, child_type));
		postgres_type.children.push_back(std::move(child_type));
		return result;
	}

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
		auto width = ((type_info.type_modifier - sizeof(int32_t)) >> 16) & 0xffff;
		auto scale = (((type_info.type_modifier - sizeof(int32_t)) & 0x7ff) ^ 1024) - 1024;
		if (type_info.type_modifier == -1 || width < 0 || scale < 0 || width > 38) {
			// fallback to double
			postgres_type.info = PostgresTypeAnnotation::NUMERIC_AS_DOUBLE;
			return LogicalType::DOUBLE;
		}
		return LogicalType::DECIMAL(width, scale);
	} else if (pgtypename == "char" || pgtypename == "bpchar" || pgtypename == "varchar" || pgtypename == "text" ||
	           pgtypename == "json") {
		return LogicalType::VARCHAR;
	} else if (pgtypename == "jsonb") {
		postgres_type.info = PostgresTypeAnnotation::JSONB;
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
		if (!transaction) {
			// unsupported so fallback to varchar
			postgres_type.info = PostgresTypeAnnotation::CAST_TO_VARCHAR;
			return LogicalType::VARCHAR;
		}
		auto context = transaction->context.lock();
		if (!context) {
			throw InternalException("Context is destroyed!?");
		}
		auto entry = schema->GetEntry(CatalogTransaction(schema->ParentCatalog(), *context), CatalogType::TYPE_ENTRY, pgtypename);
		if (!entry) {
			// unsupported so fallback to varchar
			postgres_type.info = PostgresTypeAnnotation::CAST_TO_VARCHAR;
			return LogicalType::VARCHAR;
		}
		// custom type (e.g. composite or enum)
		auto &type_entry = entry->Cast<PostgresTypeEntry>();
		auto result_type = RemoveAlias(type_entry.user_type);
		postgres_type = type_entry.postgres_type;
		return result_type;
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
	case LogicalTypeId::ENUM:
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
	case LogicalTypeId::STRUCT: {
		child_list_t<LogicalType> new_types;
		for(idx_t c = 0; c < StructType::GetChildCount(input); c++) {
			auto &name = StructType::GetChildName(input, c);
			auto &type = StructType::GetChildType(input, c);
			new_types.push_back(make_pair(name, ToPostgresType(type)));
		}
		auto result = LogicalType::STRUCT(std::move(new_types));
		result.SetAlias(input.GetAlias());
		return result;
	}
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

PostgresType PostgresUtils::CreateEmptyPostgresType(const LogicalType &type) {
	PostgresType result;
	switch(type.id()) {
	case LogicalTypeId::STRUCT:
		for(auto &child_type : StructType::GetChildTypes(type)) {
			result.children.push_back(CreateEmptyPostgresType(child_type.second));
		}
		break;
	case LogicalTypeId::LIST:
		result.children.push_back(CreateEmptyPostgresType(ListType::GetChildType(type)));
		break;
	default:
		break;
	}
	return result;
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

bool PostgresUtils::SupportedPostgresOid(const LogicalType &input) {
	switch(input.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::BIT:
	case LogicalTypeId::UUID:
		return true;
	default:
		return false;
	}
}

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
