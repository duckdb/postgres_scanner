#include "storage/postgres_type_set.hpp"
#include "storage/postgres_transaction.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "storage/postgres_type_entry.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "storage/postgres_schema_entry.hpp"
#include "duckdb/parser/parser.hpp"

namespace duckdb {

PostgresTypeSet::PostgresTypeSet(PostgresSchemaEntry &schema, PostgresTransaction &transaction) :
    PostgresCatalogSet(schema.ParentCatalog(), transaction), schema(schema) {}

LogicalType PostgresTypeSet::GetEnumType(const string &type_name) {
	auto &conn = transaction.GetConnection();
	auto res = conn.Query(
		StringUtil::Format("SELECT unnest(enum_range(NULL::%s.%s))",
						   KeywordHelper::WriteQuoted(schema.name, '"'),
						   KeywordHelper::WriteQuoted(type_name, '"')));
	Vector duckdb_levels(LogicalType::VARCHAR, res->Count());
	for (idx_t row = 0; row < res->Count(); row++) {
		duckdb_levels.SetValue(row, res->GetString(row, 0));
	}
	return LogicalType::ENUM(duckdb_levels, res->Count());
}

LogicalType PostgresTypeSet::GetCompositeType(idx_t oid, PostgresType &postgres_type) {
	auto query = StringUtil::Format(R"(
SELECT attname, typname
FROM pg_attribute JOIN pg_type ON (pg_attribute.atttypid=pg_type.oid)
WHERE attrelid=%llu
ORDER BY attnum;
)", oid);

	auto &conn = transaction.GetConnection();
	auto res = conn.Query(query);
	child_list_t<LogicalType> child_types;
	for (idx_t row = 0; row < res->Count(); row++) {
		auto type_name = res->GetString(row, 0);
		PostgresTypeData type_data;
		type_data.type_name = res->GetString(row, 1);
		PostgresType child_type;
		child_types.push_back(make_pair(type_name, PostgresUtils::TypeToLogicalType(transaction, schema, type_data, child_type)));
		postgres_type.children.push_back(std::move(child_type));
	}
	return LogicalType::STRUCT(std::move(child_types));
}

void PostgresTypeSet::LoadEntries() {
	auto query = StringUtil::Replace(R"(
SELECT t.typrelid AS id, t.typname as type, t.typtype as typeid
FROM pg_type t
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
WHERE (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid))
AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid)
AND n.nspname=${SCHEMA_NAME};
)", "${SCHEMA_NAME}", KeywordHelper::WriteQuoted(schema.name));

	auto &conn = transaction.GetConnection();
	auto result = conn.Query(query);
	auto rows = result->Count();
	// FIXME: reduce number of queries here to 3 queries (one to get list of types, one for enums, one for composite types)
	for(idx_t row = 0; row < rows; row++) {
		auto id = result->GetInt64(row, 0);
		auto type = result->GetString(row, 2);
		if (type != "e" && type != "c") {
			// unsupported type;
			continue;
		}
		PostgresType postgres_type;
		CreateTypeInfo info;
		info.name = result->GetString(row, 1);
		if (type == "e") {
			// enum
			info.type = GetEnumType(info.name);
		} else if (type == "c") {
			// composite
			info.type = GetCompositeType(id, postgres_type);
		}
		info.type.SetAlias(info.name);
		auto type_entry = make_uniq<PostgresTypeEntry>(catalog, schema, info, postgres_type);
		CreateEntry(std::move(type_entry));
	}
}

string GetCreateTypeSQL(CreateTypeInfo &info) {
	string sql = "CREATE TYPE ";
	sql += KeywordHelper::WriteQuoted(info.name, '"');
	sql += " AS ";
	switch (info.type.id()) {
	case LogicalTypeId::ENUM: {
		sql += "ENUM(";
		auto enum_size = EnumType::GetSize(info.type);
		for(idx_t i = 0; i < enum_size; i++) {
			if (i > 0) {
				sql += ", ";
			}
			auto enum_value = EnumType::GetString(info.type, i).GetString();
			sql += KeywordHelper::WriteQuoted(enum_value, '\'');
		}
		sql += ")";
		break;
	}
	case LogicalTypeId::STRUCT: {
		auto child_count = StructType::GetChildCount(info.type);
		sql += "(";
		for(idx_t c = 0; c < child_count; c++) {
			if (c > 0) {
				sql += ", ";
			}
			sql += KeywordHelper::WriteQuoted(StructType::GetChildName(info.type, c), '"');
			sql += " ";
			sql += PostgresUtils::TypeToString(StructType::GetChildType(info.type, c));
		}
		sql += ")";
		break;
	}
	default:
		throw BinderException("Unsupported type for CREATE TYPE in Postgres");
	}
	return sql;
}

optional_ptr<CatalogEntry> PostgresTypeSet::CreateType(CreateTypeInfo &info) {
	auto &conn = transaction.GetConnection();

	auto create_sql = GetCreateTypeSQL(info);
	conn.Execute(create_sql);
	auto type_entry = make_uniq<TypeCatalogEntry>(catalog, schema, info);
	return CreateEntry(std::move(type_entry));
}


}
