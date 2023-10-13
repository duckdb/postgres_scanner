#include "storage/postgres_type_set.hpp"
#include "storage/postgres_transaction.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "storage/postgres_schema_entry.hpp"
#include "duckdb/parser/parser.hpp"
#include "postgres_conversion.hpp"

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
	auto type = LogicalType::ENUM(duckdb_levels, res->Count());
	type.SetAlias(type_name);
	return type;
}

void PostgresTypeSet::LoadEntries() {
	auto query = StringUtil::Replace(R"(
SELECT t.typname as type, t.typtype as typeid
FROM pg_type t
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
WHERE (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid))
AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid)
AND n.nspname=${SCHEMA_NAME};
)", "${SCHEMA_NAME}", KeywordHelper::WriteQuoted(schema.name));

	auto &conn = transaction.GetConnection();
	auto result = conn.Query(query);
	auto rows = result->Count();
	for(idx_t row = 0; row < rows; row++) {
		auto type = result->GetString(row, 1);
		if (type != "e") {
			// we only support enum types for now
			continue;
		}
		CreateTypeInfo info;
		info.name = result->GetString(row, 0);
		info.type = GetEnumType(info.name);
		auto type_entry = make_uniq<TypeCatalogEntry>(catalog, schema, info);
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
