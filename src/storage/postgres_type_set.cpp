#include "storage/postgres_type_set.hpp"
#include "storage/postgres_transaction.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "storage/postgres_type_entry.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "storage/postgres_schema_entry.hpp"
#include "duckdb/parser/parser.hpp"

namespace duckdb {

struct PGTypeInfo {
	idx_t oid;
	idx_t typrelid;
	string name;
};

PostgresTypeSet::PostgresTypeSet(PostgresSchemaEntry &schema) :
    PostgresCatalogSet(schema.ParentCatalog()), schema(schema) {}

void PostgresTypeSet::CreateEnum(PostgresResult &result, idx_t start_row, idx_t end_row) {
	PostgresType postgres_type;
	CreateTypeInfo info;
	postgres_type.oid = result.GetInt64(start_row, 0);
	info.name = result.GetString(start_row, 1);
	// construct the enum
	idx_t enum_count = end_row - start_row;
	Vector duckdb_levels(LogicalType::VARCHAR, enum_count);
	for(idx_t enum_idx = 0; enum_idx < enum_count; enum_idx++) {
		duckdb_levels.SetValue(enum_idx, result.GetString(start_row + enum_idx, 2));
	}
	info.type = LogicalType::ENUM(duckdb_levels, enum_count);
	info.type.SetAlias(info.name);
	auto type_entry = make_uniq<PostgresTypeEntry>(catalog, schema, info, postgres_type);
	CreateEntry(std::move(type_entry));
}

void PostgresTypeSet::LoadEnumTypes(PostgresTransaction &transaction, vector<PGTypeInfo> &enum_info) {
	if (enum_info.empty()) {
		return;
	}

	auto &conn = transaction.GetConnection();
	// compose the query
	// we create a single big query that uses UNION ALL to get the values of all enums at the same time
	string query;
	for(auto &info : enum_info) {
		if (!query.empty()) {
			query += "\nUNION ALL\n";
		}
		query += StringUtil::Format("SELECT %d AS relid, %s AS enum_name, UNNEST(ENUM_RANGE(NULL::%s.%s))::VARCHAR",
		                            info.oid,
		                            KeywordHelper::WriteQuoted(info.name, '\''),
		                            KeywordHelper::WriteQuoted(schema.name, '"'),
		                            KeywordHelper::WriteQuoted(info.name, '"'));
	}
	// now construct the enums
	auto result = conn.Query(query);
	auto count = result->Count();
	idx_t start = 0;
	idx_t current_oid = idx_t(-1);
	for (idx_t row = 0; row < count; row++) {
		auto oid = result->GetInt64(row, 0);
		if (oid != current_oid) {
			if (row > start) {
				CreateEnum(*result, start, row);
			}
			start = row;
			current_oid = oid;
		}
	}
	if (count > start) {
		CreateEnum(*result, start, count);
	}
}

void PostgresTypeSet::CreateCompositeType(PostgresTransaction &transaction, PostgresResult &result, idx_t start_row, idx_t end_row, unordered_map<idx_t, string> &name_map) {
	PostgresType postgres_type;
	CreateTypeInfo info;
	postgres_type.oid = result.GetInt64(start_row, 0);
	info.name = name_map[postgres_type.oid];

	child_list_t<LogicalType> child_types;
	for (idx_t row = start_row; row < end_row; row++) {
		auto type_name = result.GetString(row, 1);
		PostgresTypeData type_data;
		type_data.type_name = result.GetString(row, 2);
		PostgresType child_type;
		child_types.push_back(make_pair(type_name, PostgresUtils::TypeToLogicalType(&transaction, &schema, type_data, child_type)));
		postgres_type.children.push_back(std::move(child_type));
	}
	info.type = LogicalType::STRUCT(std::move(child_types));
	info.type.SetAlias(info.name);
	auto type_entry = make_uniq<PostgresTypeEntry>(catalog, schema, info, postgres_type);
	CreateEntry(std::move(type_entry));
}

void PostgresTypeSet::LoadCompositeTypes(PostgresTransaction &transaction, vector<PGTypeInfo> &composite_info) {
	if (composite_info.empty()) {
		return;
	}
	auto &conn = transaction.GetConnection();
	// compose the query
	// we create a single big query that uses a big IN list to get the values of all composite types at the same time
	unordered_map<idx_t, string> name_map;
	string in_list;
	for(auto &info : composite_info) {
		if (!in_list.empty()) {
			in_list += ",";
		}
		name_map[info.typrelid] = info.name;
		in_list += to_string(info.typrelid);
	}
	string query = StringUtil::Format(R"(
SELECT attrelid, attname, typname
FROM pg_attribute JOIN pg_type ON (pg_attribute.atttypid=pg_type.oid)
WHERE attrelid IN (%s)
ORDER BY attrelid, attnum;
)", in_list);
	auto result = conn.Query(query);
	auto count = result->Count();
	idx_t start = 0;
	idx_t current_oid = idx_t(-1);
	for (idx_t row = 0; row < count; row++) {
		auto oid = result->GetInt64(row, 0);
		if (oid != current_oid) {
			if (row > start) {
				CreateCompositeType(transaction, *result, start, row, name_map);
			}
			start = row;
			current_oid = oid;
		}
	}
	if (count > start) {
		CreateCompositeType(transaction, *result, start, count, name_map);
	}
}

void PostgresTypeSet::LoadEntries(ClientContext &context) {
	auto query = StringUtil::Replace(R"(
SELECT t.oid AS oid, t.typrelid AS id, t.typname as type, t.typtype as typeid
FROM pg_type t
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
WHERE (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid))
AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid)
AND n.nspname=${SCHEMA_NAME};
)", "${SCHEMA_NAME}", KeywordHelper::WriteQuoted(schema.name));

	auto &transaction = PostgresTransaction::Get(context, catalog);
	auto &conn = transaction.GetConnection();
	auto result = conn.Query(query);
	auto rows = result->Count();
	vector<PGTypeInfo> enum_types;
	vector<PGTypeInfo> composite_types;
	for(idx_t row = 0; row < rows; row++) {
		auto type = result->GetString(row, 3);
		if (type != "e" && type != "c") {
			// unsupported type;
			continue;
		}
		PGTypeInfo info;
		info.oid = result->GetInt64(row, 0);
		info.typrelid = result->GetInt64(row, 1);
		info.name = result->GetString(row, 2);
		if (type == "e") {
			// enum
			enum_types.push_back(std::move(info));
		} else if (type == "c") {
			// composite
			composite_types.push_back(std::move(info));
		}
	}
	LoadEnumTypes(transaction, enum_types);
	LoadCompositeTypes(transaction, composite_types);
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

optional_ptr<CatalogEntry> PostgresTypeSet::CreateType(ClientContext &context, CreateTypeInfo &info) {
	auto &transaction = PostgresTransaction::Get(context, catalog);
	auto &conn = transaction.GetConnection();

	auto create_sql = GetCreateTypeSQL(info);
	conn.Execute(create_sql);
	info.type.SetAlias(info.name);
	auto pg_type = PostgresUtils::CreateEmptyPostgresType(info.type);
	auto type_entry = make_uniq<PostgresTypeEntry>(catalog, schema, info, pg_type);
	return CreateEntry(std::move(type_entry));
}


}
