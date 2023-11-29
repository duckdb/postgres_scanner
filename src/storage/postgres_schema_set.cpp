#include "storage/postgres_schema_set.hpp"
#include "storage/postgres_index_set.hpp"
#include "storage/postgres_table_set.hpp"
#include "storage/postgres_transaction.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "storage/postgres_table_set.hpp"

namespace duckdb {

PostgresSchemaSet::PostgresSchemaSet(Catalog &catalog) :
    PostgresCatalogSet(catalog) {}

vector<PostgresResultSlice> SliceResult(PostgresResult &schemas, PostgresResult &to_slice) {
	vector<PostgresResultSlice> result;
	idx_t current_offset = 0;
	for(idx_t schema_idx = 0; schema_idx < schemas.Count(); schema_idx++) {
		auto oid = schemas.GetInt64(schema_idx, 0);
		idx_t start = current_offset;
		for(; current_offset < to_slice.Count(); current_offset++) {
			auto current_oid = to_slice.GetInt64(current_offset, 0);
			if (current_oid != oid) {
				break;
			}
		}
		result.emplace_back(to_slice, start, current_offset);
	}
	return result;
}

string PostgresSchemaSet::GetInitializeQuery() {
	return R"(
SELECT oid, nspname
FROM pg_namespace
ORDER BY oid;
)";
}

void PostgresSchemaSet::LoadEntries(ClientContext &context) {
	string schema_query = PostgresSchemaSet::GetInitializeQuery();
	string tables_query = PostgresTableSet::GetInitializeQuery();
	string enum_types_query = R"(
SELECT n.oid, typname, enumtypid, enumsortorder, enumlabel
FROM pg_enum e
JOIN pg_type t ON e.enumtypid = t.oid
JOIN pg_namespace AS n ON (typnamespace=n.oid)
ORDER BY n.oid, enumtypid, enumsortorder;
)";
	string composite_types_query = R"(
SELECT n.oid, t.oid AS oid, t.typrelid AS id, t.typname as type, t.typtype as typeid, pg_attribute.attname, sub_type.typname
FROM pg_type t
JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
JOIN pg_class ON pg_class.oid = t.typrelid
JOIN pg_attribute ON attrelid=t.typrelid
JOIN pg_type sub_type ON (pg_attribute.atttypid=sub_type.oid)
WHERE pg_class.relkind = 'c'
AND t.typtype='c'
ORDER BY n.oid, t.oid, attrelid, attnum;
)";
	string index_query = PostgresIndexSet::GetInitializeQuery();

	auto full_query = schema_query + tables_query + enum_types_query + composite_types_query + index_query;

	auto &transaction = PostgresTransaction::Get(context, catalog);
	auto results = transaction.ExecuteQueries(full_query);
	auto result = std::move(results[0]);
	results.erase(results.begin());
	auto rows = result->Count();

	auto tables = SliceResult(*result, *results[0]);
	auto enums = SliceResult(*result, *results[1]);
	auto composite_types = SliceResult(*result, *results[2]);
	auto indexes = SliceResult(*result, *results[3]);
	for(idx_t row = 0; row < rows; row++) {
		auto oid = result->GetInt64(row, 0);
		auto schema_name = result->GetString(row, 1);
		auto schema = make_uniq<PostgresSchemaEntry>(transaction, catalog, schema_name, tables[row], enums[row], composite_types[row], indexes[row]);
		CreateEntry(std::move(schema));
	}
}

optional_ptr<CatalogEntry> PostgresSchemaSet::CreateSchema(ClientContext &context, CreateSchemaInfo &info) {
	auto &transaction = PostgresTransaction::Get(context, catalog);

	string create_sql = "CREATE SCHEMA " + KeywordHelper::WriteQuoted(info.schema, '"');
	transaction.Query(create_sql);
	auto schema_entry = make_uniq<PostgresSchemaEntry>(catalog, info.schema);
	return CreateEntry(std::move(schema_entry));
}

}
