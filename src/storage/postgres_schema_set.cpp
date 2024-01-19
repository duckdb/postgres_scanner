#include "storage/postgres_schema_set.hpp"
#include "storage/postgres_index_set.hpp"
#include "storage/postgres_table_set.hpp"
#include "storage/postgres_type_set.hpp"
#include "storage/postgres_transaction.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "storage/postgres_table_set.hpp"

namespace duckdb {

PostgresSchemaSet::PostgresSchemaSet(Catalog &catalog) : PostgresCatalogSet(catalog, false) {
}

vector<unique_ptr<PostgresResultSlice>> SliceResult(PostgresResult &schemas, unique_ptr<PostgresResult> to_slice_ptr) {
	auto shared_result = shared_ptr<PostgresResult>(to_slice_ptr.release());
	auto &to_slice = *shared_result;

	vector<unique_ptr<PostgresResultSlice>> result;
	idx_t current_offset = 0;
	for (idx_t schema_idx = 0; schema_idx < schemas.Count(); schema_idx++) {
		auto oid = schemas.GetInt64(schema_idx, 0);
		idx_t start = current_offset;
		for (; current_offset < to_slice.Count(); current_offset++) {
			auto current_oid = to_slice.GetInt64(current_offset, 0);
			if (current_oid != oid) {
				break;
			}
		}
		result.push_back(make_uniq<PostgresResultSlice>(shared_result, start, current_offset));
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
	string enum_types_query = PostgresTypeSet::GetInitializeEnumsQuery();
	string composite_types_query = PostgresTypeSet::GetInitializeCompositesQuery();
	string index_query = PostgresIndexSet::GetInitializeQuery();

	auto full_query = schema_query + tables_query + enum_types_query + composite_types_query + index_query;

	auto &transaction = PostgresTransaction::Get(context, catalog);
	auto results = transaction.ExecuteQueries(full_query);
	auto result = std::move(results[0]);
	results.erase(results.begin());
	auto rows = result->Count();

	auto tables = SliceResult(*result, std::move(results[0]));
	auto enums = SliceResult(*result, std::move(results[1]));
	auto composite_types = SliceResult(*result, std::move(results[2]));
	auto indexes = SliceResult(*result, std::move(results[3]));
	for (idx_t row = 0; row < rows; row++) {
		auto oid = result->GetInt64(row, 0);
		auto schema_name = result->GetString(row, 1);
		auto schema =
		    make_uniq<PostgresSchemaEntry>(catalog, schema_name, std::move(tables[row]), std::move(enums[row]),
		                                   std::move(composite_types[row]), std::move(indexes[row]));
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

} // namespace duckdb
