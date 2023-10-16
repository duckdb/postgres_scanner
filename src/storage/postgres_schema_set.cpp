#include "storage/postgres_schema_set.hpp"
#include "storage/postgres_transaction.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"

namespace duckdb {

PostgresSchemaSet::PostgresSchemaSet(Catalog &catalog) :
    PostgresCatalogSet(catalog) {}

void PostgresSchemaSet::LoadEntries(ClientContext &context) {
	auto query = R"(
SELECT schema_name
FROM information_schema.schemata;
)";

	auto &transaction = PostgresTransaction::Get(context, catalog);
	auto result = transaction.Query(query);
	auto rows = result->Count();

	for(idx_t row = 0; row < rows; row++) {
		auto schema_name = result->GetString(row, 0);
		auto schema = make_uniq<PostgresSchemaEntry>(catalog, schema_name);
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
