#include "storage/postgres_schema_set.hpp"
#include "storage/postgres_transaction.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"

namespace duckdb {

PostgresSchemaSet::PostgresSchemaSet(Catalog &catalog, PostgresTransaction &transaction) :
    PostgresCatalogSet(catalog, transaction) {}

void PostgresSchemaSet::LoadEntries() {
	auto query = R"(
SELECT schema_name
FROM information_schema.schemata;
)";

	auto &conn = transaction.GetConnection();
	auto result = conn.Query(query);
	auto rows = result->Count();

	for(idx_t row = 0; row < rows; row++) {
		auto schema_name = result->GetString(row, 0);
		auto schema = make_uniq<PostgresSchemaEntry>(transaction, catalog, schema_name);
		entries.insert(make_pair(schema_name, std::move(schema)));
	}
}

optional_ptr<CatalogEntry> PostgresSchemaSet::CreateSchema(CreateSchemaInfo &info) {
	auto &conn = transaction.GetConnection();

	string create_sql = "CREATE SCHEMA " + KeywordHelper::WriteQuoted(info.schema, '"');
	conn.Execute(create_sql);
	auto schema_entry = make_uniq<PostgresSchemaEntry>(transaction, catalog, info.schema);
	return CreateEntry(std::move(schema_entry));
}

}
