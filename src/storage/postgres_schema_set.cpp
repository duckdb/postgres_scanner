#include "storage/postgres_schema_set.hpp"
#include "storage/postgres_transaction.hpp"

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

}
