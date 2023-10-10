#include "storage/postgres_index_set.hpp"
#include "storage/postgres_schema_entry.hpp"
#include "storage/postgres_transaction.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "storage/postgres_index_entry.hpp"

namespace duckdb {

PostgresIndexSet::PostgresIndexSet(PostgresSchemaEntry &schema, PostgresTransaction &transaction) :
    PostgresCatalogSet(schema.ParentCatalog(), transaction), schema(schema) {}

void PostgresIndexSet::LoadEntries() {
	auto query = StringUtil::Replace(R"(
SELECT tablename, indexname
FROM pg_indexes
WHERE schemaname=${SCHEMA_NAME}
)", "${SCHEMA_NAME}", KeywordHelper::WriteQuoted(schema.name));

	auto &conn = transaction.GetConnection();
	auto result = conn.Query(query);
	auto rows = result->Count();

	for(idx_t row = 0; row < rows; row++) {
		auto table_name = result->GetString(row, 0);
		auto index_name = result->GetString(row, 1);
		CreateIndexInfo info;
		info.schema = schema.name;
		info.table = table_name;
		info.index_name = index_name;
		auto index_entry = make_uniq<PostgresIndexEntry>(catalog, schema, info, table_name);
		entries.insert(make_pair(index_name, std::move(index_entry)));
	}
}

}
