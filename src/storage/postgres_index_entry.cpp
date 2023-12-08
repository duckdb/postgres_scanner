#include "storage/postgres_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

namespace duckdb {

PostgresIndexEntry::PostgresIndexEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateIndexInfo &info,
                                       string table_name_p)
    : IndexCatalogEntry(catalog, schema, info), table_name(std::move(table_name_p)) {
}

string PostgresIndexEntry::GetSchemaName() const {
	return schema.name;
}

string PostgresIndexEntry::GetTableName() const {
	return table_name;
}

} // namespace duckdb
