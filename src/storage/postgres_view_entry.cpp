#include "storage/postgres_catalog.hpp"
#include "storage/postgres_view_entry.hpp"
#include "storage/postgres_transaction.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "postgres_scanner.hpp"

namespace duckdb {

PostgresViewEntry::PostgresViewEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateViewInfo &info)
    : ViewCatalogEntry(catalog, schema, info) {
	idx_t column_count = types.size();
	for (idx_t c = 0; c < column_count; c++) {
		auto &type = types[c];
		auto &name = names[c];
		if (type.HasAlias()) {
			type = PostgresUtils::RemoveAlias(type);
		}
		postgres_types.push_back(PostgresUtils::CreateEmptyPostgresType(type));
		postgres_names.push_back(name);
	}
	approx_num_pages = 0;
}

PostgresViewEntry::PostgresViewEntry(Catalog &catalog, SchemaCatalogEntry &schema, PostgresViewInfo &info)
    : ViewCatalogEntry(catalog, schema, *info.create_info), postgres_types(std::move(info.postgres_types)),
      postgres_names(std::move(info.postgres_names)) {
	D_ASSERT(postgres_types.size() == types.size());
	approx_num_pages = info.approx_num_pages;
}

PostgresViewEntry::~PostgresViewEntry() {
}

} // namespace duckdb
