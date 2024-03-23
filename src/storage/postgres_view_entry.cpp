#include "storage/postgres_catalog.hpp"
#include "storage/postgres_view_entry.hpp"
#include "storage/postgres_transaction.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
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

static CreateViewInfo &ReplaceUnknownNames(CreateViewInfo &view_info) {
	// When expressions like `select 'x'` are passed to postgres, the "?column?" name is given to them
	// this clashes with DuckDB's behavior, as we assign the ToString of the expression to them instead.
	// so we have to replace the "?column?" names with their DuckDB version
	auto &select_node = view_info.query->node->Cast<SelectNode>();
	auto &select_list = select_node.select_list;
	// Aliases can override names, if provided
	D_ASSERT(view_info.aliases.size() <= view_info.names.size());
	// Every name corresponds to an expression
	D_ASSERT(view_info.names.size() == select_list.size());
	for (idx_t i = 0; i < view_info.names.size(); i++) {
		if (i < view_info.aliases.size()) {
			// No need to do anything, alias will be used instead of the expression name
			continue;
		}
		if (view_info.names[i] != "?column?") {
			// This expression was given an alias already
			continue;
		}
		auto &expression = select_list[i];
		// If it had an alias the postgres name wouldn't be "?column?"
		D_ASSERT(expression->alias.empty());
		view_info.names[i] = expression->ToString();
	}
	return view_info;
}

PostgresViewEntry::PostgresViewEntry(Catalog &catalog, SchemaCatalogEntry &schema, PostgresViewInfo &info)
    : ViewCatalogEntry(catalog, schema, ReplaceUnknownNames(*info.create_info)),
      postgres_types(std::move(info.postgres_types)), postgres_names(std::move(info.postgres_names)) {
	D_ASSERT(postgres_types.size() == types.size());
	approx_num_pages = info.approx_num_pages;
}

PostgresViewEntry::~PostgresViewEntry() {
}

} // namespace duckdb
