#include "storage/postgres_catalog.hpp"
#include "storage/postgres_index.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

PostgresCreateIndex::PostgresCreateIndex(unique_ptr<CreateIndexInfo> info, TableCatalogEntry &table)
    : PhysicalOperator(PhysicalOperatorType::EXTENSION, {LogicalType::BIGINT}, 1), info(std::move(info)), table(table) {
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PostgresCreateIndex::GetData(ExecutionContext &context, DataChunk &chunk,
                                              OperatorSourceInput &input) const {
	auto &catalog = table.catalog;
	if (info->catalog == INVALID_CATALOG && info->schema == catalog.GetName()) {
		info->schema = DEFAULT_SCHEMA;
	}
	auto &schema = catalog.GetSchema(context.client, info->schema);
	schema.CreateIndex(context.client, *info, table);

	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Logical Operator
//===--------------------------------------------------------------------===//
class LogicalPostgresCreateIndex : public LogicalExtensionOperator {
public:
	LogicalPostgresCreateIndex(unique_ptr<CreateIndexInfo> info_p, TableCatalogEntry &table)
	    : info(std::move(info_p)), table(table) {
	}

	unique_ptr<CreateIndexInfo> info;
	TableCatalogEntry &table;

	unique_ptr<PhysicalOperator> CreatePlan(ClientContext &context, PhysicalPlanGenerator &generator) override {
		return make_uniq<PostgresCreateIndex>(std::move(info), table);
	}

	void Serialize(Serializer &serializer) const override {
		throw NotImplementedException("Cannot serialize Postgres Create index");
	}

	void ResolveTypes() override {
		types = {LogicalType::BIGINT};
	}
};

unique_ptr<LogicalOperator> PostgresCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                             TableCatalogEntry &table,
                                                             unique_ptr<LogicalOperator> plan) {
	return make_uniq<LogicalPostgresCreateIndex>(unique_ptr_cast<CreateInfo, CreateIndexInfo>(std::move(stmt.info)),
	                                             table);
}

} // namespace duckdb
