#include "storage/postgres_query.hpp"
#include "storage/postgres_table_entry.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "storage/postgres_catalog.hpp"
#include "storage/postgres_transaction.hpp"
#include "postgres_connection.hpp"

namespace duckdb {

PostgresQuery::PostgresQuery(LogicalOperator &op, TableCatalogEntry &table, string query_p)
    : PhysicalOperator(PhysicalOperatorType::EXTENSION, op.types, 1), table(table), query(std::move(query_p)) {
	// FIXME - mega hacky
	query = StringUtil::Replace(query, table.catalog.GetName() + ".", "");
	query = StringUtil::Replace(query, KeywordHelper::WriteQuoted(table.catalog.GetName(), '"') + ".", "");
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType PostgresQuery::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
	auto &transaction = PostgresTransaction::Get(context.client, table.catalog);
	auto &con = transaction.GetConnection();
	auto result = con.Query(query);

	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(result->AffectedRows()));

	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string PostgresQuery::GetName() const {
	return "QUERY";
}

string PostgresQuery::ParamsToString() const {
	return query;
}

//===--------------------------------------------------------------------===//
// Plan
//===--------------------------------------------------------------------===//
unique_ptr<PhysicalOperator> PostgresCatalog::PlanUpdate(ClientContext &context, LogicalUpdate &op,
                                                       unique_ptr<PhysicalOperator> plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for update of a Postgres table");
	}
	auto current_query = context.GetCurrentQuery();
	if (current_query.empty()) {
		throw InvalidInputException("UPDATE in Postgres connector is only supported through a SQL query");
	}
	auto update = make_uniq<PostgresQuery>(op, op.table, current_query);
	return std::move(update);
}

unique_ptr<PhysicalOperator> PostgresCatalog::PlanDelete(ClientContext &context, LogicalDelete &op,
                                                       unique_ptr<PhysicalOperator> plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for deletion of a Postgres table");
	}
	auto current_query = context.GetCurrentQuery();
	if (current_query.empty()) {
		throw InvalidInputException("DELETE in Postgres connector is only supported through a SQL query");
	}
	auto del = make_uniq<PostgresQuery>(op, op.table, current_query);
	return std::move(del);
}

} // namespace duckdb
