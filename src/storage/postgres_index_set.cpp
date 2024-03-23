#include "storage/postgres_index_set.hpp"
#include "storage/postgres_schema_entry.hpp"
#include "storage/postgres_transaction.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "storage/postgres_index_entry.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"

namespace duckdb {

PostgresIndexSet::PostgresIndexSet(PostgresSchemaEntry &schema, unique_ptr<PostgresResultSlice> index_result_p)
    : PostgresInSchemaSet(schema, !index_result_p), index_result(std::move(index_result_p)) {
}

string PostgresIndexSet::GetInitializeQuery() {
	return R"(
SELECT pg_namespace.oid, tablename, indexname
FROM pg_indexes
JOIN pg_namespace ON (schemaname=nspname)
ORDER BY pg_namespace.oid;
)";
}

void PostgresIndexSet::LoadEntries(ClientContext &context) {
	if (!index_result) {
		throw InternalException("PostgresIndexSet::LoadEntries called without an index result defined");
	}
	auto &result = index_result->GetResult();
	for (idx_t row = index_result->start; row < index_result->end; row++) {
		auto table_name = result.GetString(row, 1);
		D_ASSERT(!table_name.empty());
		auto index_name = result.GetString(row, 2);
		CreateIndexInfo info;
		info.schema = schema.name;
		info.table = table_name;
		info.index_name = index_name;
		auto index_entry = make_uniq<PostgresIndexEntry>(catalog, schema, info, table_name);
		CreateEntry(std::move(index_entry));
	}
	index_result.reset();
}

void PGUnqualifyColumnReferences(ParsedExpression &expr) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &colref = expr.Cast<ColumnRefExpression>();
		auto name = std::move(colref.column_names.back());
		colref.column_names = {std::move(name)};
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(expr, PGUnqualifyColumnReferences);
}

string PGGetCreateIndexSQL(CreateIndexInfo &info, TableCatalogEntry &tbl) {
	string sql;
	sql = "CREATE";
	if (info.constraint_type == IndexConstraintType::UNIQUE) {
		sql += " UNIQUE";
	}
	sql += " INDEX ";
	sql += KeywordHelper::WriteOptionallyQuoted(info.index_name);
	sql += " ON ";
	sql += KeywordHelper::WriteOptionallyQuoted(tbl.schema.name) + ".";
	sql += KeywordHelper::WriteOptionallyQuoted(tbl.name);
	sql += "(";
	for (idx_t i = 0; i < info.parsed_expressions.size(); i++) {
		if (i > 0) {
			sql += ", ";
		}
		PGUnqualifyColumnReferences(*info.parsed_expressions[i]);
		sql += info.parsed_expressions[i]->ToString();
	}
	sql += ")";
	return sql;
}

optional_ptr<CatalogEntry> PostgresIndexSet::CreateIndex(ClientContext &context, CreateIndexInfo &info,
                                                         TableCatalogEntry &table) {
	auto &postgres_transaction = PostgresTransaction::Get(context, table.catalog);
	postgres_transaction.Query(PGGetCreateIndexSQL(info, table));
	auto index_entry = make_uniq<PostgresIndexEntry>(schema.ParentCatalog(), schema, info, table.name);
	return CreateEntry(std::move(index_entry));
}

} // namespace duckdb
