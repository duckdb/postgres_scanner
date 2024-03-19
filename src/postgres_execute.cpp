#include "duckdb.hpp"

#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "postgres_scanner.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "storage/postgres_catalog.hpp"
#include "storage/postgres_transaction.hpp"

namespace duckdb {

struct PGExecuteBindData : public TableFunctionData {
	explicit PGExecuteBindData(PostgresCatalog &pg_catalog, string query_p)
	    : pg_catalog(pg_catalog), query(std::move(query_p)) {
	}

	bool finished = false;
	PostgresCatalog &pg_catalog;
	string query;
};

static duckdb::unique_ptr<FunctionData> PGExecuteBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");

	// look up the database to query
	auto db_name = input.inputs[0].GetValue<string>();
	auto &db_manager = DatabaseManager::Get(context);
	auto db = db_manager.GetDatabase(context, db_name);
	if (!db) {
		throw BinderException("Failed to find attached database \"%s\" referenced in postgres_query", db_name);
	}
	auto &catalog = db->GetCatalog();
	if (catalog.GetCatalogType() != "postgres") {
		throw BinderException("Attached database \"%s\" does not refer to a Postgres database", db_name);
	}
	auto &pg_catalog = catalog.Cast<PostgresCatalog>();
	return make_uniq<PGExecuteBindData>(pg_catalog, input.inputs[1].GetValue<string>());
}

static void PGExecuteFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<PGExecuteBindData>();
	if (data.finished) {
		return;
	}
	auto &transaction = Transaction::Get(context, data.pg_catalog).Cast<PostgresTransaction>();
	transaction.ExecuteQueries(data.query);
	data.finished = true;
}

PostgresExecuteFunction::PostgresExecuteFunction()
    : TableFunction("postgres_execute", {LogicalType::VARCHAR, LogicalType::VARCHAR}, PGExecuteFunction,
                    PGExecuteBind) {
}

} // namespace duckdb
