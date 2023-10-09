//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "storage/postgres_schema_set.hpp"
#include "postgres_connection.hpp"

namespace duckdb {
class PostgresCatalog;
class PostgresSchemaEntry;
class PostgresTableEntry;

class PostgresTransaction : public Transaction {
public:
	PostgresTransaction(PostgresCatalog &postgres_catalog, TransactionManager &manager, ClientContext &context);
	~PostgresTransaction() override;

	void Start();
	void Commit();
	void Rollback();

	PostgresConnection &GetConnection();
	PostgresSchemaSet &GetSchemas() {
		return schemas;
	}

	static PostgresTransaction &Get(ClientContext &context, Catalog &catalog);

private:
	PostgresConnection connection;
	PostgresSchemaSet schemas;
};

} // namespace duckdb
