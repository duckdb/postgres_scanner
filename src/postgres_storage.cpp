#include "duckdb.hpp"

#include "postgres_storage.hpp"
#include "storage/postgres_catalog.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "storage/postgres_transaction_manager.hpp"

namespace duckdb {

static unique_ptr<Catalog> PostgresAttach(StorageExtensionInfo *storage_info, AttachedDatabase &db, const string &name,
                                          AttachInfo &info, AccessMode access_mode) {
	return make_uniq<PostgresCatalog>(db, info.path, access_mode);
}

static unique_ptr<TransactionManager> PostgresCreateTransactionManager(StorageExtensionInfo *storage_info,
                                                                       AttachedDatabase &db, Catalog &catalog) {
	auto &postgres_catalog = catalog.Cast<PostgresCatalog>();
	return make_uniq<PostgresTransactionManager>(db, postgres_catalog);
}

PostgresStorageExtension::PostgresStorageExtension() {
	attach = PostgresAttach;
	create_transaction_manager = PostgresCreateTransactionManager;
}

} // namespace duckdb
