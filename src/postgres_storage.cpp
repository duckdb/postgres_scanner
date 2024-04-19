#include "duckdb.hpp"

#include "postgres_storage.hpp"
#include "storage/postgres_catalog.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "storage/postgres_transaction_manager.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {

string EscapeConnectionString(const string &input) {
	string result = "'";
	for(auto c : input) {
		if (c == '\\') {
			result += "\\\\";
		} else if (c == '\'') {
			result += "\\'";
		} else {
			result += c;
		}
	}
	result +=  "'";
	return result;
}

string AddConnectionOption(const KeyValueSecret &kv_secret, const string &name) {
	Value input_val = kv_secret.TryGetValue(name);
	if (input_val.IsNull()) {
		// not provided
		return string();
	}
	string result;
	result += name;
	result += "=";
	result += EscapeConnectionString(input_val.ToString());
	result += " ";
	return result;
}

static unique_ptr<Catalog> PostgresAttach(StorageExtensionInfo *storage_info, ClientContext &context,
                                          AttachedDatabase &db, const string &name, AttachInfo &info,
                                          AccessMode access_mode) {
	string connection_string = info.path;
	for(auto &entry : info.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "type" || lower_name == "read_only") {
			// already handled
		} else if (lower_name == "secret") {
			auto &secret_manager = SecretManager::Get(context);
			auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
			auto secret_name = entry.second.ToString();
			auto secret_entry = secret_manager.GetSecretByName(transaction, secret_name);
			if (!secret_entry) {
				throw BinderException("Secret with name \"%s\" not found", secret_name);
			}
			// secret found - read
			const auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
			string new_connection_info;

			new_connection_info += AddConnectionOption(kv_secret, "user");
			new_connection_info += AddConnectionOption(kv_secret, "password");
			new_connection_info += AddConnectionOption(kv_secret, "host");
			new_connection_info += AddConnectionOption(kv_secret, "port");
			new_connection_info += AddConnectionOption(kv_secret, "dbname");

			connection_string = new_connection_info + connection_string;
		} else {
			throw BinderException("Unrecognized option for Postgres attach: %s", entry.first);
		}
	}
	return make_uniq<PostgresCatalog>(db, connection_string, access_mode);
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
