#include "storage/postgres_catalog_set.hpp"
#include "storage/postgres_transaction.hpp"

namespace duckdb {

PostgresCatalogSet::PostgresCatalogSet(Catalog &catalog, PostgresTransaction &transaction) :
    catalog(catalog), transaction(transaction), is_loaded(false) {}

optional_ptr<CatalogEntry> PostgresCatalogSet::GetEntry(const string &name) {
	if (!is_loaded) {
		LoadEntries();
		is_loaded = true;
	}
	auto entry = entries.find(name);
	if (entry == entries.end()) {
		return nullptr;
	}
	return entry->second.get();
}

void PostgresCatalogSet::DropEntry(const string &name, bool cascade) {
	entries.erase(name);
	string drop_query = "DROP ";
	drop_query += EntryName();
	drop_query += KeywordHelper::WriteQuoted(name, '"');
	if (cascade) {
		drop_query += "CASCADE";
	}
	auto &conn = transaction.GetConnection();
	conn.Execute(drop_query);
}

void PostgresCatalogSet::Scan(const std::function<void(CatalogEntry &)> &callback) {
	for(auto &entry : entries) {
		callback(*entry.second);
	}
}



}
