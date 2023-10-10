#include "storage/postgres_catalog_set.hpp"
#include "storage/postgres_transaction.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
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

void PostgresCatalogSet::DropEntry(DropInfo &info) {
	entries.erase(info.name);
	string drop_query = "DROP ";
	drop_query += CatalogTypeToString(info.type) + " ";
	if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
		drop_query += " IF EXISTS ";
	}
	drop_query += KeywordHelper::WriteQuoted(info.name, '"');
	if (info.cascade) {
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

optional_ptr<CatalogEntry> PostgresCatalogSet::CreateEntry(unique_ptr<CatalogEntry> entry) {
	auto result = entry.get();
	entries.insert(make_pair(result->name, std::move(entry)));
	return result;
}



}
