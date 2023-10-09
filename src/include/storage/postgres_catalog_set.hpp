//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_catalog_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {
class PostgresTransaction;

class PostgresCatalogSet {
public:
	PostgresCatalogSet(Catalog &catalog, PostgresTransaction &transaction);

	optional_ptr<CatalogEntry> GetEntry(const string &name);
	void DropEntry(const string &name, bool cascade);
	void Scan(const std::function<void(CatalogEntry &)> &callback);
	case_insensitive_map_t<unique_ptr<CatalogEntry>> &GetEntries() {
		return entries;
	}

protected:
	virtual void LoadEntries() = 0;
	virtual string EntryName() = 0;

protected:
	Catalog &catalog;
	PostgresTransaction &transaction;
	case_insensitive_map_t<unique_ptr<CatalogEntry>> entries;
	bool is_loaded;
};

} // namespace duckdb
