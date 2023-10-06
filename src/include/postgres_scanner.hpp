//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

class PostgresAttachFunction : public TableFunction {
public:
	PostgresAttachFunction();
};

class PostgresScanFunction : public TableFunction {
public:
	PostgresScanFunction();
};

class PostgresScanFunctionFilterPushdown : public TableFunction {
public:
	PostgresScanFunctionFilterPushdown();
};


} // namespace duckdb
