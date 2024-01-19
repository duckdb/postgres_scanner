//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/config.hpp"

namespace duckdb {

class PostgresOptimizer {
public:
	static void Optimize(ClientContext &context, OptimizerExtensionInfo *info, unique_ptr<LogicalOperator> &plan);
};

} // namespace duckdb
