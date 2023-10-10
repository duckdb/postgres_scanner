//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_query.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/index_vector.hpp"

namespace duckdb {

class PostgresQuery : public PhysicalOperator {
public:
	PostgresQuery(LogicalOperator &op, TableCatalogEntry &table, string query);

	//! The table this query affects
	TableCatalogEntry &table;
	//! The query to execute
	string query;

public:
	// Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

	string GetName() const override;
	string ParamsToString() const override;
};

} // namespace duckdb
