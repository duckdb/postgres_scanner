//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_update.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/index_vector.hpp"

namespace duckdb {

class PostgresUpdate : public PhysicalOperator {
public:
	PostgresUpdate(LogicalOperator &op, TableCatalogEntry &table, vector<PhysicalIndex> columns);

	//! The table to delete from
	TableCatalogEntry &table;
	//! The set of columns to update
	vector<PhysicalIndex> columns;

public:
	// Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return false;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace duckdb
