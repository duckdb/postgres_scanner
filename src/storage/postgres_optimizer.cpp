#include "storage/postgres_index_set.hpp"
#include "storage/postgres_schema_entry.hpp"
#include "storage/postgres_transaction.hpp"
#include "storage/postgres_optimizer.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "storage/postgres_catalog.hpp"
#include "postgres_scanner.hpp"

namespace duckdb {

struct PostgresOperators {
	reference_map_t<PostgresCatalog, vector<reference<LogicalGet>>> scans;
};

void GatherPostgresScans(LogicalOperator &op, PostgresOperators &result) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		auto &table_scan = get.function;
		if (!PostgresCatalog::IsPostgresScan(table_scan.name)) {
			// not a postgres scan - skip
			return;
		}
		auto &bind_data = get.bind_data->Cast<PostgresBindData>();
		auto catalog = bind_data.GetCatalog();
		if (!catalog) {
			// "postgres_scan" functions are fully independent - we can always stream them
			return;
		}
		result.scans[*catalog].push_back(get);
	}
	// recurse into children
	for (auto &child : op.children) {
		GatherPostgresScans(*child, result);
	}
}

void PostgresOptimizer::Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	// look at the query plan and check if we can enable streaming query scans
	PostgresOperators operators;
	GatherPostgresScans(*plan, operators);
	if (operators.scans.empty()) {
		// no scans
		return;
	}
	for (auto &entry : operators.scans) {
		auto &catalog = entry.first;
		auto multiple_scans = entry.second.size() > 1;
		for (auto &scan : entry.second) {
			auto &bind_data = scan.get().bind_data->Cast<PostgresBindData>();
			// if there is a single scan in the plan we can always stream using the main thread
			// if there is more than one scan we either (1) need to materialize, or (2) cannot use the main thread
			if (multiple_scans) {
				if (bind_data.max_threads > 1 && bind_data.read_only) {
					bind_data.requires_materialization = false;
					bind_data.can_use_main_thread = false;
				} else {
					bind_data.requires_materialization = true;
					bind_data.can_use_main_thread = true;
				}
			} else {
				bind_data.requires_materialization = false;
				bind_data.can_use_main_thread = true;
			}
		}
	}
}

} // namespace duckdb
