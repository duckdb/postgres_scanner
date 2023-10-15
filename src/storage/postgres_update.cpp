#include "storage/postgres_update.hpp"
#include "storage/postgres_table_entry.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "storage/postgres_catalog.hpp"
#include "storage/postgres_transaction.hpp"
#include "postgres_connection.hpp"
#include "postgres_stmt.hpp"
#include "duckdb/common/types/uuid.hpp"

namespace duckdb {

PostgresUpdate::PostgresUpdate(LogicalOperator &op, TableCatalogEntry &table, vector<PhysicalIndex> columns_p)
    : PhysicalOperator(PhysicalOperatorType::EXTENSION, op.types, 1), table(table), columns(std::move(columns_p)) {
}

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class PostgresUpdateGlobalState : public GlobalSinkState {
public:
	explicit PostgresUpdateGlobalState(PostgresTableEntry &table) : table(table), update_count(0) {
	}

	PostgresTableEntry &table;
	PostgresCopyState copy_state;
	DataChunk insert_chunk;
	DataChunk varchar_chunk;
	string update_sql;
	idx_t update_count;
};

string CreateUpdateTable(const string &name, PostgresTableEntry &table, const vector<PhysicalIndex> &index) {
	string result;
	result = "CREATE LOCAL TEMPORARY TABLE " + KeywordHelper::WriteOptionallyQuoted(name);
	result += "(";
	for (idx_t i = 0; i < index.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		auto &col = table.GetColumn(LogicalIndex(index[i].index));
		result += KeywordHelper::WriteQuoted(col.GetName(), '"');
		result += " ";
		result += PostgresUtils::TypeToString(col.GetType());
	}
	result += ", __page_id_string VARCHAR) ON COMMIT DROP;";
	return result;
}

string GetUpdateSQL(const string &name, PostgresTableEntry &table, const vector<PhysicalIndex> &index) {
	string result;
	result = "UPDATE " + KeywordHelper::WriteQuoted(table.name, '"');
	result += " SET ";
	for (idx_t i = 0; i < index.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		auto &col = table.GetColumn(LogicalIndex(index[i].index));
		result += KeywordHelper::WriteQuoted(col.GetName(), '"');
		result += " = ";
		result += KeywordHelper::WriteQuoted(name, '"');
		result += ".";
		result += KeywordHelper::WriteQuoted(col.GetName(), '"');
	}
	result += " FROM " + KeywordHelper::WriteOptionallyQuoted(name);
	result += " WHERE ";
	result += KeywordHelper::WriteQuoted(table.name, '"');
	result += ".ctid=__page_id_string::TID";
	return result;
}


unique_ptr<GlobalSinkState> PostgresUpdate::GetGlobalSinkState(ClientContext &context) const {
	auto &postgres_table = table.Cast<PostgresTableEntry>();

	auto &transaction = PostgresTransaction::Get(context, postgres_table.catalog);
	auto result = make_uniq<PostgresUpdateGlobalState>(postgres_table);
	auto &connection = transaction.GetConnection();
	// create a temporary table to stream the update data into
	auto table_name = "update_data_" + UUID::ToString(UUID::GenerateRandomUUID());
	connection.Execute(CreateUpdateTable(table_name, postgres_table, columns));
	// generate the final UPDATE sql
	result->update_sql = GetUpdateSQL(table_name, postgres_table, columns);
	// initialize the insertion chunk
	vector<LogicalType> insert_types;
	for (idx_t i = 0; i < columns.size(); i++) {
		auto &col = table.GetColumn(LogicalIndex(columns[i].index));
		insert_types.push_back(col.GetType());
	}
	insert_types.push_back(LogicalType::VARCHAR);
	result->insert_chunk.Initialize(context, insert_types);

	// begin the COPY TO
	vector<string> column_names;
	connection.BeginCopyTo(context, result->copy_state, postgres_table, column_names);
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType PostgresUpdate::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<PostgresUpdateGlobalState>();

	chunk.Flatten();
	// reference the data columns directly
	for(idx_t c = 0; c < columns.size(); c++) {
		gstate.insert_chunk.data[c].Reference(chunk.data[c]);
	}
	// convert our row ids back into ctids
	auto &row_identifiers = chunk.data[chunk.ColumnCount() - 1];
	auto &ctid_vector = gstate.insert_chunk.data[gstate.insert_chunk.ColumnCount() - 1];
	auto row_data = FlatVector::GetData<row_t>(row_identifiers);
	auto varchar_data = FlatVector::GetData<string_t>(ctid_vector);

	for (idx_t r = 0; r < chunk.size(); r++) {
		// extract the ctid from the row id
		auto row_in_page = row_data[r] & 0xFFFF;
		auto page_index = row_data[r] >> 16;

		string ctid_string;
		ctid_string += "'(";
		ctid_string += to_string(page_index);
		ctid_string += ",";
		ctid_string += to_string(row_in_page);
		ctid_string += ")'";
		varchar_data[r] = StringVector::AddString(ctid_vector, ctid_string);
	}
	gstate.insert_chunk.SetCardinality(chunk);

	auto &transaction = PostgresTransaction::Get(context.client, gstate.table.catalog);
	auto &connection = transaction.GetConnection();
	connection.CopyChunk(context.client, gstate.copy_state, gstate.insert_chunk, gstate.varchar_chunk);
	gstate.update_count += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType PostgresUpdate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
						  OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<PostgresUpdateGlobalState>();
	auto &transaction = PostgresTransaction::Get(context, gstate.table.catalog);
	auto &connection = transaction.GetConnection();
	// flush the copy to state
	connection.FinishCopyTo(gstate.copy_state);
	// merge the update_info table into the actual table (i.e. perform the actual update)
	connection.Execute(gstate.update_sql);
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType PostgresUpdate::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
	auto &insert_gstate = sink_state->Cast<PostgresUpdateGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(insert_gstate.update_count));

	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string PostgresUpdate::GetName() const {
	return "UPDATE";
}

string PostgresUpdate::ParamsToString() const {
	return table.name;
}

//===--------------------------------------------------------------------===//
// Plan
//===--------------------------------------------------------------------===//
unique_ptr<PhysicalOperator> PostgresCatalog::PlanUpdate(ClientContext &context, LogicalUpdate &op,
                                                       unique_ptr<PhysicalOperator> plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for updates of a Postgres table");
	}
	for (auto &expr : op.expressions) {
		if (expr->type == ExpressionType::VALUE_DEFAULT) {
			throw BinderException("SET DEFAULT is not yet supported for updates of a Postgres table");
		}
	}
	PostgresCatalog::MaterializePostgresScans(*plan);
	auto insert = make_uniq<PostgresUpdate>(op, op.table, std::move(op.columns));
	insert->children.push_back(std::move(plan));
	return std::move(insert);
}

} // namespace duckdb
