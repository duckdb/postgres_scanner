#include "postgres_connection.hpp"
#include "postgres_binary_writer.hpp"

namespace duckdb {

void PostgresConnection::BeginCopyTo(const string &schema_name, const string &table_name, const vector<string> &column_names) {
	string query = "COPY ";
	if (!schema_name.empty()) {
		query += KeywordHelper::WriteQuoted(schema_name, '"') + ".";
	}
	query += KeywordHelper::WriteQuoted(table_name, '"') + " ";
	if (!column_names.empty()) {
		query += "(";
		for(idx_t c = 0; c < column_names.size(); c++) {
			if (c > 0) {
				query += ", ";
			}
			query += KeywordHelper::WriteOptionallyQuoted(column_names[c]);
		}
		query += ") ";
	}
	query += "FROM STDIN (FORMAT binary)";
	auto result = PQexec(GetConn(), query.c_str());
	if (!result || PQresultStatus(result) != PGRES_COPY_IN) {
		throw std::runtime_error("Failed to prepare COPY \"" + query + "\": " + string(PQresultErrorMessage(result)));
	}
	PostgresBinaryWriter writer;
	writer.WriteHeader();
	CopyData(writer);
}

void PostgresConnection::CopyData(data_ptr_t buffer, idx_t size) {
	int result;
	do {
		result = PQputCopyData(GetConn(), (const char *) buffer, size);
	} while(result == 0);
	if (result == -1) {
		throw InternalException("Error during PQputCopyData: %s", PQerrorMessage(GetConn()));
	}
}

void PostgresConnection::CopyData(PostgresBinaryWriter &writer) {
	CopyData(writer.stream.GetData(), writer.stream.GetPosition());
}

void PostgresConnection::FinishCopyTo() {
	PostgresBinaryWriter writer;
	writer.WriteFooter();
	CopyData(writer);

	auto result_code = PQputCopyEnd(GetConn(), nullptr);
	if (result_code != 1) {
		throw InternalException("Error during PQputCopyEnd: %s", PQerrorMessage(GetConn()));
	}
	// fetch the query result to check for errors
	auto result = PQgetResult(GetConn());
	if (!result || PQresultStatus(result) != PGRES_COMMAND_OK) {
		throw std::runtime_error("Failed to copy data: " + string(PQresultErrorMessage(result)));
	}
}

void PostgresConnection::CopyChunk(DataChunk &chunk) {
	chunk.Flatten();

	PostgresBinaryWriter writer;
	for (idx_t r = 0; r < chunk.size(); r++) {
		writer.BeginRow(chunk.ColumnCount());
		for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
			auto &col = chunk.data[c];
			writer.WriteValue(col, r);
		}
		writer.FinishRow();
	}
	CopyData(writer);
}


}
