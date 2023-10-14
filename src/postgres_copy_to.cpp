#include "postgres_connection.hpp"
#include "postgres_binary_writer.hpp"
#include "postgres_text_writer.hpp"

namespace duckdb {

void PostgresConnection::BeginCopyTo(PostgresCopyFormat format, const string &schema_name, const string &table_name, const vector<string> &column_names) {
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
	query += "FROM STDIN (FORMAT ";
	switch(format) {
	case PostgresCopyFormat::BINARY:
		query += "BINARY";
		break;
	case PostgresCopyFormat::TEXT:
		query += "TEXT, NULL '\b'";
		break;
	default:
		throw InternalException("Unsupported type for postgres copy format");
	}
	query += ")";
	auto result = PQexec(GetConn(), query.c_str());
	if (!result || PQresultStatus(result) != PGRES_COPY_IN) {
		throw std::runtime_error("Failed to prepare COPY \"" + query + "\": " + string(PQresultErrorMessage(result)));
	}
	if (format == PostgresCopyFormat::BINARY) {
		// binary copy requires a header
		PostgresBinaryWriter writer;
		writer.WriteHeader();
		CopyData(writer);
	}
}

void PostgresConnection::CopyData(data_ptr_t buffer, idx_t size) {
	int result;
	do {
		result = PQputCopyData(GetConn(), (const char *) buffer, int(size));
	} while(result == 0);
	if (result == -1) {
		throw InternalException("Error during PQputCopyData: %s", PQerrorMessage(GetConn()));
	}
}

void PostgresConnection::CopyData(PostgresBinaryWriter &writer) {
	CopyData(writer.stream.GetData(), writer.stream.GetPosition());
}

void PostgresConnection::CopyData(PostgresTextWriter &writer) {
	CopyData(writer.stream.GetData(), writer.stream.GetPosition());
}

void PostgresConnection::FinishCopyTo(PostgresCopyFormat format) {
	if (format == PostgresCopyFormat::BINARY) {
		// binary copy requires a footer
		PostgresBinaryWriter writer;
		writer.WriteFooter();
		CopyData(writer);
	} else if (format == PostgresCopyFormat::TEXT) {
		// text copy requires a footer
		PostgresTextWriter writer;
		writer.WriteFooter();
		CopyData(writer);
	}

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

void CastToPostgresVarchar(ClientContext &context, Vector &input, Vector &result, idx_t size);

void CastListToPostgresArray(ClientContext &context, Vector &input, Vector &varchar_vector, idx_t size) {
	// cast child list
	auto &child_data = ListVector::GetEntry(input);
	auto child_count = ListVector::GetListSize(input);
	Vector child_varchar(LogicalType::VARCHAR, child_count);
	CastToPostgresVarchar(context, child_data, child_varchar, child_count);

	// construct the list entries
	auto child_entries = FlatVector::GetData<string_t>(child_varchar);
	auto list_entries = FlatVector::GetData<list_entry_t>(input);
	auto result_entries = FlatVector::GetData<string_t>(varchar_vector);
	for(idx_t r = 0; r < size; r++) {
		if (FlatVector::IsNull(input, r)) {
			FlatVector::SetNull(varchar_vector, r, true);
			continue;
		}
		auto list_entry = list_entries[r];
		string result;
		result = "{";
		for(idx_t list_idx = 0; list_idx < list_entry.length; list_idx++) {
			if (list_idx > 0) {
				result += ",";
			}
			auto child_idx = list_entry.offset + list_idx;
			if (FlatVector::IsNull(child_varchar, child_idx)) {
				result += "NULL";
			} else {
				result += child_entries[child_idx].GetString();
			}
		}
		result += "}";
		result_entries[r] = StringVector::AddString(varchar_vector, result);
	}
}

void CastBlobToPostgres(ClientContext &context, Vector &input, Vector &result, idx_t size) {
	auto input_data = FlatVector::GetData<string_t>(input);
	auto result_data = FlatVector::GetData<string_t>(result);
	for(idx_t r = 0; r < size; r++) {
		if (FlatVector::IsNull(input, r)) {
			FlatVector::SetNull(result, r, true);
			continue;
		}
		const char *HEX_STRING = "0123456789ABCDEF";
		string blob_str = "\\\\x";
		auto blob_data = const_data_ptr_cast(input_data[r].GetData());
		auto blob_size = input_data[r].GetSize();
		for(idx_t c = 0; c < blob_size; c++) {
			blob_str += HEX_STRING[blob_data[c] / 16];
			blob_str += HEX_STRING[blob_data[c] % 16];
		}
		result_data[r] = StringVector::AddString(result, blob_str);
	}
}

void CastToPostgresVarchar(ClientContext &context, Vector &input, Vector &result, idx_t size) {
	switch (input.GetType().id()) {
	case LogicalTypeId::LIST:
		CastListToPostgresArray(context, input, result, size);
		break;
	case LogicalTypeId::BLOB:
		CastBlobToPostgres(context, input, result, size);
		break;
	default:
		VectorOperations::Cast(context, input, result, size);
		break;
	}
}


void PostgresConnection::CopyChunk(ClientContext &context, PostgresCopyFormat format, DataChunk &chunk, DataChunk &varchar_chunk) {
	chunk.Flatten();

	if (format == PostgresCopyFormat::BINARY) {
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
	} else if (format == PostgresCopyFormat::TEXT) {
		// cast columns to varchar
		if (varchar_chunk.ColumnCount() == 0) {
			// not initialized yet
			vector<LogicalType> varchar_types;
			for(idx_t c = 0; c < chunk.ColumnCount(); c++) {
				varchar_types.push_back(LogicalType::VARCHAR);
			}
			varchar_chunk.Initialize(Allocator::DefaultAllocator(), varchar_types);
		}
		D_ASSERT(chunk.ColumnCount() == varchar_chunk.ColumnCount());
		// for text format cast to varchar first
		for(idx_t c = 0; c < chunk.ColumnCount(); c++) {
			CastToPostgresVarchar(context, chunk.data[c], varchar_chunk.data[c], chunk.size());
		}
		varchar_chunk.SetCardinality(chunk.size());

		PostgresTextWriter writer;
		for (idx_t r = 0; r < chunk.size(); r++) {
			for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
				if (c > 0) {
					writer.WriteSeparator();
				}
				D_ASSERT(varchar_chunk.data[c].GetType().id() == LogicalTypeId::VARCHAR);
				auto &col = varchar_chunk.data[c];
				writer.WriteValue(col, r);
			}
			writer.FinishRow();
		}
		CopyData(writer);
	}
}


}
