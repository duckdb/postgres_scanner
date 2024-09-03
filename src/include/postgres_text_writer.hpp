//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_text_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "postgres_conversion.hpp"

namespace duckdb {

class PostgresTextWriter {
public:
	explicit PostgresTextWriter(PostgresCopyState &state) : state(state) {
	}

	void WriteNull() {
		stream.WriteData(const_data_ptr_cast("\b"), 1);
	}

	void WriteCharInternal(char c) {
		stream.WriteData(const_data_ptr_cast(&c), 1);
	}

	void WriteChar(char c) {
		switch (c) {
		case '\n':
			WriteCharInternal('\\');
			WriteCharInternal('n');
			break;
		case '\r':
			WriteCharInternal('\\');
			WriteCharInternal('r');
			break;
		case '\b':
			WriteCharInternal('\\');
			WriteCharInternal('b');
			break;
		case '\f':
			WriteCharInternal('\\');
			WriteCharInternal('f');
			break;
		case '\t':
			WriteCharInternal('\\');
			WriteCharInternal('t');
			break;
		case '\v':
			WriteCharInternal('\\');
			WriteCharInternal('v');
			break;
		case '\\':
			WriteCharInternal('\\');
			WriteCharInternal('\\');
			break;
		case '"':
			WriteCharInternal('\\');
			WriteCharInternal('"');
			break;
		case '\0':
			if (!state.has_null_byte_replacement) {
				throw InvalidInputException("Attempting to write a VARCHAR value with a NULL-byte. Postgres does not "
				                            "support NULL-bytes in VARCHAR values.\n* SET pg_null_byte_replacement='' "
				                            "to remove NULL bytes or replace them with another character");
			}
			for (const auto replacement_chr : state.null_byte_replacement) {
				WriteChar(replacement_chr);
			}
			break;
		default:
			WriteCharInternal(c);
			break;
		}
	}

	void WriteVarchar(string_t value) {
		auto size = value.GetSize();
		auto data = value.GetData();
		for (idx_t c = 0; c < size; c++) {
			WriteChar(data[c]);
		}
	}

	void WriteValue(Vector &col, idx_t r) {
		if (col.GetType().id() != LogicalTypeId::VARCHAR) {
			throw InternalException("Text format can only write VARCHAR columns");
		}
		if (FlatVector::IsNull(col, r)) {
			WriteNull();
		} else {
			WriteVarchar(FlatVector::GetData<string_t>(col)[r]);
		}
	}

	void WriteSeparator() {
		stream.WriteData(const_data_ptr_cast("\t"), 1);
	}

	void FinishRow() {
		stream.WriteData(const_data_ptr_cast("\n"), 1);
	}

	void WriteFooter() {
		stream.WriteData(const_data_ptr_cast("\\.\n"), 3);
	}

public:
	MemoryStream stream;
	PostgresCopyState &state;
};

} // namespace duckdb
