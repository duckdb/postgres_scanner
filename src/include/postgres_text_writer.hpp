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
};

} // namespace duckdb
