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

	void WriteChar(char c) {
		switch(c) {
		case '\n':
			WriteChar('\\');
			WriteChar('n');
			break;
		case '\r':
			WriteChar('\\');
			WriteChar('r');
			break;
		case '\b':
			WriteChar('\\');
			WriteChar('b');
			break;
		case '\f':
			WriteChar('\\');
			WriteChar('f');
			break;
		case '\t':
			WriteChar('\\');
			WriteChar('t');
			break;
		case '\v':
			WriteChar('\\');
			WriteChar('v');
			break;
		default:
			stream.WriteData(const_data_ptr_cast(&c), 1);
			break;
		}
	}

	void WriteVarchar(string_t value) {
		auto size = value.GetSize();
		auto data = value.GetData();
		for(idx_t c = 0; c < size; c++) {
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
