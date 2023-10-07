//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_conversion.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "postgres_conversion.hpp"

namespace duckdb {

class PostgresBinaryWriter {
public:

	template<class T>
	T GetInteger(T val) {
		if (sizeof(T) == sizeof(uint16_t)) {
			return htons(val);
		} else if (sizeof(T) == sizeof(uint32_t)) {
			return htonl(val);
		} else if (sizeof(T) == sizeof(uint64_t)) {
			return htonll(val);
		} else {
			D_ASSERT(0);
			return val;
		}
	}

	template<class T>
	void WriteRawInteger(T val) {
		serializer.Write<T>(GetInteger(val));
	}

public:
	void WriteHeader() {
		serializer.WriteData(const_data_ptr_cast("PGCOPY\n\377\r\n\0"), 11);
		WriteRawInteger<int32_t>(0);
		WriteRawInteger<int32_t>(0);
	}

	void WriteFooter() {
		WriteRawInteger<int16_t>(-1);
	}

	void BeginRow(idx_t column_count) {
		// field count
		WriteRawInteger<int16_t>(column_count);
	}

	void FinishRow() {
	}

	void WriteNull() {
		WriteRawInteger<int32_t>(-1);
	}

	template<class T>
	void WriteInteger(T value) {
		WriteRawInteger<int32_t>(sizeof(T));
		WriteRawInteger<T>(value);
	}

public:
	MemoryStream serializer;
};

} // namespace duckdb
