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

#include <arpa/inet.h>
// htonll is not available on Linux it seems
#ifndef ntohll
#define ntohll(x) ((((uint64_t)ntohl(x & 0xFFFFFFFF)) << 32) + ntohl(x >> 32))
#endif

#define POSTGRES_EPOCH_JDATE 2451545 /* == date2j(2000, 1, 1) */

namespace duckdb {

struct PostgresConversion {
	static constexpr const char *COPY_HEADER = "PGCOPY\n\377\r\n\0";
	static constexpr const idx_t COPY_HEADER_LENGTH = 11;
};

} // namespace duckdb
