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
	template <class T>
	static T LoadInteger(const_data_ptr_t &value_ptr) {
		T val = Load<T>(value_ptr);
		if (sizeof(T) == sizeof(uint16_t)) {
			val = ntohs(val);
		} else if (sizeof(T) == sizeof(uint32_t)) {
			val = ntohl(val);
		} else if (sizeof(T) == sizeof(uint64_t)) {
			val = ntohll(val);
		} else {
			D_ASSERT(0);
		}
		value_ptr += sizeof(T);
		return val;
	}

	static float LoadFloat(const_data_ptr_t &value_ptr) {
		auto i = PostgresConversion::LoadInteger<uint32_t>(value_ptr);
		return *((float *)&i);
	}

	static float LoadDouble(const_data_ptr_t &value_ptr) {
		auto i = PostgresConversion::LoadInteger<uint64_t>(value_ptr);
		return *((double *)&i);
	}

	static date_t LoadDate(const_data_ptr_t &value_ptr) {
		auto jd = PostgresConversion::LoadInteger<uint32_t>(value_ptr);
		return date_t(jd + POSTGRES_EPOCH_JDATE - 2440588); // magic!
	}

	static timestamp_t LoadTimestamp(const_data_ptr_t &value_ptr) {
		auto usec = ntohll(Load<uint64_t>(value_ptr));
		auto time = usec % Interval::MICROS_PER_DAY;
		// adjust date
		auto date = (usec / Interval::MICROS_PER_DAY) + POSTGRES_EPOCH_JDATE - 2440588;
		// glue it back together
		return timestamp_t(date * Interval::MICROS_PER_DAY + time);
	}

	static interval_t LoadInterval(const_data_ptr_t &value_ptr) {
		interval_t res;
		res.micros = PostgresConversion::LoadInteger<uint64_t>(value_ptr);
		res.days = PostgresConversion::LoadInteger<uint32_t>(value_ptr);
		res.months = PostgresConversion::LoadInteger<uint32_t>(value_ptr);
		return res;
	}

	static hugeint_t LoadUUID(const_data_ptr_t &value_ptr) {
		hugeint_t res;

		auto upper = PostgresConversion::LoadInteger<uint64_t>(value_ptr);
		res.upper = upper ^ (int64_t(1) << 63);
		res.lower = PostgresConversion::LoadInteger<uint64_t>(value_ptr);

		return res;
	}
};

} // namespace duckdb
