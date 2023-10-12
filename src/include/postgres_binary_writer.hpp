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
	template <class T>
	T GetInteger(T val) {
		if (sizeof(T) == sizeof(uint8_t)) {
			return val;
		} else if (sizeof(T) == sizeof(uint16_t)) {
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

	template <class T>
	void WriteRawInteger(T val) {
		stream.Write<T>(GetInteger(val));
	}

public:
	void WriteHeader() {
		stream.WriteData(const_data_ptr_cast(PostgresConversion::COPY_HEADER), PostgresConversion::COPY_HEADER_LENGTH);
		WriteRawInteger<int32_t>(0);
		WriteRawInteger<int32_t>(0);
	}

	void WriteFooter() {
		WriteRawInteger<int16_t>(-1);
	}

	void BeginRow(idx_t column_count) {
		// field count
		WriteRawInteger<int16_t>(int16_t(column_count));
	}

	void FinishRow() {
	}

	void WriteNull() {
		WriteRawInteger<int32_t>(-1);
	}

	template <class T>
	void WriteInteger(T value) {
		WriteRawInteger<int32_t>(sizeof(T));
		WriteRawInteger<T>(value);
	}

	void WriteBoolean(bool value) {
		WriteInteger<uint8_t>(value ? 1 : 0);
	}

	void WriteFloat(float value) {
		uint32_t i = *reinterpret_cast<uint32_t *>(&value);
		WriteInteger<uint32_t>(i);
	}

	void WriteDouble(double value) {
		uint64_t i = *reinterpret_cast<uint64_t *>(&value);
		WriteInteger<uint64_t>(i);
	}

	int32_t DuckDBDateToPostgres(int32_t value) {
		if (value <= POSTGRES_MIN_DATE || value >= POSTGRES_MAX_DATE) {
			throw InvalidInputException("DATE \"%s\" is out of range for Postgres' DATE field",
			                            Date::ToString(date_t(value)));
		}
		return value + DUCKDB_EPOCH_DATE - POSTGRES_EPOCH_JDATE;
	}

	void WriteDate(date_t value) {
		WriteInteger<int32_t>(DuckDBDateToPostgres(value.days));
	}

	void WriteTime(dtime_t value) {
		WriteInteger<uint64_t>(value.micros);
	}

	void WriteTimeTZ(dtime_tz_t value) {
		WriteRawInteger<int32_t>(sizeof(uint64_t) + sizeof(int32_t));
		WriteRawInteger<uint64_t>(value.time().micros);
		WriteRawInteger<int32_t>(-value.offset());
	}

	uint64_t DuckDBTimestampToPostgres(timestamp_t value) {
		if (value == timestamp_t::infinity()) {
			return POSTGRES_INFINITY;
		}
		if (value == timestamp_t::ninfinity()) {
			return POSTGRES_NINFINITY;
		}
		auto time = value.value % Interval::MICROS_PER_DAY;
		// adjust date
		auto date = DuckDBDateToPostgres(value.value / Interval::MICROS_PER_DAY);
		// glue it back together
		return date * Interval::MICROS_PER_DAY + time;
	}

	void WriteTimestamp(timestamp_t value) {
		WriteInteger<uint64_t>(DuckDBTimestampToPostgres(value));
	}

	void WriteInterval(interval_t value) {
		WriteRawInteger<int32_t>(sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint32_t));
		WriteRawInteger<uint64_t>(value.micros);
		WriteRawInteger<uint32_t>(value.days);
		WriteRawInteger<uint32_t>(value.months);
	}

	void WriteUUID(hugeint_t value) {
		WriteRawInteger<int32_t>(sizeof(uint64_t) * 2);
		WriteRawInteger<uint64_t>(value.upper ^ uint64_t(1) << 63);
		WriteRawInteger<uint64_t>(value.lower);
	}

	template <class T, class OP = DecimalConversionInteger>
	void WriteDecimal(T value, uint16_t scale) {
		constexpr idx_t MAX_DIGITS = sizeof(T) * 4;
		uint16_t sign;
		if (value < 0) {
			value = -value;
			sign = NUMERIC_NEG;
		} else {
			sign = NUMERIC_POS;
		}
		// divide the decimal into the integer part (before the decimal point) and fractional part (after the point)
		T integer_part;
		T fractional_part;
		if (scale == 0) {
			integer_part = value;
			fractional_part = 0;
		} else {
			integer_part = value / OP::GetPowerOfTen(scale);
			fractional_part = value % OP::GetPowerOfTen(scale);
		}
		uint16_t integral_digits[MAX_DIGITS];
		uint16_t fractional_digits[MAX_DIGITS];
		int32_t integral_ndigits = 0;
		// split the integral part into parts of up to NBASE (4 digits => 0..9999)
		while (integer_part > 0) {
			integral_digits[integral_ndigits++] = uint16_t(integer_part % T(NBASE));
			integer_part /= T(NBASE);
		}
		// split the fractional part into parts of up to NBASE (4 digits => 0..9999)
		// count the amount of digits required for the fractional part
		// note that while it is technically possible to leave out zeros here this adds even more complications
		// so we just always write digits for the full "scale", even if not strictly required
		int32_t fractional_ndigits = (scale + DEC_DIGITS - 1) / DEC_DIGITS;
		// fractional digits are LEFT aligned (for some unknown reason)
		// that means if we write ".12" with a scale of 2 we actually need to write "1200", instead of "12"
		// this means we need to "correct" the number 12 by multiplying by 100 in this case
		// this correction factor is the "number of digits to the next full number"
		int32_t correction = fractional_ndigits * DEC_DIGITS - scale;
		fractional_part *= OP::GetPowerOfTen(correction);
		for (idx_t i = 0; i < fractional_ndigits; i++) {
			fractional_digits[i] = uint16_t(fractional_part % NBASE);
			fractional_part /= NBASE;
		}

		int32_t ndigits = integral_ndigits + fractional_ndigits;
		int32_t weight = integral_ndigits - 1;
		// size
		WriteRawInteger<int32_t>(int32_t(sizeof(uint16_t)) * (4 + ndigits));
		// header
		WriteRawInteger<uint16_t>(ndigits);
		WriteRawInteger<int16_t>(weight);
		WriteRawInteger<uint16_t>(sign);
		WriteRawInteger<uint16_t>(scale);
		// wriet the integer and fractional values (in reverse order)
		for (idx_t i = integral_ndigits; i > 0; i--) {
			WriteRawInteger<uint16_t>(integral_digits[i - 1]);
		}
		for (idx_t i = fractional_ndigits; i > 0; i--) {
			WriteRawInteger<uint16_t>(fractional_digits[i - 1]);
		}
	}

	void WriteVarchar(string_t value) {
		WriteRawInteger<int32_t>(value.GetSize());
		stream.WriteData(const_data_ptr_cast(value.GetData()), value.GetSize());
	}

public:
	MemoryStream stream;
};

} // namespace duckdb
