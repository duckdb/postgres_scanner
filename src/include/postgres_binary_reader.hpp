//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_binary_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types/interval.hpp"
#include "postgres_conversion.hpp"

namespace duckdb {

struct PostgresBinaryReader {
	explicit PostgresBinaryReader(PostgresConnection &con_p) : con(con_p) {
	}
	~PostgresBinaryReader() {
		Reset();
	}

	bool Next() {
		Reset();
		char *out_buffer;
		int len = PQgetCopyData(con.GetConn(), &out_buffer, 0);
		auto new_buffer = data_ptr_cast(out_buffer);

		// len -1 signals end
		if (len == -1) {
			return false;
		}

		// len -2 is error
		// we expect at least 2 bytes in each message for the tuple count
		if (!new_buffer || len < sizeof(int16_t)) {
			throw IOException("Unable to read binary COPY data from Postgres: %s",
			                  string(PQerrorMessage(con.GetConn())));
		}
		buffer = new_buffer;
		buffer_ptr = buffer;
		end = buffer + len;
		return true;
	}

	void CheckResult() {
		auto result = PQgetResult(con.GetConn());
		if (!result || PQresultStatus(result) != PGRES_COMMAND_OK) {
			throw std::runtime_error("Failed to execute COPY: " + string(PQresultErrorMessage(result)));
		}
	}

	void Reset() {
		if (buffer) {
			PQfreemem(buffer);
		}
		buffer = nullptr;
		buffer_ptr = nullptr;
		end = nullptr;
	}
	bool Ready() {
		return buffer_ptr != nullptr;
	}

	void CheckHeader() {
		auto magic_len = PostgresConversion::COPY_HEADER_LENGTH;
		auto flags_len = 8;
		auto header_len = magic_len + flags_len;

		if (buffer_ptr + header_len >= end) {
			throw IOException("Unable to read binary COPY data from Postgres, invalid header");
		}
		if (memcmp(buffer_ptr, PostgresConversion::COPY_HEADER, magic_len) != 0) {
			throw IOException("Expected Postgres binary COPY header, got something else");
		}
		buffer_ptr += header_len;
		// as far as i can tell the "Flags field" and the "Header
		// extension area length" do not contain anything interesting
	}

public:
	template <class T>
	inline T ReadIntegerUnchecked() {
		T val = Load<T>(buffer_ptr);
		if (sizeof(T) == sizeof(uint8_t)) {
			// no need to flip single byte
		} else if (sizeof(T) == sizeof(uint16_t)) {
			val = ntohs(val);
		} else if (sizeof(T) == sizeof(uint32_t)) {
			val = ntohl(val);
		} else if (sizeof(T) == sizeof(uint64_t)) {
			val = ntohll(val);
		} else {
			D_ASSERT(0);
		}
		buffer_ptr += sizeof(T);
		return val;
	}

	bool OutOfBuffer() {
		return buffer_ptr >= end;
	}

	template <class T>
	inline T ReadInteger() {
		if (buffer_ptr + sizeof(T) > end) {
			throw IOException("Postgres scanner - out of buffer in ReadInteger");
		}
		return ReadIntegerUnchecked<T>();
	}

	inline bool ReadBoolean() {
		auto i = ReadInteger<uint8_t>();
		return i > 0;
	}

	inline float ReadFloat() {
		auto i = ReadInteger<uint32_t>();
		return *reinterpret_cast<float *>(&i);
	}

	inline double ReadDouble() {
		auto i = ReadInteger<uint64_t>();
		return *reinterpret_cast<double *>(&i);
	}

	inline date_t ReadDate() {
		auto jd = ReadInteger<uint32_t>();
		if (jd == POSTGRES_DATE_INF) {
			return date_t::infinity();
		}
		if (jd == POSTGRES_DATE_NINF) {
			return date_t::ninfinity();
		}
		return date_t(jd + POSTGRES_EPOCH_JDATE - DUCKDB_EPOCH_DATE); // magic!
	}

	inline dtime_t ReadTime() {
		return dtime_t(ReadInteger<uint64_t>());
	}

	inline dtime_tz_t ReadTimeTZ() {
		auto usec = ReadInteger<uint64_t>();
		auto tzoffset = ReadInteger<int32_t>();
		return dtime_tz_t(dtime_t(usec), -tzoffset);
	}

	inline timestamp_t ReadTimestamp() {
		auto usec = ReadInteger<uint64_t>();
		if (usec == POSTGRES_INFINITY) {
			return timestamp_t::infinity();
		}
		if (usec == POSTGRES_NINFINITY) {
			return timestamp_t::ninfinity();
		}
		return timestamp_t(usec + (POSTGRES_EPOCH_TS - DUCKDB_EPOCH_TS));
	}

	inline interval_t ReadInterval() {
		interval_t res;
		res.micros = ReadInteger<uint64_t>();
		res.days = ReadInteger<uint32_t>();
		res.months = ReadInteger<uint32_t>();
		return res;
	}

	inline hugeint_t ReadUUID() {
		hugeint_t res;
		auto upper = ReadInteger<uint64_t>();
		res.upper = upper ^ (int64_t(1) << 63);
		res.lower = ReadInteger<uint64_t>();
		return res;
	}

	const char *ReadString(idx_t string_length) {
		if (buffer_ptr + string_length > end) {
			throw IOException("Postgres scanner - out of buffer in ReadString");
		}
		auto result = const_char_ptr_cast(buffer_ptr);
		buffer_ptr += string_length;
		return result;
	}

	PostgresDecimalConfig ReadDecimalConfig() {
		PostgresDecimalConfig config;
		config.ndigits = ReadInteger<uint16_t>();
		config.weight = ReadInteger<int16_t>();
		auto sign = ReadInteger<uint16_t>();

		if (!(sign == NUMERIC_POS || sign == NUMERIC_NAN || sign == NUMERIC_PINF || sign == NUMERIC_NINF ||
		      sign == NUMERIC_NEG)) {
			throw NotImplementedException("Postgres numeric NA/Inf");
		}
		config.is_negative = sign == NUMERIC_NEG;
		config.scale = ReadInteger<uint16_t>();

		return config;
	}

	template <class T, class OP = DecimalConversionInteger>
	T ReadDecimal() {
		// this is wild
		auto config = ReadDecimalConfig();
		auto scale_POWER = OP::GetPowerOfTen(config.scale);

		if (config.ndigits == 0) {
			return 0;
		}
		T integral_part = 0, fractional_part = 0;

		if (config.weight >= 0) {
			integral_part = ReadInteger<uint16_t>();
			for (auto i = 1; i <= config.weight; i++) {
				integral_part *= NBASE;
				if (i < config.ndigits) {
					integral_part += ReadInteger<uint16_t>();
				}
			}
			integral_part *= scale_POWER;
		}

		// we need to find out how large the fractional part is in terms of powers
		// of ten this depends on how many times we multiplied with NBASE
		// if that is different from scale, we need to divide the extra part away
		// again
		// similarly, if trailing zeroes have been suppressed, we have not been multiplying t
		// the fractional part with NBASE often enough. If so, add additional powers
		if (config.ndigits > config.weight + 1) {
			auto fractional_power = (config.ndigits - config.weight - 1) * DEC_DIGITS;
			auto fractional_power_correction = fractional_power - config.scale;
			D_ASSERT(fractional_power_correction < 20);
			fractional_part = 0;
			for (auto i = config.weight + 1; i < config.ndigits; i++) {
				if (i + 1 < config.ndigits) {
					// more digits remain - no need to compensate yet
					fractional_part *= NBASE;
					fractional_part += ReadInteger<uint16_t>();
				} else {
					// last digit, compensate
					T final_base = NBASE;
					T final_digit = ReadInteger<uint16_t>();
					if (fractional_power_correction >= 0) {
						T compensation = OP::GetPowerOfTen(fractional_power_correction);
						final_base /= compensation;
						final_digit /= compensation;
					} else {
						T compensation = OP::GetPowerOfTen(-fractional_power_correction);
						final_base *= compensation;
						final_digit *= compensation;
					}
					fractional_part *= final_base;
					fractional_part += final_digit;
				}
			}
		}

		// finally
		auto base_res = OP::Finalize(config, integral_part + fractional_part);
		return (config.is_negative ? -base_res : base_res);
	}

private:
	data_ptr_t buffer = nullptr;
	data_ptr_t buffer_ptr = nullptr;
	data_ptr_t end = nullptr;
	PostgresConnection &con;
};

} // namespace duckdb
