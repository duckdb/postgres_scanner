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

#define NBASE      10000
#define DEC_DIGITS 4 /* decimal digits per NBASE digit */

/*
 * Interpretation of high bits.
 */

#define NUMERIC_SIGN_MASK 0xC000
#define NUMERIC_POS       0x0000
#define NUMERIC_NEG       0x4000
#define NUMERIC_SHORT     0x8000
#define NUMERIC_SPECIAL   0xC000

/*
 * Definitions for special values (NaN, positive infinity, negative infinity).
 *
 * The two bits after the NUMERIC_SPECIAL bits are 00 for NaN, 01 for positive
 * infinity, 11 for negative infinity.  (This makes the sign bit match where
 * it is in a short-format value, though we make no use of that at present.)
 * We could mask off the remaining bits before testing the active bits, but
 * currently those bits must be zeroes, so masking would just add cycles.
 */
#define NUMERIC_EXT_SIGN_MASK 0xF000 /* high bits plus NaN/Inf flag bits */
#define NUMERIC_NAN           0xC000
#define NUMERIC_PINF          0xD000
#define NUMERIC_NINF          0xF000
#define NUMERIC_INF_SIGN_MASK 0x2000

#define NUMERIC_EXT_FLAGBITS(n) ((n)->choice.n_header & NUMERIC_EXT_SIGN_MASK)
#define NUMERIC_IS_NAN(n)       ((n)->choice.n_header == NUMERIC_NAN)
#define NUMERIC_IS_PINF(n)      ((n)->choice.n_header == NUMERIC_PINF)
#define NUMERIC_IS_NINF(n)      ((n)->choice.n_header == NUMERIC_NINF)
#define NUMERIC_IS_INF(n)       (((n)->choice.n_header & ~NUMERIC_INF_SIGN_MASK) == NUMERIC_PINF)

/*
 * Short format definitions.
 */

#define NUMERIC_DSCALE_MASK            0x3FFF
#define NUMERIC_SHORT_SIGN_MASK        0x2000
#define NUMERIC_SHORT_DSCALE_MASK      0x1F80
#define NUMERIC_SHORT_DSCALE_SHIFT     7
#define NUMERIC_SHORT_DSCALE_MAX       (NUMERIC_SHORT_DSCALE_MASK >> NUMERIC_SHORT_DSCALE_SHIFT)
#define NUMERIC_SHORT_WEIGHT_SIGN_MASK 0x0040
#define NUMERIC_SHORT_WEIGHT_MASK      0x003F
#define NUMERIC_SHORT_WEIGHT_MAX       NUMERIC_SHORT_WEIGHT_MASK
#define NUMERIC_SHORT_WEIGHT_MIN       (-(NUMERIC_SHORT_WEIGHT_MASK + 1))

#define NUMERIC_SIGN(is_short, header1)                                                                                \
	(is_short ? ((header1 & NUMERIC_SHORT_SIGN_MASK) ? NUMERIC_NEG : NUMERIC_POS) : (header1 & NUMERIC_SIGN_MASK))
#define NUMERIC_DSCALE(is_short, header1)                                                                              \
	(is_short ? (header1 & NUMERIC_SHORT_DSCALE_MASK) >> NUMERIC_SHORT_DSCALE_SHIFT : (header1 & NUMERIC_DSCALE_MASK))
#define NUMERIC_WEIGHT(is_short, header1, header2)                                                                     \
	(is_short ? ((header1 & NUMERIC_SHORT_WEIGHT_SIGN_MASK ? ~NUMERIC_SHORT_WEIGHT_MASK : 0) |                         \
	             (header1 & NUMERIC_SHORT_WEIGHT_MASK))                                                                \
	          : (header2))

// copied from cast_helpers.cpp because windows linking issues
static const int64_t POWERS_OF_TEN[] {1,
                                      10,
                                      100,
                                      1000,
                                      10000,
                                      100000,
                                      1000000,
                                      10000000,
                                      100000000,
                                      1000000000,
                                      10000000000,
                                      100000000000,
                                      1000000000000,
                                      10000000000000,
                                      100000000000000,
                                      1000000000000000,
                                      10000000000000000,
                                      100000000000000000,
                                      1000000000000000000};

struct PostgresDecimalConfig {
	uint16_t scale;
	uint16_t ndigits;
	int16_t weight;
	bool is_negative;
};

struct PostgresBinaryReader {
	explicit PostgresBinaryReader(PostgresConnection &con_p) : con(con_p) {
	}
	~PostgresBinaryReader() {
		Reset();
	}

	void Next() {
		Reset();
		char *out_buffer;
		idx_t len = PQgetCopyData(con.GetConn(), &out_buffer, 0);
		buffer = data_ptr_cast(out_buffer);

		// len -2 is error
		// len -1 is supposed to signal end but does not actually happen in practise
		// we expect at least 2 bytes in each message for the tuple count
		if (!buffer || len < sizeof(int16_t)) {
			throw IOException("Unable to read binary COPY data from Postgres: %s",
			                  string(PQerrorMessage(con.GetConn())));
		}
		buffer_ptr = buffer;
		end = buffer + len;
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
		return date_t(jd + POSTGRES_EPOCH_JDATE - 2440588); // magic!
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
		auto time = usec % Interval::MICROS_PER_DAY;
		// adjust date
		auto date = (usec / Interval::MICROS_PER_DAY) + POSTGRES_EPOCH_JDATE - 2440588;
		// glue it back together
		return timestamp_t(date * Interval::MICROS_PER_DAY + time);
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

	const char *ReadString(idx_t string_length) {
		if (buffer_ptr + string_length > end) {
			throw IOException("Postgres scanner - out of buffer in ReadString");
		}
		auto result = const_char_ptr_cast(buffer_ptr);
		buffer_ptr += string_length;
		return result;
	}

	template <class T>
	T ReadDecimal(PostgresDecimalConfig &config) {
		// this is wild
		auto scale_POWER = POWERS_OF_TEN[config.scale];

		if (config.ndigits == 0) {
			return 0;
		}
		T integral_part = 0, fractional_part = 0;

		if (config.weight >= 0) {
			D_ASSERT(config.weight <= config.ndigits);
			integral_part = ReadInteger<uint16_t>();
			for (auto i = 1; i <= config.weight; i++) {
				integral_part *= NBASE;
				if (i < config.ndigits) {
					integral_part += ReadInteger<uint16_t>();
				}
			}
			integral_part *= scale_POWER;
		}

		if (config.ndigits > config.weight + 1) {
			fractional_part = ReadInteger<uint16_t>();
			for (auto i = config.weight + 2; i < config.ndigits; i++) {
				fractional_part *= NBASE;
				if (i < config.ndigits) {
					fractional_part += ReadInteger<uint16_t>();
				}
			}

			// we need to find out how large the fractional part is in terms of powers
			// of ten this depends on how many times we multiplied with NBASE
			// if that is different from scale, we need to divide the extra part away
			// again
			// similarly, if trailing zeroes have been suppressed, we have not been multiplying t
			// the fractional part with NBASE often enough. If so, add additional powers
			auto fractional_power = (config.ndigits - config.weight - 1) * DEC_DIGITS;
			auto fractional_power_correction = fractional_power - config.scale;
			D_ASSERT(fractional_power_correction < 20);
			if (fractional_power_correction >= 0) {
				fractional_part /= POWERS_OF_TEN[fractional_power_correction];
			} else {
				fractional_part *= POWERS_OF_TEN[-fractional_power_correction];
			}
		}

		// finally
		auto base_res = (integral_part + fractional_part);
		return (config.is_negative ? -base_res : base_res);
	}

private:
	data_ptr_t buffer = nullptr;
	data_ptr_t buffer_ptr = nullptr;
	data_ptr_t end = nullptr;
	PostgresConnection &con;
};

} // namespace duckdb
