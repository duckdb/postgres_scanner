//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_version.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

struct PostgresVersion {
	PostgresVersion() {
	}
	PostgresVersion(idx_t major, idx_t minor, idx_t patch = 0) : major(major), minor(minor), patch(patch) {
	}

	idx_t major = 0;
	idx_t minor = 0;
	idx_t patch = 0;

	inline bool operator<(const PostgresVersion &rhs) const {
		if (major < rhs.major) {
			return true;
		}
		if (major > rhs.major) {
			return false;
		}
		if (minor < rhs.minor) {
			return true;
		}
		if (minor > rhs.minor) {
			return false;
		}
		return patch < rhs.patch;
	};
	inline bool operator<=(const PostgresVersion &rhs) const {
		return !(rhs < *this);
	};
	inline bool operator>(const PostgresVersion &rhs) const {
		return rhs < *this;
	};
	inline bool operator>=(const PostgresVersion &rhs) const {
		return !(*this < rhs);
	};
};

} // namespace duckdb
