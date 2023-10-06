//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "postgres_utils.hpp"

namespace duckdb {

class PostgresResult {
public:
	PostgresResult(PGresult *res_p) : res(res_p) {
	}
	~PostgresResult() {
		if (res) {
			PQclear(res);
		}
	}
	PGresult *res = nullptr;

public:
	string GetString(idx_t row, idx_t col) {
		D_ASSERT(res);
		return string(PQgetvalue(res, row, col));
	}

	int32_t GetInt32(idx_t row, idx_t col) {
		return atoi(PQgetvalue(res, row, col));
	}
	int64_t GetInt64(idx_t row, idx_t col) {
		return atoll(PQgetvalue(res, row, col));
	}
	bool GetBool(idx_t row, idx_t col) {
		return strcmp(PQgetvalue(res, row, col), "t");
	}
	idx_t Count() {
		D_ASSERT(res);
		return PQntuples(res);
	}
};

}
