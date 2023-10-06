//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "postgres_utils.hpp"

#include <cstddef>

namespace duckdb {

class PostgresStatement {
public:
	PostgresStatement();
	PostgresStatement(PGconn *connection, string name);
	~PostgresStatement();
	// disable copy constructors
	PostgresStatement(const PostgresStatement &other) = delete;
	PostgresStatement &operator=(const PostgresStatement &) = delete;
	//! enable move constructors
	PostgresStatement(PostgresStatement &&other) noexcept;
	PostgresStatement &operator=(PostgresStatement &&) noexcept;

	PGconn *connection;
	string name;
};

} // namespace duckdb
