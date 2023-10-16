//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_connection_pool.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {
class PostgresConnectionPool;

class PostgresConnectionReservation {
public:
	PostgresConnectionReservation();
	PostgresConnectionReservation(optional_ptr<PostgresConnectionPool> pool, idx_t reservation_count);
	~PostgresConnectionReservation();
	// disable copy constructors
	PostgresConnectionReservation(const PostgresConnectionReservation &other) = delete;
	PostgresConnectionReservation &operator=(const PostgresConnectionReservation &) = delete;
	//! enable move constructors
	PostgresConnectionReservation(PostgresConnectionReservation &&other) noexcept;
	PostgresConnectionReservation &operator=(PostgresConnectionReservation &&) noexcept;

	idx_t GetConnectionCount();

private:
	optional_ptr<PostgresConnectionPool> pool;
	idx_t reservation_count;
};

class PostgresConnectionPool {
public:
	static constexpr const idx_t DEFAULT_MAX_CONNECTIONS = 64;

	PostgresConnectionPool(idx_t maximum_connections = DEFAULT_MAX_CONNECTIONS);

public:
	PostgresConnectionReservation AllocateConnections(idx_t count);
	void FreeConnections(idx_t count);
	void SetMaximumConnections(idx_t new_max);

private:
	mutex connection_lock;
	idx_t remaining_connections;
	idx_t maximum_connections;
};

} // namespace duckdb
