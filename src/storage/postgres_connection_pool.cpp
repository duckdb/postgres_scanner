#include "storage/postgres_connection_pool.hpp"

namespace duckdb {

PostgresConnectionReservation::PostgresConnectionReservation() : pool(nullptr), reservation_count(0) {}

PostgresConnectionReservation::PostgresConnectionReservation(optional_ptr<PostgresConnectionPool> pool, idx_t reservation_count) :
	pool(pool), reservation_count(reservation_count) {}

PostgresConnectionReservation::~PostgresConnectionReservation() {
	if (pool && reservation_count == 0) {
		pool->FreeConnections(reservation_count);
	}
}

PostgresConnectionReservation::PostgresConnectionReservation(PostgresConnectionReservation &&other) noexcept {
	std::swap(pool, other.pool);
	std::swap(reservation_count, other.reservation_count);
}

PostgresConnectionReservation &PostgresConnectionReservation::operator=(PostgresConnectionReservation &&other) noexcept {
	std::swap(pool, other.pool);
	std::swap(reservation_count, other.reservation_count);
	return *this;
}

idx_t PostgresConnectionReservation::GetConnectionCount() {
	if (reservation_count == 0) {
		return 1;
	}
	return reservation_count;
}

PostgresConnectionPool::PostgresConnectionPool(idx_t maximum_connections_p) :
	remaining_connections(maximum_connections_p), maximum_connections(maximum_connections_p) {}

PostgresConnectionReservation PostgresConnectionPool::AllocateConnections(idx_t count) {
	lock_guard<mutex> l(connection_lock);
	idx_t reserve_connections = MinValue<idx_t>(count, remaining_connections);
	remaining_connections -= reserve_connections;
	return PostgresConnectionReservation(this, reserve_connections);
}

void PostgresConnectionPool::FreeConnections(idx_t count) {
	lock_guard<mutex> l(connection_lock);
	remaining_connections += count;
	if (remaining_connections > maximum_connections) {
		remaining_connections = maximum_connections;
	}
}

void PostgresConnectionPool::SetMaximumConnections(idx_t new_max) {
	lock_guard<mutex> l(connection_lock);
	if (new_max < maximum_connections) {
		idx_t reduced_connections = maximum_connections - new_max;
		if (remaining_connections >= reduced_connections) {
			remaining_connections -= reduced_connections;
		} else {
			// we can't reclaim all connections because there are outstanding connections left
			// set the remaining connections to zero and wait to reclaim them
			remaining_connections = 0;
		}
	} else {
		idx_t additional_connections = new_max - maximum_connections;
		remaining_connections += additional_connections;
	}
	maximum_connections = new_max;
}

}
