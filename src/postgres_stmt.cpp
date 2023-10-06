#include "postgres_stmt.hpp"
#include "postgres_connection.hpp"

namespace duckdb {

PostgresStatement::PostgresStatement() : connection(nullptr) {
}

PostgresStatement::PostgresStatement(PGconn *connection, string name_p) : connection(connection), name(std::move(name_p)) {
	D_ASSERT(connection);
}

PostgresStatement::~PostgresStatement() {
}

PostgresStatement::PostgresStatement(PostgresStatement &&other) noexcept {
	std::swap(connection, other.connection);
	std::swap(name, other.name);
}

PostgresStatement &PostgresStatement::operator=(PostgresStatement &&other) noexcept {
	std::swap(connection, other.connection);
	std::swap(name, other.name);
	return *this;
}

} // namespace duckdb
