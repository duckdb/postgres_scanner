#include "duckdb.hpp"
#include <libpq-fe.h>
#include <thread>
#include <chrono>
#include <random>

std::random_device rd;  // only used once to initialise (seed) engine
std::mt19937 rng(rd()); // random-number engine used (Mersenne-Twister in this case)

using namespace duckdb;

struct PostgresResult {
	~PostgresResult() {
		if (res) {
			PQclear(res);
		}
	}
	PGresult *res = nullptr;

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
};

static void PGExec(PGconn *conn, string q) {
	auto res = make_uniq<PostgresResult>();
	res->res = PQexec(conn, q.c_str());

	if (!res->res) {
		throw IOException("Unable to query Postgres");
	}
	if (PQresultStatus(res->res) != PGRES_COMMAND_OK) {
		throw IOException("Unable to query Postgres: %s", string(PQresultErrorMessage(res->res)));
	}
}

static unique_ptr<PostgresResult> PGQuery(PGconn *conn, string q) {
	auto res = make_uniq<PostgresResult>();
	res->res = PQexec(conn, q.c_str());

	if (!res->res) {
		throw IOException("Unable to query Postgres");
	}
	if (PQresultStatus(res->res) != PGRES_TUPLES_OK) {
		throw IOException("Unable to query Postgres: %s", string(PQresultErrorMessage(res->res)));
	}
	return res;
}

static DuckDB db;

static string CHECKQ = "SELECT SUM(val), COUNT(val) FROM series";
static uint64_t INVARIANT = 42000000;
static uint64_t THREADS = 4;

static const char *DSN = "";

static bool carry_on = true;

void ptask(idx_t i) {
	idx_t n = 0;
	std::uniform_int_distribution<int> ids_rng(1, 1000000); // guaranteed unbiased
	std::uniform_int_distribution<int> amount_rng(1, 100);  // guaranteed unbiased

	auto pconn = PQconnectdb(DSN);

	if (PQstatus(pconn) == CONNECTION_BAD) {
		throw IOException("Unable to connect to Postgres at %s", DSN);
	}

	while (carry_on) {
		auto source = ids_rng(rng);
		auto target = ids_rng(rng);
		auto amount = amount_rng(rng);

		/* Multiple queries sent in a single PQexec call are processed in a single
		 * transaction, unless there are explicit BEGIN/COMMIT commands included in
		 * the query string to divide it into multiple transactions. */
		PGExec(pconn, StringUtil::Format(R"(
UPDATE series SET val = val - %d WHERE id=%d;
UPDATE series SET val = val + %d WHERE id=%d;
        )",
		                                 amount, source, amount, target));

		n++;
	}

	PQfinish(pconn);
	printf("ptask %llu done %llu\n", i, n);
}

void ctask(idx_t sleep_ms) {
	idx_t n = 0;
	auto pconn = PQconnectdb(DSN);

	if (PQstatus(pconn) == CONNECTION_BAD) {
		throw IOException("Unable to connect to Postgres at %s", DSN);
	}

	while (carry_on) {
		PGExec(pconn, "CHECKPOINT");
		std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));

		n++;
	}
	PQfinish(pconn);
	printf("ctask done %llu\n", n);
}

void pctask() {
	idx_t failure = 0;
	idx_t n = 0;
	auto pconn = PQconnectdb(DSN);

	while (carry_on) {

		auto pres = PGQuery(pconn, CHECKQ);
		if (pres->GetInt64(0, 0) != INVARIANT) {
			InternalException("Invariant fail");
		}
		pres.reset();
		n++;
	}
	PQfinish(pconn);

	printf("pctask done %llu, inconsistent %llu\n", n, failure);
}

void dtask() {
	idx_t failure = 0;

	idx_t n = 0;
	Connection dconn(db);

	while (carry_on) {

		auto dres = dconn.Query(CHECKQ);
		if (!dres->success) {
			throw InternalException(dres->error);
		}
		auto val = dres->GetValue(0, 0).GetValue<int64_t>();
		if (val != INVARIANT) {
			failure++;
			printf("Invariant fail : %llu %llu\n", val, dres->GetValue(1, 0).GetValue<int64_t>());
		}
		n++;
	}

	printf("dtask done %llu, inconsistent %llu\n", n, failure);
}

void ttask(idx_t n) {
	std::this_thread::sleep_for(std::chrono::milliseconds(n));
	carry_on = false;
}

int main() {
	auto pconn = PQconnectdb(DSN);

	if (PQstatus(pconn) == CONNECTION_BAD) {
		throw IOException("Unable to connect to Postgres at %s", DSN);
	}
	PGExec(pconn, "DROP TABLE IF EXISTS series");
	PGExec(pconn, "CREATE TABLE series AS select * from generate_series (1, "
	              "1000000) id, LATERAL (SELECT 42 val) sq");
	PGExec(pconn, "CREATE INDEX series_pk ON series (id)");
	PGExec(pconn, "CHECKPOINT");

	// query the table schema so we can interpret the bits in the pages
	// fun fact: this query also works in DuckDB ^^
	auto pres = PGQuery(pconn, CHECKQ);
	if (pres->GetInt64(0, 0) != INVARIANT) {
		InternalException("Invariant fail");
	}
	pres.reset();
	PQfinish(pconn);

	Connection dconn(db);
	auto dres = dconn.Query("LOAD 'build/release/postgres_scanner.duckdb_extension'");
	if (!dres->success) {
		throw InternalException(dres->error);
	}
	dres = dconn.Query("CALL postgres_attach('')");
	if (!dres->success) {
		throw InternalException(dres->error);
	}
	dres = dconn.Query(CHECKQ);
	if (!dres->success) {
		throw InternalException(dres->error);
	}
	auto val = dres->GetValue(0, 0).GetValue<int64_t>();
	if (val != INVARIANT) {
		printf("Initial Invariant fail : %llu %llu\n", val, dres->GetValue(1, 0).GetValue<int64_t>());
	}

	// battle plan: launch n threads, have them loop transactions that move val
	// around between random ids launch another thread that loops the duckdb query

	std::thread d1(dtask);
	std::thread pc1(pctask);

	std::thread t1(ttask, 10000);
	std::thread c1(ctask, 500);

	vector<std::thread *> pthreads(THREADS);
	for (idx_t i = 0; i < THREADS; i++) {
		pthreads[i] = new std::thread(ptask, i);
	}

	for (idx_t i = 0; i < THREADS; i++) {
		pthreads[i]->join();
		delete pthreads[i];
	}

	d1.join();
	pc1.join();

	t1.join();
	c1.join();

	printf("done\n");
	return 0;
}
