LOAD 'build/release/postgres_scanner.duckdb_extension';
PRAGMA enable_profiling;
PRAGMA threads=8;
SELECT * FROM postgres_scan('', 'lineitem') limit 10;
