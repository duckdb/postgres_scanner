#!/bin/bash
echo "
CREATE SCHEMA tpch; 
CREATE SCHEMA tpcds;
CALL dbgen(sf=0.01, schema='tpch');
CALL dsdgen(sf=0.01, schema='tpcds');
EXPORT DATABASE '/tmp/postgresscannertmp';
" | \
./build/release/duckdb

dropdb --if-exists postgresscanner
createdb postgresscanner

psql -d postgresscanner < /tmp/postgresscannertmp/schema.sql
psql -d postgresscanner < /tmp/postgresscannertmp/load.sql
rm -rf /tmp/postgresscannertmp

psql -d postgresscanner < test/all_pg_types.sql
psql -d postgresscanner < test/decimals.sql
psql -d postgresscanner < test/other.sql


psql -d postgresscanner -c "CHECKPOINT"
psql -d postgresscanner -c "VACUUM"
