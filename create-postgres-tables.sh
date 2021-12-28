#!/bin/bash
make
echo "
CREATE SCHEMA tpch; 
CREATE SCHEMA tpcds;
CALL dbgen(sf=1, schema='tpch');
CALL dsdgen(sf=1, schema='tpcds');
EXPORT DATABASE '/tmp/postgresscannertmp';
" | \
./duckdb/build/release/duckdb 

dropdb postgresscanner
createdb postgresscanner
psql -d postgresscanner < /tmp/postgresscannertmp/schema.sql
psql -d postgresscanner < /tmp/postgresscannertmp/load.sql
rm -rf /tmp/postgresscannertmp
psql -d postgresscanner -c "CHECKPOINT"