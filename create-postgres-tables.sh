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
psql -d postgresscanner < all_pg_types.sql

rm -rf /tmp/postgresscannertmp

echo "
create table nulltest (c1 integer, c2 integer, c3 integer, c4 integer, c5 integer, c6 integer, c7 integer, c8 integer, c9 integer, c10 integer);
insert into nulltest values (1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
insert into nulltest values (1, NULL, 3, 4, NULL, 6, 7, 8, NULL, 10);
insert into nulltest values (NULL, NULL, 3, 4, 5, 6, 7, NULL, NULL, NULL);
insert into nulltest values (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
" | psql -d postgresscanner

echo "
create type color_t as enum('blue', 'red', 'gray', 'black');

drop table if exists cars;
create table cars
 (
   brand   text,
   model   text,
   color   color_t
 );

insert into cars(brand, model, color)
     values ('ferari', 'testarosa', 'red'),
            ('aston martin', 'db2', 'blue'),
            ('bentley', 'mulsanne', 'gray'),
            ('ford', 'T', 'black');

create table intervals as select '42 day'::INTERVAL interval_days UNION ALL SELECT '42 month'::INTERVAL UNION ALL SELECT '42 year'::INTERVAL UNION ALL SELECT  '42 minute'::INTERVAL UNION ALL SELECT  '42 second'::INTERVAL UNION ALL SELECT '0.42 second'::INTERVAL UNION ALL SELECT '-42 day'::INTERVAL interval_days  UNION ALL SELECT NULL::INTERVAL;

create table numerics (n numeric(12, 7));
insert into numerics values (32.8875000), (-32.8875000), (10000), (1000), (100), (10), (1), (0.1), (0.01), (0.001), (0.0001), (0.00001), (0.000001), (0.0000001), (10001), (1.0001), (0.0001001), (10001.0001), (1.0001001), (10001.0001001), (0);
" |  psql -d postgresscanner



psql -d postgresscanner -c "CHECKPOINT"
psql -d postgresscanner -c "VACUUM"

