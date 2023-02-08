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

" |  psql -d postgresscanner



echo "
CREATE TABLE
  duckdb_arr_test
  (
    id      INTEGER     NOT NULL,
    my_ints INTEGER[]   NOT NULL
  );

INSERT INTO
  duckdb_arr_test
  (
    id,
    my_ints
  )
VALUES
  (
    123,
    ARRAY[11, 22, 33]
  )
;
INSERT INTO
  duckdb_arr_test
  (
    id,
    my_nums
  )
VALUES
  (
    234,
    ARRAY[]::INTEGER[]
  )
;
" | psql -d postgresscanner


echo "
CREATE TABLE oids (i oid);
INSERT INTO oids VALUES (42), (43);" | psql -d postgresscanner

echo "
CREATE TABLE daterange (room int, during tsrange);
INSERT INTO daterange VALUES
    (1108, '[2010-01-01 14:30, 2010-01-01 15:30)');
" | psql -d postgresscanner


echo "
CREATE DOMAIN my_type_v30 AS VARCHAR(30) NOT NULL;

CREATE DOMAIN my_id AS INT4;

CREATE TABLE my_table (
    table_id my_id PRIMARY KEY,
    table_var varchar(10),
    table_v30 my_type_v30
);
insert into my_table values (42, 'something', 'something else');

" | psql -d postgresscanner

echo "
CREATE SCHEMA some_schema;

create type some_schema.some_enum as enum('one', 'two');

CREATE TABLE some_schema.some_table (
    some_field some_schema.some_enum
);
insert into some_schema.some_table values ('two');

" | psql -d postgresscanner

echo "create table fail(n numeric(12,7));
      insert into fail values (32.8875000);" | psql -d postgresscanner


echo "
CREATE TABLE dum();
CREATE TABLE dee();
INSERT INTO dee DEFAULT VALUES;
" | psql -d postgresscanner


psql -d postgresscanner -c "CHECKPOINT"
psql -d postgresscanner -c "VACUUM"
