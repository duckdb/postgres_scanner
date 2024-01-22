# DuckDB Postgres extension

The Postgres extension allows DuckDB to directly read and write data from a running Postgres database instance. The data can be queried directly from the underlying Postgres database. Data can be loaded from Postgres tables into DuckDB tables, or vice versa.

## Reading Data from Postgres

To make a Postgres database accessible to DuckDB use the `ATTACH` command:

```sql
ATTACH 'dbname=postgresscanner' AS postgres_db (TYPE postgres);
USE postgres_db;
```

The `ATTACH` command takes as input a [`libpq` connection string](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING) - which is a set of `key=value` pairs separated by spaces. Below are some example connection strings and commonly used parameters. A full list of available parameters can be found [in the Postgres documentation](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS).

```
dbname=postgresscanner
host=localhost port=5432 dbname=mydb connect_timeout=10
```

|   Name   |             Description              |    Default     |
|----------|--------------------------------------|----------------|
| host     | Name of host to connect to           | localhost      |
| hostaddr | Host IP address                      | localhost      |
| port     | Port Number                          | 5432           |
| user     | Postgres User Name                   | [OS user name] |
| password | Postgres Password                    |                |
| dbname   | Database Name                        | [user]         |
| passfile | Name of file passwords are stored in | ~/.pgpass      |


The tables in the file can be read as if they were normal DuckDB tables, but the underlying data is read directly from Postgres at query time.

```sql
D SHOW TABLES;
┌───────────────────────────────────────┐
│                 name                  │
│                varchar                │
├───────────────────────────────────────┤
│ uuids                                 │
└───────────────────────────────────────┘
D SELECT * FROM uuids;
┌──────────────────────────────────────┐
│                  u                   │
│                 uuid                 │
├──────────────────────────────────────┤
│ 6d3d2541-710b-4bde-b3af-4711738636bf │
│ NULL                                 │
│ 00000000-0000-0000-0000-000000000001 │
│ ffffffff-ffff-ffff-ffff-ffffffffffff │
└──────────────────────────────────────┘
```

It might be desirable to create a copy of the Postgres databases in DuckDB to prevent the system from re-reading the tables from Postgres continuously, particularly for large tables.

Data can be copied over from Postgres to DuckDB using standard SQL, for example:

```sql
CREATE TABLE duckdb_table AS FROM postgres_db.postgres_tbl;
```

## Writing Data to Postgres

In addition to reading data from Postgres, the extension allows you to create tables, ingest data into Postgres and make other modifications to a Postgres database using standard SQL queries.

This allows you to use DuckDB to, for example, export data that is stored in a Postgres database to Parquet, or read data from a Parquet file into Postgres.

Below is a brief example of how to create a new table in Postgres and load data into it.

```sql
ATTACH 'dbname=postgresscanner' AS postgres_db (TYPE postgres);
CREATE TABLE postgres_db.tbl(id INTEGER, name VARCHAR);
INSERT INTO postgres_db.tbl VALUES (42, 'DuckDB');
```
Many operations on Postgres tables are supported. All these operations directly modify the Postgres database, and the result of subsequent operations can then be read using Postgres.
Note that if modifications are not desired, `ATTACH` can be run with the `READ_ONLY` property which prevents making modifications to the underlying database. For example:

```sql
ATTACH 'dbname=postgresscanner' AS postgres_db (TYPE postgres, READ_ONLY);
```

Below is a list of supported operations.

###### CREATE TABLE
```sql
CREATE TABLE postgres_db.tbl(id INTEGER, name VARCHAR);
```

###### INSERT INTO
```sql
INSERT INTO postgres_db.tbl VALUES (42, 'DuckDB');
```

###### SELECT
```sql
SELECT * FROM postgres_db.tbl;
┌───────┬─────────┐
│  id   │  name   │
│ int64 │ varchar │
├───────┼─────────┤
│    42 │ DuckDB  │
└───────┴─────────┘
```

###### COPY
```sql
COPY postgres_db.tbl TO 'data.parquet';
COPY postgres_db.tbl FROM 'data.parquet';
```

###### UPDATE
```sql
UPDATE postgres_db.tbl SET name='Woohoo' WHERE id=42;
```

###### DELETE
```sql
DELETE FROM postgres_db.tbl WHERE id=42;
```

###### ALTER TABLE
```sql
ALTER TABLE postgres_db.tbl ADD COLUMN k INTEGER;
```

###### DROP TABLE
```sql
DROP TABLE postgres_db.tbl;
```

###### CREATE VIEW
```sql
CREATE VIEW postgres_db.v1 AS SELECT 42;
```

###### CREATE SCHEMA/DROP SCHEMA
```sql
CREATE SCHEMA postgres_db.s1;
CREATE TABLE postgres_db.s1.integers(i int);
INSERT INTO postgres_db.s1.integers VALUES (42);
SELECT * FROM postgres_db.s1.integers;
┌───────┐
│   i   │
│ int32 │
├───────┤
│    42 │
└───────┘
DROP SCHEMA postgres_db.s1;
```

###### Transactions
```sql
CREATE TABLE postgres_db.tmp(i INTEGER);
BEGIN;
INSERT INTO postgres_db.tmp VALUES (42);
SELECT * FROM postgres_db.tmp;
┌───────┐
│   i   │
│ int64 │
├───────┤
│    42 │
└───────┘
ROLLBACK;
SELECT * FROM postgres_db.tmp;
┌────────┐
│   i    │
│ int64  │
├────────┤
│ 0 rows │
└────────┘
```

## Running SQL Queries in Postgres with postgres_query

The postgres_query function allows you to run arbitrary SQL within an attached database. `postgres_query` takes the name of the attached Postgres database to execute the query in, as well as the SQL query to execute. The result of the query is returned.

```sql
postgres_query(attached_database::VARCHAR, query::VARCHAR)
```

###### Example

```sql
ATTACH 'dbname=postgresscanner' AS s (TYPE POSTGRES);
SELECT * FROM postgres_query('s', 'SELECT * FROM cars LIMIT 3');
┌──────────────┬───────────┬─────────┐
│    brand     │   model   │  color  │
│   varchar    │  varchar  │ varchar │
├──────────────┼───────────┼─────────┤
│ ferari       │ testarosa │ red     │
│ aston martin │ db2       │ blue    │
│ bentley      │ mulsanne  │ gray    │
└──────────────┴───────────┴─────────┘
```

## Settings

The extension exposes the following configuration parameters.

|              name               |                                description                                 | default |
|---------------------------------|----------------------------------------------------------------------------|---------|
| pg_debug_show_queries           | DEBUG SETTING: print all queries sent to Postgres to stdout                | false   |
| pg_experimental_filter_pushdown | Whether or not to use filter pushdown (currently experimental)             | false   |
| pg_array_as_varchar             | Read Postgres arrays as varchar - enables reading mixed dimensional arrays | false   |
| pg_connection_cache             | Whether or not to use the connection cache                                 | true    |
| pg_connection_limit             | The maximum amount of concurrent Postgres connections                      | 64      |
| pg_pages_per_task               | The amount of pages per task                                               | 1000    |
| pg_use_binary_copy              | Whether or not to use BINARY copy to read data                             | true    |


## Schema Cache

To avoid having to continuously fetch schema data from Postgres, DuckDB keeps schema information - such as the names of tables, their columns, etc -  cached. If changes are made to the schema through a different connection to the Postgres instance, such as new columns being added to a table, the cached schema information might be outdated. In this case, the function `pg_clear_cache` can be executed to clear the internal caches.

```sql
CALL pg_clear_cache();
```

## Building & Loading the Extension

The DuckDB submodule must be initialized prior to building.

```bash
git submodule init
git pull --recurse-submodules
```

To build, type 
```
make
```

To run, run the bundled `duckdb` shell:
```
 ./build/release/duckdb -unsigned  # allow unsigned extensions
```

Then, load the Postgres extension like so:
```SQL
LOAD 'build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
```
