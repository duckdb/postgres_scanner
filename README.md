# DuckDB postgresscanner extension

The postgresscanner extension allows DuckDB to directly read data from a running Postgres instance. The data can be queried directly from the underlying Postgres tables, or read into DuckDB tables.

## Usage

To make a Postgres database accessible to DuckDB, use the `POSTGRES_ATTACH` command:
```SQL
CALL POSTGRES_ATTACH('');
```
`POSTGRES_ATTACH` takes a single required string parameter, which is the [`libpq` connection string](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING). For example you can pass `'dbname=postgresscanner'` to select a different database name. In the simplest case, the parameter is just `''`. There are three additional named parameters:
* `source_schema` the name of a non-standard schema name in Postgres to get tables from. Default is `public`.
* `sink_schema` the schema name in DuckDB to create views. Default is `main`.
* `overwrite` whether we should overwrite existing views in the target schema, default is `false`.
* `filter_pushdown` whether filter predicates that DuckDB derives from the query should be forwarded to Postgres, defaults to `false`.

### `sink_schema` usage

attach Postgres schema to another DuckDB schema.

```sql
-- create a new schema in DuckDB first
create schema abc;
CALL postgres_attach('dbname=postgres user=postgres host=127.0.0.1',source_schema='public' , sink_schema='abc');
select table_schema,table_name,table_type  FROM information_schema.tables;
```

The tables in the database are registered as views in DuckDB, you can list them with
```SQL
PRAGMA show_tables;
```
Then you can query those views normally using SQL.

If you prefer to not attach all tables, but just query a single table, that is possible using the `POSTGRES_SCAN` table-producing function, e.g.

```SQL
SELECT * FROM POSTGRES_SCAN('', 'public', 'mytable');
```

`POSTGRES_SCAN` takes three string parameters, the `libpq` connection string (see above), a Postgres schema name and a table name. The schema name is often `public`.




## Building & Loading the Extension

To build, type 
```
make
```

To run, run the bundled `duckdb` shell:
```
 ./duckdb/build/release/duckdb -unsigned  # allow unsigned extensions
```

Then, load the Postgres extension like so:
```SQL
LOAD 'build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
```


## License

Copyright 2022 DuckDB Labs BV [hello@duckdblabs.com].

This project is licensed under the GNU General Public License (LICENSE-GPL). Alternative licensing options are available from DuckDB Labs.
