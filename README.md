# DuckDB <> PostgreSQL integration


To build, type 
```
make
```

To run, run the bundled `duckdb` shell:
```
 ./duckdb/build/release/duckdb 
```

Then, load the Postgres extension like so:
```SQL
LOAD 'build/release/postgers_scanner.duckdb_extension';
```

To make a SQLite file accessible to DuckDB, use the `POSTGRES_ATTACH` command:
```SQL
CALL POSTGRES_ATTACH('');
```
`POSTGRES_ATTACH` takes a single required string parameter, which is the [`libpq` connection string](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING). For example you can pass `'dbname=postgresscanner'` to select a different database name. In the simplest case, the parameter is just `''`. There are three additional named parameters:
 * `source_schema` the name of a non-standard schema name in Postgres to get tables from. Default is `public`.
 * `target_schema` the name of a DuckDB schema to put the views in, the default is `main`.
 * `overwrite` whether we should overwrite existing views in the target schema, default is `false`.


The tables in the database are registered as views in DuckDB, you can list them with
```SQL
PRAGMA show_tables;
```
Then you can query those views normally using SQL.

If you prefer to not attach all tables, but just query a single table, that is possible using the `POSTGRES_SCAN` table-producing function, e.g.

```SQL
select * from postgres_scan('', 'public', 'mytable');
```

`POSTGRES_SCAN` takes three string parameters, the `libpq` connection string (see above), a Postgres schema name and a table name. The schema name is often `public`.



