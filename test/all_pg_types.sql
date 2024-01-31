CREATE TABLE pg_numtypes (
	bool_col bool,
	smallint_col smallint,
	integer_col integer,
	bigint_col bigint,
	float_col float4,
	double_col float8,
	decimal_col decimal(4, 1),
	udecimal_col decimal);

CREATE TABLE pg_bytetypes (
    char_default char,
	char_1_col char(1),
	char_9_col char(9),
	varchar_1_col varchar(1),
	varchar_9_col varchar(9),
	text_col text,
	blob_col bytea,
	json_col json,
	uuid_col uuid);

CREATE TABLE pg_datetypes (
	date_col date,
	time_col time,
	timetz_col timetz,
	timestamp_col timestamp,
	timestamptz_col timestamptz
);

CREATE TYPE enum_type AS ENUM ('foo', 'bar', 'baz');

CREATE TABLE pg_numarraytypes (
                             bool_col _bool,
                             smallint_col _int2,
                             integer_col _int4,
                             bigint_col _int8,
                             float_col _float4,
                             double_col _float8,
                             numeric_col _numeric(4, 1),
                             unumeric_col _numeric);

CREATE TABLE pg_bytearraytypes (
                              char_col _char,
                              bpchar_col _bpchar,
                              varchar_col _varchar(10),
                              uvarchar_col _varchar,
                              text_col _text,
                              blob_col _bytea,
                              json_col _json,
                              uuid_col _uuid);

CREATE TABLE pg_datearraytypes (
                              date_col _date,
                              time_col _time,
                              timetz_col _timetz,
                              timestamp_col _timestamp,
                              timestamptz_col _timestamptz);

CREATE TABLE pg_enumarraytypes (
							  enum_col _enum_type);

INSERT INTO pg_numtypes (bool_col, smallint_col, integer_col, bigint_col, float_col, double_col, decimal_col, udecimal_col) VALUES
	(false, 0, 0, 0, 0, 0, 0, 0),
	(false, -42, -42, -42, -42.01, -42.01, -42.01, -42.01),
	(true, 42, 42, 42, 42.01, 42.01, 42.01, 42.01),
	(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);


INSERT INTO pg_bytetypes (char_default, char_1_col, char_9_col, varchar_1_col, varchar_9_col, text_col, blob_col, json_col, uuid_col) VALUES
	('a', 'a', '', '', '', '', '',  '42', '00000000-0000-0000-0000-000000000000'),
	('a', 'a', 'aaaaaaaaa', 'a', 'aaaaaaaaa', 'dpfkg', 'dpfkg', '{"a":42}', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'),
	('Z', 'Z', 'ZZZZZZZZZ', 'Z', 'ZZZZZZZZZ', 'dpfkg', 'dpfkg', '{"a":42}',  'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'),
	(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);


SET TIMEZONE='Asia/Kathmandu'; -- UTC - 05:45 hell yeah!

INSERT INTO pg_datetypes (date_col, time_col, timetz_col, timestamp_col, timestamptz_col) VALUES ('2021-03-01', '12:45:01', '12:45:01', '2021-03-01T12:45:01', '2021-03-01T12:45:01'), (NULL, NULL, NULL, NULL, NULL);

insert into pg_numarraytypes (bool_col, smallint_col, integer_col, bigint_col, float_col, double_col, numeric_col, unumeric_col)
VALUES ('{true, false, NULL}', '{-42, 42, NULL}', '{-4200, 4200, NULL}', '{-420000, 420000, NULL}', '{-4.2, 4.2}', '{-4.2, 4.2}', '{-4.2, 4.2}', '{-4.2, 4.2}'), (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

insert into pg_bytearraytypes (char_col, bpchar_col , varchar_col, uvarchar_col, text_col, blob_col , json_col , uuid_col)
VALUES ('{a, Z, NULL}', '{a, Z, NULL}', '{aaaa, ZZZZ, NULL}', '{aaaa, ZZZZ, NULL}', '{aaaa, ZZZZ, NULL}', '{\x00, \xff, NULL}', array['{"a":42}', NULL]::json[], '{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11, NULL}'), (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

insert into pg_datearraytypes (date_col, time_col,timetz_col, timestamp_col, timestamptz_col)
VALUES ('{2019-11-26, 2021-03-01, NULL}','{14:42:43, 12:45:01, NULL}','{14:42:43, 12:45:01, NULL}','{2019-11-26T12:45:01, 2021-03-01T12:45:01, NULL}','{2019-11-26T12:45:01, 2021-03-01T12:45:01, NULL}'), (NULL, NULL, NULL, NULL, NULL);

insert into pg_enumarraytypes (enum_col)
VALUES ('{}'), ('{foo}'), ('{foo, bar}'), ('{foo, bar, baz}'), ('{foo, bar, baz, NULL}'), (NULL); 


CREATE TABLE pg_macaddr (
	macaddr_col macaddr
);
insert into pg_macaddr values ('08:00:2b:01:02:03');

CREATE TABLE pg_complex_types_mix (
	id int,
	macaddr_col macaddr[],
	str varchar,
	str_list varchar[],
	b bytea
);

-- unbounded numerics
CREATE TABLE pg_giant_numeric (
	n NUMERIC
);
INSERT INTO pg_giant_numeric
VALUES  (0), (99999999999999999999999999999999.99999999999999999999999999999999), (-123456789123456789123456789.123456789123456789123456789);

CREATE TABLE pg_giant_numeric_array (
	n NUMERIC[]
);
INSERT INTO pg_giant_numeric_array
VALUES  (ARRAY[0, 99999999999999999999999999999999.99999999999999999999999999999999, -123456789123456789123456789.123456789123456789123456789]), (ARRAY[]::NUMERIC[]);

CREATE TABLE pg_numeric_empty (
	n NUMERIC
);
CREATE TABLE pg_numeric_array_empty (
	n NUMERIC[]
);

-- json
CREATE TABLE pg_json (
	regular_json JSON
);
INSERT INTO pg_json VALUES ('{"a": 42, "b": "string"}');

-- composite type example
CREATE TYPE inventory_item AS (
    name            text,
    supplier_id     integer,
    price           numeric
);
CREATE TABLE on_hand (
    item      inventory_item,
    count     integer
);
INSERT INTO on_hand VALUES (ROW('fuzzy dice', 42, 1.99), 1000);

-- complex composite types
CREATE TYPE composite_with_arrays AS (
    names            text[],
    ids              integer[]
);
CREATE TYPE pg_mood AS ENUM ('sad', 'ok', 'happy');
CREATE TYPE composite_with_enums AS (
    current_mood            pg_mood,
    past_moods              pg_mood[]
);
CREATE TABLE composite_with_arrays_tbl (
    item      composite_with_arrays
);
INSERT INTO composite_with_arrays_tbl VALUES (ROW(array['Name 1', 'Name 2'], array[42, 84, 100, 120]));

CREATE TABLE composite_with_enums_tbl (
    item      composite_with_enums
);
INSERT INTO composite_with_enums_tbl VALUES (ROW('happy', ARRAY['ok', 'happy', 'sad', 'happy', 'ok']::pg_mood[]));

CREATE TABLE array_of_composites_tbl (
	items inventory_item[]
);
INSERT INTO array_of_composites_tbl VALUES (ARRAY[ROW('fuzzy dice', 42, 1.99), ROW('fuzzy mice', 84, 0.5)]::inventory_item[]);

CREATE TABLE many_multidimensional_arrays (
	i int[][][],
	s varchar[][]
);
INSERT INTO many_multidimensional_arrays VALUES (
	ARRAY[ARRAY[[1, 2, 3]], ARRAY[[4, 5, 6]], ARRAY[[7, 8, 9]]],
	ARRAY[ARRAY['hello world', 'abc'], ARRAY['this is', 'an array']]);

-- postgres allows mixing array dimensions (for some reason)
-- duckdb does not, so we need to catch this error
CREATE TABLE mixed_arrays (
	i int[]
);
INSERT INTO mixed_arrays VALUES (
	ARRAY[ARRAY[[1, 2, 3]], ARRAY[[4, 5, 6]], ARRAY[[7, 8, 9]]]
);
INSERT INTO mixed_arrays VALUES (
	ARRAY[1, 2, 3]
);

CREATE TYPE composite_a AS (i INTEGER, j INTEGER);
CREATE TYPE composite_b AS (a composite_a, k INTEGER);
CREATE TABLE composites_of_composites (b composite_b);

INSERT INTO composites_of_composites VALUES (((1,2),3)), (((4,5),6)), (((7,8),9));

-- Issue #136 - Inconsistent results from querying postgres numeric columns
create TABLE public_amounts (
    id bigint NOT NULL,
    rate numeric NOT NULL
);
insert into public_amounts values
   (1, 0.67),
   (2, 0.067),
   (3, 0.0067),
   (4, 0.00067),
   (5, 0.000067),
   (6, 0.0000067),
   (7, 0.00000067),
   (8, 0.000000067),
   (9, 0.0000000067),
   (10, 0.00000000067),
   (11, 0.000000000067),
   (12, 0.0000000000067),
   (13, 0.00000000000067),
   (14, 0.000000000000067),
   (15, 0.0000000000000067),
   (16, 0.00000000000000067),
   (17, 0.000000000000000067),
   (18, 0.0000000000000000067),
   (19, 0.00000000000000000067),
   (20, 0.000000000000000000067);

CREATE TABLE big_generated_table AS SELECT * FROM generate_series(0,999999);

-- built-in geometric types
create table geometry(p point);
insert into geometry values ('(1,2)');
insert into geometry values ('(-1.5,-2.5)');
insert into geometry values (NULL);
