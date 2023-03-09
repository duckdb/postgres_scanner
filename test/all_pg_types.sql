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
