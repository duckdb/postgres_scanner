DROP TABLE IF EXISTS pg_numtypes;
DROP TABLE IF EXISTS pg_bytetypes;
DROP TABLE IF EXISTS pg_datetypes;

CREATE TABLE pg_numtypes (
	bool_col bool,
	smallint_col smallint,
	integer_col integer,
	bigint_col bigint,
	float_col float4,
	double_col float8,
	decimal_col decimal(4, 1));

CREATE TABLE pg_bytetypes (
	char_1_col char(1),
	char_9_col char(9),
	varchar_1_col varchar(1),
	varchar_9_col varchar(9),
	text_col text,
	blob_col bytea,
	json_col json,
	jsonb_col jsonb,
	uuid_col uuid);

CREATE TABLE pg_datetypes (
	date_col date,
	time_col time,
	timetz_col timetz,
	timestamp_col timestamp,
	timestamptz_col timestamptz
);

INSERT INTO pg_numtypes (bool_col, smallint_col, integer_col, bigint_col, float_col, double_col, decimal_col) VALUES 
	(false, 0, 0, 0, 0, 0, 0), 
	(false, -42, -42, -42, -42.01, -42.01, -42.01), 
	(true, 42, 42, 42, 42.01, 42.01, 42.01), 
	(NULL, NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO pg_bytetypes (char_1_col, char_9_col, varchar_1_col, varchar_9_col, text_col, blob_col, json_col, jsonb_col, uuid_col) VALUES
	('a', '', '', '', '', '',  '42', '42', '00000000-0000-0000-0000-000000000000'),
	('a', 'aaaaaaaaa', 'a', 'aaaaaaaaa', 'dpfkg', 'dpfkg', '{"a":42}', '{"a":42}', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'),
	('Z', 'ZZZZZZZZZ', 'Z', 'ZZZZZZZZZ', 'dpfkg', 'dpfkg', '{"a":42}', '{"a":42}', 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'),
	(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);


SET TIMEZONE='Asia/Kathmandu'; -- UTC - 05:45 hell yeah!

INSERT INTO pg_datetypes (date_col, time_col, timetz_col, timestamp_col, timestamptz_col) VALUES ('2021-03-01', '12:45:01', '12:45:01', '2021-03-01T12:45:01', '2021-03-01T12:45:01'), (NULL, NULL, NULL, NULL, NULL);
