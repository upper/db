BEGIN TRANSACTION;

DROP TABLE IF EXISTS artist;

CREATE TABLE artist (
  name string
);

DROP TABLE IF EXISTS publication;

CREATE TABLE publication (
  title string,
  author_id int
);

DROP TABLE IF EXISTS review;

CREATE TABLE review (
  publication_id int,
  name string,
  comments string,
  created time
);


DROP TABLE IF EXISTS data_types;

CREATE TABLE data_types (
  _uint uint,
  _uint8 uint8,
  _uint16 uint16,
  _uint32 uint32,
  _uint64 uint64,
  _int int,
  _int8 int8,
  _int16 int16,
  _int32 int32,
  _int64 int64,
  _float32 float32,
  _float64 float64,
  _bool bool,
  _string string,
  _date time,
  _nildate time,
  _ptrdate time,
  _time time
);

DROP TABLE IF EXISTS stats_test;

CREATE TABLE stats_test (
	id uint,
	numeric int64,
	value int64
);

DROP TABLE IF EXISTS composite_keys;

-- Composite keys are currently not supported in QL.
CREATE TABLE composite_keys (
-- code string,
-- user_id string,
  some_val string,
-- primary key (code, user_id)
);

COMMIT;
