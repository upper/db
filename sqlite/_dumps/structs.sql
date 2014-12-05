PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;

DROP TABLE IF EXISTS artist;

CREATE TABLE artist (
  id integer primary key,
  name varchar(60)
);

DROP TABLE IF EXISTS publication;

CREATE TABLE publication (
  id integer primary key,
  title varchar(80),
  author_id integer
);

DROP TABLE IF EXISTS review;

CREATE TABLE review (
  id integer primary key,
  publication_id integer,
  name varchar(80),
  comments text,
  created varchar(20)
);

DROP TABLE IF EXISTS data_types;

CREATE TABLE data_types (
  id integer primary key,
 _uint integer,
 _uintptr integer,
 _uint8 integer,
 _uint16 int,
 _uint32 int,
 _uint64 int,
 _int integer,
 _int8 integer,
 _int16 integer,
 _int32 integer,
 _int64 integer,
 _float32 real,
 _float64 real,
 _byte integer,
 _rune integer,
 _bool integer,
 _string text,
 _date text,
 _nildate text,
 _ptrdate text,
 _bytea text,
 _time text
);

DROP TABLE IF EXISTS stats_test;

CREATE TABLE stats_test (
  id integer primary key,
  numeric integer,
  value integer
);

DROP TABLE IF EXISTS composite_keys;

CREATE TABLE composite_keys (
  code VARCHAR(255) default '',
  user_id VARCHAR(255) default '',
  some_val VARCHAR(255) default '',
  primary key (code, user_id)
);

COMMIT;
