DROP TABLE IF EXISTS artist;

CREATE TABLE artist (
  id serial primary key,
  name varchar(60)
);

DROP TABLE IF EXISTS publication;

CREATE TABLE publication (
  id serial primary key,
  title varchar(80),
  author_id integer
);

DROP TABLE IF EXISTS review;

CREATE TABLE review (
  id serial primary key,
  publication_id integer,
  name varchar(80),
  comments text,
  created timestamp without time zone
);

DROP TABLE IF EXISTS data_types;

CREATE TABLE data_types (
  id serial primary key,
  _uint integer,
  _uint8 integer,
  _uint16 integer,
  _uint32 integer,
  _uint64 integer,
  _int integer,
  _int8 integer,
  _int16 integer,
  _int32 integer,
  _int64 integer,
  _float32 numeric(10,6),
  _float64 numeric(10,6),
  _bool boolean,
  _string text,
  _date timestamp without time zone,
  _nildate timestamp without time zone null,
  _ptrdate timestamp without time zone,
  _time time without time zone
);

DROP TABLE IF EXISTS stats_test;

CREATE TABLE stats_test (
  id serial primary key,
  numeric integer,
  value integer
);

DROP TABLE IF EXISTS composite_keys;

CREATE TABLE composite_keys (
  code varchar(255) default '',
  user_id varchar(255) default '',
  some_val varchar(255) default '',
  primary key (code, user_id)
);
