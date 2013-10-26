DROP TABLE IF EXISTS artist;

CREATE TABLE artist (
  id serial primary key,
  name VARCHAR(60)
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
  _time time without time zone
);
