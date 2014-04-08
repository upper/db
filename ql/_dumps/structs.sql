BEGIN TRANSACTION;

DROP TABLE IF EXISTS artist;

CREATE TABLE artist (
  id int,
  name string
);

DROP TABLE IF EXISTS data_types;

CREATE TABLE data_types (
  id int,
  _uint int,
  _uint8 int,
  _uint16 int,
  _uint32 int,
  _uint64 int,
  _int int,
  _int8 int,
  _int16 int,
  _int32 int,
  _int64 int,
  _float32 float32,
  _float64 float64,
  _bool bool,
  _string string,
  _date time,
  _time time
);

COMMIT;
