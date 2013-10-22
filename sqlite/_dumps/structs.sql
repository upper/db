PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;

CREATE TABLE artist (id integer primery key, name varchar(60));
CREATE TABLE album (id integer primary key, artist_id integer, name varchar(60));
CREATE TABLE tracks (id integer primary key, album_id integer, name varchar(60));

CREATE TABLE data_types (id integer primary key, _uint integer, _uintptr integer, _uint8 integer, _uint16 int, _uint32 int, _uint64 int, _int integer, _int8 integer, _int16 integer, _int32 integer, _int64 integer, _float32 real, _float64 real, _byte integer, _rune integer, _bool integer, _string text, _date text, _bytea text, _time text);

COMMIT;
