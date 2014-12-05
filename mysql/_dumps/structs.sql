USE upperio_tests;

DROP TABLE IF EXISTS artist;

CREATE TABLE artist (
  id BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
  PRIMARY KEY(id),
  name VARCHAR(60)
);

DROP TABLE IF EXISTS publication;

CREATE TABLE publication (
  id BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
  PRIMARY KEY(id),
  title VARCHAR(80),
  author_id BIGINT(20)
);

DROP TABLE IF EXISTS review;

CREATE TABLE review (
  id BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
  PRIMARY KEY(id),
  publication_id BIGINT(20),
  name VARCHAR(80),
  comments TEXT,
  created DATETIME
);

DROP TABLE IF EXISTS data_types;

CREATE TABLE data_types (
  id BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
  PRIMARY KEY(id),
  _uint INT(10) UNSIGNED DEFAULT 0,
  _uint8 INT(10) UNSIGNED DEFAULT 0,
  _uint16 INT(10) UNSIGNED DEFAULT 0,
  _uint32 INT(10) UNSIGNED DEFAULT 0,
  _uint64 INT(10) UNSIGNED DEFAULT 0,
  _int INT(10) DEFAULT 0,
  _int8 INT(10) DEFAULT 0,
  _int16 INT(10) DEFAULT 0,
  _int32 INT(10) DEFAULT 0,
  _int64 INT(10) DEFAULT 0,
  _float32 DECIMAL(10,6),
  _float64 DECIMAL(10,6),
  _bool TINYINT(1),
  _string text,
  _date DATETIME NOT NULL,
  _nildate DATETIME NULL,
  _ptrdate DATETIME NULL,
  _time TIME NOT NULL
);

DROP TABLE IF EXISTS stats_test;

CREATE TABLE stats_test (
  id BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT, PRIMARY KEY(id),
	`numeric` INT(10),
	`value` INT(10)
);

DROP TABLE IF EXISTS composite_keys;

CREATE TABLE composite_keys (
  code VARCHAR(255) default '',
  user_id VARCHAR(255) default '',
  some_val VARCHAR(255) default '',
  primary key (code, user_id)
);
