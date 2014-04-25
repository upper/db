DROP DATABASE IF EXISTS upperio_tests;

CREATE DATABASE upperio_tests;

CREATE USER 'upperio'@'%' IDENTIFIED BY 'upperio';
GRANT ALL PRIVILEGES ON upperio_tests.* to 'upperio'@'%' WITH GRANT OPTION;

