DROP DATABASE IF EXISTS upperio_tests;

CREATE DATABASE upperio_tests;

GRANT ALL ON upperio_tests.* to upperio@'%' identified by 'upperio';
