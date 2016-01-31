DROP DATABASE IF EXISTS upperio_tests;
CREATE DATABASE upperio_tests;

GRANT ALL PRIVILEGES ON upperio_tests.* to upperio_tests@localhost IDENTIFIED BY 'upperio_secret';
