DROP DATABASE IF EXISTS upperio_tests;

CREATE DATABASE upperio_tests;

GRANT ALL PRIVILEGES ON upperio_tests.* to upperio_tests@localhost IDENTIFIED BY 'upperio_secret';
GRANT ALL PRIVILEGES ON upperio_tests.* to upperio_tests@'10.1.2.1' IDENTIFIED BY 'upperio_secret';
