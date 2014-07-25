DROP DATABASE IF EXISTS upperio_tests;

CREATE DATABASE upperio_tests;

GRANT ALL PRIVILEGES ON upperio_tests.* to upperio@localhost IDENTIFIED BY 'upperio';
GRANT ALL PRIVILEGES ON upperio_tests.* to upperio@'10.1.2.1' IDENTIFIED BY 'upperio';
