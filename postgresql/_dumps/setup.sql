DROP DATABASE IF EXISTS upperio_tests;

DROP ROLE IF EXISTS upperio_tests;

CREATE USER upperio_tests WITH PASSWORD 'upperio_secret';

CREATE DATABASE upperio_tests ENCODING 'UTF-8' LC_COLLATE='en_US.UTF-8' LC_CTYPE='en_US.UTF-8' TEMPLATE template0;

GRANT ALL PRIVILEGES ON DATABASE upperio_tests TO upperio_tests;
