TEST_HOST ?= 127.0.0.1

all:
	cat structs.sql | PGPASSWORD="upperio" psql -Uupperio upperio_tests -h$(TEST_HOST)
