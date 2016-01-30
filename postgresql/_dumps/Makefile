TEST_HOST ?= localhost

all:
	cat structs.sql | PGPASSWORD="upperio_secret" psql -Uupperio_tests upperio_tests -h$(TEST_HOST)
