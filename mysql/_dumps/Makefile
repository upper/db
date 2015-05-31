TEST_HOST ?= 127.0.0.1

all:
	cat structs.sql | mysql -uupperio -pupperio upperio_tests -h$(TEST_HOST)
