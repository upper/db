TEST_HOST ?= localhost

all:
	cat structs.sql | mysql -uupperio_tests -pupperio_secret upperio_tests -h$(TEST_HOST)
