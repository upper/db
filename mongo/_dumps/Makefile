TEST_HOST ?= 127.0.0.1
BENCHTIME ?= 10s

all: setup

setup:
	mongo upperio_tests --eval 'db.addUser("upperio_tests", "upperio_secret")' --host $(TEST_HOST)
