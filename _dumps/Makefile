TEST_HOST ?= 127.0.0.1
BENCHTIME ?= 10s

all: setup

setup:
	mongo upperio_tests --eval 'db.dropUser("upperio_tests")' --host $$TEST_HOST
	mongo upperio_tests --eval 'db.createUser({user: "upperio_tests", pwd: "upperio_secret", roles: [{role: "readWrite", db: "upperio_tests"}]})' --host $$TEST_HOST
