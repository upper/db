TEST_HOST ?= 127.0.0.1
BENCHTIME ?= 10s

all: setup

setup:
	# Waiting for official Travis CI support for MongoDB 3.0
	#mongo upperio_tests --eval 'db.dropUser("upperio_tests")' --host $$TEST_HOST
	#mongo upperio_tests --eval 'db.createUser({user: "upperio_tests", pwd: "upperio_secret", roles: [{role: "readWrite", db: "upperio_tests"}]})' --host $$TEST_HOST
	mongo upperio_tests --eval 'db.addUser("upperio_tests", "upperio_secret")' --host $(TEST_HOST)
