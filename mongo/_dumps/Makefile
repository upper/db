TEST_HOST ?= 127.0.0.1
BENCHTIME ?= 10s

DB_USERNAME ?= upperio_tests
DB_PASSWORD ?= upperio_secret
DB_NAME     ?= upperio_tests

reset-db:
	mongo $(DB_NAME) --eval 'db.dropDatabase()' --host $(TEST_HOST) && \
	$(MAKE) setup

setup:
	# Waiting for official Travis CI support for MongoDB 3.0
	mongo upperio_tests --eval 'db.dropUser("$(DB_USERNAME)")' --host $(TEST_HOST) && \
	mongo upperio_tests --eval 'db.createUser({user: "$(DB_USERNAME)", pwd: "$(DB_PASSWORD)", roles: [{role: "readWrite", db: "$(DB_NAME)"}]})' --host $(TEST_HOST)
	#mongo upperio_tests --eval 'db.addUser("upperio_tests", "upperio_secret")' --host $(TEST_HOST)
