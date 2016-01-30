TEST_HOST ?=localhost

all:
	mongo upperio_tests --eval 'db.dropUser("upperio_tests")' --host $(TEST_HOST)
	mongo upperio_tests --eval 'db.createUser({user: "upperio_tests", pwd: "upperio_secret", roles: [{role: "readWrite", db: "upperio_tests"}]})' --host $(TEST_HOST)
