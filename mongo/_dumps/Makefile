all: setup

require-client:
	if [ -z "$(which mongo)" ]; then \
		echo 'Missing "mongo" command. Please install the MongoDB client and try again.' && \
		exit 1; \
	fi

setup: require-client
	mongo upperio_tests --eval 'db.addUser("upperio_tests", "upperio_secret")'
