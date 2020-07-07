SHELL ?= bash

DB_NAME ?= sqlite3-test.db

export DB_NAME

build:
	go build && go install

require-client:
	@if [ -z "$$(which sqlite3)" ]; then \
		echo 'Missing "sqlite3" command. Please install SQLite3 and try again.' && \
		exit 1; \
	fi

reset-db: require-client
	rm -f $(DB_NAME)

test: reset-db
	go test -v

test-extended: test
