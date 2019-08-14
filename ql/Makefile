SHELL := bash

DB_NAME ?= ql-test.db

export DB_NAME

build:
	go build && go install

require-client:
	@if [ -z "$$(which ql)" ]; then \
		echo 'Missing "ql" command. Please install the QL cli tool and try again.' && \
		exit 1; \
	fi

reset-db: require-client
	rm -f $(DB_NAME)

test: reset-db
	go test -v

test-extended: test
