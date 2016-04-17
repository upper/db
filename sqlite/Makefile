SHELL := bash

DB_NAME ?= sqlite3-test.db

BENCHTIME ?= 5s

export DB_NAME

build:
	go build && go install

require-client:
	@if [ -z "$$(which sqlite3)" ]; then \
		echo 'Missing "sqlite3" command. Please install SQLite3 and try again.' && \
		exit 1; \
	fi

generate:
	go generate && \
	go get -d -t -v ./...

reset-db: require-client
	rm -f $(DB_NAME)

test: reset-db generate
	go test -v -race

bench: reset-db generate
	go test -v -test.bench=. -test.benchtime=$(BENCHTIME) -benchmem
