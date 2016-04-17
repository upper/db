SHELL := bash

DB_NAME ?= ql-test.db

BENCHTIME ?= 5s

export DB_NAME

build:
	go build && go install

require-client:
	@if [ -z "$$(which ql)" ]; then \
		echo 'Missing "ql" command. Please install the QL cli tool and try again.' && \
		exit 1; \
	fi

generate:
	go generate && \
	go get -d -t -v ./...

reset-db: require-client
	rm -f $(DB_NAME)

test: reset-db generate
	go test -v

bench: reset-db generate
	go test -v -bench=. -benchtime=$(BENCHTIME) -benchmem -test.parallel=1 -short -cpu 1
