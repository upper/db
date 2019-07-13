SHELL                 := /bin/bash

MONGO_VERSION         ?= 3
MYSQL_VERSION         ?= 5
POSTGRES_VERSION      ?= 11
MSSQL_VERSION         ?= 2017-latest-ubuntu

DB_NAME               ?= upperio
DB_USERNAME           ?= upperio_user
DB_PASSWORD           ?= upperio//s3cr37

BIND_HOST             ?= 127.0.0.1

WRAPPER               ?= all
DB_HOST               ?= 127.0.0.1

TEST_FLAGS            ?=

PARALLEL_FLAGS        ?= --halt 2 -v

export MONGO_VERSION
export MYSQL_VERSION
export POSTGRES_VERSION
export MSSQL_VERSION

export DB_USERNAME
export DB_PASSWORD
export DB_NAME
export DB_HOST

export BIND_HOST
export WRAPPER

test: reset-db test-libs test-main test-adapters

benchmark-lib:
	go test -v -benchtime=500ms -bench=. ./lib/...

benchmark-internal:
	go test -v -benchtime=500ms -bench=. ./internal/...

benchmark: benchmark-lib benchmark-internal

test-lib:
	go test -v ./lib/...

test-internal:
	go test -v ./internal/...

test-libs:
	parallel $(PARALLEL_FLAGS) -u \
		"$(MAKE) test-{}" ::: \
			lib \
			internal

test-adapters:
	parallel $(PARALLEL_FLAGS) -u \
		"$(MAKE) test-adapter-{}" ::: \
			postgresql \
			mysql \
			sqlite \
			mssql \
			ql \
			mongo

test-main:
	go test $(TEST_FLAGS) -v ./tests/...

reset-db:
	parallel $(PARALLEL_FLAGS) \
		"$(MAKE) reset-db-{}" ::: \
			postgresql \
			mysql \
			sqlite \
			mssql \
			ql \
			mongo

test-adapter-%:
	($(MAKE) -C $* test || exit 1)

reset-db-%:
	($(MAKE) -C $* reset-db || exit 1)

db-up:
	docker-compose -p upper up -d && \
	sleep 15

db-down:
	docker-compose -p upper down
