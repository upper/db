SHELL                 := /bin/bash

POSTGRESQL_IMAGE      ?= postgres
POSTGRESQL_VERSION    ?= 11
POSTGRESQL_CONTAINER  ?= upper-postgresql
POSTGRESQL_PORT       ?= 5432

MYSQL_IMAGE           ?= mysql
MYSQL_VERSION         ?= 5
MYSQL_CONTAINER       ?= upper-mysql
MYSQL_PORT            ?= 3306

MONGO_IMAGE           ?= mongo
MONGO_VERSION         ?= 3
MONGO_CONTAINER       ?= upper-mongo
MONGO_PORT            ?= 27017

MSSQL_IMAGE           ?= mcr.microsoft.com/mssql/server
MSSQL_VERSION         ?= latest
MSSQL_CONTAINER       ?= upper-mssql
MSSQL_PORT            ?= 1433

DB_USERNAME           ?= upperio_tests
DB_PASSWORD           ?= upperio_secret

BIND_HOST             ?= 0.0.0.0

WRAPPER               ?= all
DB_HOST               ?= 127.0.0.1

TEST_FLAGS            ?=

PARALLEL_FLAGS        ?= --halt 2 -v

export DB_HOST
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

databases:
	parallel $(PARALLEL_FLAGS) -u \
		"$(MAKE) docker-{}" ::: \
			postgresql \
			mysql \
			mssql \
			mongo && \
	sleep 30

docker-mongo:
	docker pull $(MONGO_IMAGE):$(MONGO_VERSION) && \
	(docker rm -f $(MONGO_CONTAINER) || exit 0) && \
	docker run \
		-d \
		--rm \
		-e "MONGO_USER=$(DB_USERNAME)" \
		-e "MONGO_PASSWORD=$(DB_PASSWORD)" \
		-e "MONGO_DATABASE=$(DB_USERNAME)" \
		-p $(BIND_HOST):$(MONGO_PORT):27017 \
		--name $(MONGO_CONTAINER) \
		$(MONGO_IMAGE):$(MONGO_VERSION)

docker-mysql:
	docker pull $(MYSQL_IMAGE):$(MYSQL_VERSION) && \
	(docker rm -f $(MYSQL_CONTAINER) || exit 0) && \
	docker run \
		-d \
		--rm \
		-e "MYSQL_USER=$(DB_USERNAME)" \
		-e "MYSQL_PASSWORD=$(DB_PASSWORD)" \
		-e "MYSQL_ALLOW_EMPTY_PASSWORD=1" \
		-e "MYSQL_DATABASE=$(DB_USERNAME)" \
		-p $(BIND_HOST):$(MYSQL_PORT):3306 \
		--name $(MYSQL_CONTAINER) \
		$(MYSQL_IMAGE):$(MYSQL_VERSION)

docker-mssql:
	docker pull $(MSSQL_IMAGE):$(MSSQL_VERSION) && \
	(docker rm -f $(MSSQL_CONTAINER) || exit 0) && \
	docker run \
		-d \
		--rm \
		-e "ACCEPT_EULA=Y" \
		-e 'SA_PASSWORD=my$$Password' \
		-p $(BIND_HOST):$(MSSQL_PORT):1433 \
		--name $(MSSQL_CONTAINER) \
		$(MSSQL_IMAGE):$(MSSQL_VERSION)

docker-postgresql:
	docker pull $(POSTGRESQL_IMAGE):$(POSTGRESQL_VERSION) && \
	(docker rm -f $(POSTGRESQL_CONTAINER) || exit 0) && \
	docker run \
		-d \
		--rm \
		-e "POSTGRES_USER=$(DB_USERNAME)" \
		-e "POSTGRES_PASSWORD=$(DB_PASSWORD)" \
		-p $(BIND_HOST):$(POSTGRESQL_PORT):5432 \
		--name $(POSTGRESQL_CONTAINER) \
		$(POSTGRESQL_IMAGE):$(POSTGRESQL_VERSION)
