SHELL                 ?= bash

COCKROACHDB_VERSION   ?= v21.2.2
COCKROACHDB_SUPPORTED ?= $(COCKROACHDB_VERSION) v20.2.18 v19.2.12
PROJECT               ?= upper_cockroachdb_$(COCKROACHDB_VERSION)

DB_HOST               ?= 127.0.0.1
DB_PORT               ?= 26257

DB_NAME               ?= upperio
DB_USERNAME           ?= upperio_user

TEST_FLAGS            ?=
PARALLEL_FLAGS        ?= --halt-on-error 2 --jobs 1

export COCKROACHDB_VERSION

export DB_HOST
export DB_NAME
export DB_PASSWORD
export DB_PORT
export DB_USERNAME

export TEST_FLAGS

test:
	go test -v -failfast -race -timeout 20m $(TEST_FLAGS)

test-no-race:
	go test -v -failfast $(TEST_FLAGS)

server-up: server-down
	docker-compose -p $(PROJECT) up -d && \
	sleep 5 && \
	psql -Uroot -h$(DB_HOST) -p$(DB_PORT) postgres -c "\
		DROP USER IF EXISTS $(DB_USERNAME); \
		DROP DATABASE IF EXISTS $(DB_NAME); \
		CREATE DATABASE $(DB_NAME); \
		CREATE USER $(DB_USERNAME); \
		GRANT ALL ON DATABASE $(DB_NAME) TO $(DB_USERNAME); \
	"

server-down:
	docker-compose -p $(PROJECT) down

test-extended:
	parallel $(PARALLEL_FLAGS) \
		"COCKROACHDB_VERSION={} DB_PORT=\$$(($(DB_PORT)+{#})) $(MAKE) server-up test server-down" ::: \
		$(COCKROACHDB_SUPPORTED)
