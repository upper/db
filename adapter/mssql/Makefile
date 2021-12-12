SHELL					        ?= bash

MSSQL_VERSION         ?= 2019-latest
MSSQL_SUPPORTED       ?= $(MSSQL_VERSION) 2017-latest
PROJECT               ?= upper_mssql_$(MSSQL_VERSION)

DB_HOST               ?= 127.0.0.1
DB_PORT               ?= 1433

DB_NAME               ?= master
DB_USERNAME           ?= sa
DB_PASSWORD           ?= upperio//s3cr37

TEST_FLAGS            ?=
PARALLEL_FLAGS        ?= --halt-on-error 2 --jobs 1

export MSSQL_VERSION

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
	sleep 20

server-down:
	docker-compose -p $(PROJECT) down

test-extended:
	parallel $(PARALLEL_FLAGS) \
		"MSSQL_VERSION={} DB_PORT=\$$((1433+{#})) $(MAKE) server-up test server-down" ::: \
		$(MSSQL_SUPPORTED)
