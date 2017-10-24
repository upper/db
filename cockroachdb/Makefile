SHELL := bash

DB_HOST   ?= 127.0.0.1
DB_PORT   ?= 26257

DB_USERNAME ?= upperio_tests
DB_PASSWORD ?= upperio_secret
DB_NAME     ?= upperio_tests

TEST_FLAGS ?=

export DB_HOST
export DB_NAME
export DB_PASSWORD
export DB_PORT
export DB_USERNAME

build:
	go build && go install

require-client:
	@if [ -z "$$(which psql)" ]; then \
		echo 'Missing "psql" command. Please install the PostgreSQL client and try again.' && \
		exit 1; \
	fi

generate:
	go generate && \
	go get -d -t -v ./...

reset-db: require-client
	SQL="" && \
	SQL+="DROP DATABASE IF EXISTS $(DB_NAME)_schema CASCADE;" && \
	SQL+="DROP DATABASE IF EXISTS $(DB_NAME) CASCADE;" && \
	SQL+="DROP USER IF EXISTS $(DB_USERNAME);" && \
	SQL+="CREATE USER $(DB_USERNAME) WITH PASSWORD '$(DB_PASSWORD)';" && \
	SQL+="CREATE DATABASE $(DB_NAME) ENCODING='UTF-8';" && \
	SQL+="CREATE DATABASE $(DB_NAME)_schema ENCODING='UTF-8';" && \
	SQL+="GRANT ALL ON DATABASE $(DB_NAME) TO $(DB_USERNAME);" && \
	SQL+="GRANT ALL ON DATABASE $(DB_NAME)_schema TO $(DB_USERNAME);" && \
	if [ ! -z "$$CI" ]; then \
		psql -Uroot -h$(DB_HOST) -p$(DB_PORT) <<< $$SQL; \
	else \
		PGPASSWORD="$(DB_PASSWORD)" \
		psql -Uroot -h$(DB_HOST) -p$(DB_PORT) <<< $$SQL; \
	fi

test: reset-db generate
	#go test -tags generated -v -race # race: limit on 8192 simultaneously alive goroutines is exceeded, dying
	go test -tags generated -v $(TEST_FLAGS)
