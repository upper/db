SHELL := bash

DB_HOST ?= 127.0.0.1
DB_PORT	?= 3306

DB_USERNAME ?= upperio_tests
DB_PASSWORD ?= upperio_secret
DB_NAME     ?= upperio_tests

BENCHTIME ?= 5s

MYSQL_ROOT_USERNAME ?=root
MYSQL_ROOT_PASSWORD ?=

export DB_HOST
export DB_NAME
export DB_PASSWORD
export DB_PORT
export DB_USERNAME

build:
	go build && go install

require-client:
	@if [ -z "$$(which mysql)" ]; then \
		echo 'Missing "mysql" command. Please install the MySQL client and try again.' && \
		exit 1; \
	fi

generate:
	go generate && \
	go get -d -t -v ./...

reset-db: require-client
	@if [ -z "$(MYSQL_ROOT_PASSWORD)" ]; then \
	       SQL="" && \
	       SQL+="DROP DATABASE IF EXISTS $(DB_NAME);" && \
	       SQL+="CREATE DATABASE $(DB_NAME);" && \
	       SQL+="GRANT ALL PRIVILEGES ON $(DB_NAME).* TO $(DB_USERNAME) IDENTIFIED BY '$(DB_PASSWORD)';" && \
	       mysql -u$(MYSQL_ROOT_USERNAME) -h$(DB_HOST) -P$(DB_PORT) <<< $$SQL; \
	else \
	       SQL="" && \
	       SQL+="DROP DATABASE IF EXISTS $(DB_NAME);" && \
	       SQL+="CREATE DATABASE $(DB_NAME);" && \
	       SQL+="GRANT ALL PRIVILEGES ON $(DB_NAME).* TO $(DB_USERNAME) IDENTIFIED BY '$(DB_PASSWORD)';" && \
	       mysql -u$(MYSQL_ROOT_USERNAME) -p$(MYSQL_ROOT_PASSWORD) -h$(DB_HOST) -P$(DB_PORT) <<< $$SQL; \
	fi

test: reset-db generate
	go test -v -race

bench: reset-db generate
	go test -v -test.bench=. -test.benchtime=$(BENCHTIME) -benchmem
