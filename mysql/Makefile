SHELL					        := bash

DB_HOST               ?= 127.0.0.1
DB_PORT               ?= 3306

DB_NAME               ?= upperio
DB_USERNAME           ?= upperio_user
DB_PASSWORD           ?= upperio//s3cr37

TEST_FLAGS ?=

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
	SQL="" && \
	SQL+="DROP DATABASE IF EXISTS $(DB_NAME);" && \
	SQL+="CREATE DATABASE $(DB_NAME);" && \
	SQL+="GRANT ALL PRIVILEGES ON $(DB_NAME).* TO $(DB_USERNAME) IDENTIFIED BY '$(DB_PASSWORD)';" && \
	mysql -uroot -h"$(DB_HOST)" -P$(DB_PORT) <<< $$SQL

test: reset-db generate
	#go test -tags generated -v -race # race: limit on 8192 simultaneously alive goroutines is exceeded, dying
	go test -tags generated -v $(TEST_FLAGS)
