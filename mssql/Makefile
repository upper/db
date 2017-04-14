SHELL := bash

DB_HOST ?= 127.0.0.1
DB_PORT	?= 1433

DB_USERNAME ?= upperio_tests
DB_PASSWORD ?= upperio_Secre3t
DB_NAME     ?= upperio_tests

export DB_HOST
export DB_NAME
export DB_PASSWORD
export DB_PORT
export DB_USERNAME

build:
	go build && go install

require-client:
	@if [ -z "$$(which tsql)" ]; then \
		echo 'Missing "tsql" command.' && \
		exit 1; \
	fi

generate:
	go generate && \
	go get -d -t -v ./...

reset-db: require-client
	SQL="" && \
	SQL+="USE [master]\n" && \
	SQL+="GO\n" && \
	SQL+="DROP DATABASE IF EXISTS [$(DB_NAME)]\n" && \
	SQL+="CREATE DATABASE [$(DB_NAME)]\n" && \
	SQL+="GO\n" && \
	SQL+="DROP ROLE IF EXISTS [$(DB_USERNAME)]\n" && \
	SQL+="CREATE ROLE [$(DB_USERNAME)]\n" && \
	SQL+="GO\n" && \
	SQL+="USE [$(DB_NAME)]\n" && \
	SQL+="GO\n" && \
	SQL+="DROP USER IF EXISTS [$(DB_USERNAME)]\n" && \
	SQL+="CREATE USER [$(DB_USERNAME)]\n" && \
	SQL+="GO\n" && \
	SQL+="IF EXISTS (SELECT name FROM sys.database_principals WHERE name = '$(DB_USERNAME)') BEGIN DROP LOGIN $(DB_USERNAME) END\n" && \
	SQL+="CREATE LOGIN $(DB_USERNAME) WITH PASSWORD = '$(DB_PASSWORD)'\n" && \
	SQL+="GO\n" && \
	SQL+="EXEC sp_change_users_login 'Update_One', '$(DB_USERNAME)', '$(DB_USERNAME)'\n" && \
	SQL+="EXEC sp_addrolemember 'db_owner', N'$(DB_USERNAME)'\n" && \
	SQL+="GO\n" && \
	echo -ne $$SQL | tsql -H $(DB_HOST) -p $(DB_PORT) -U sa -P 'my$$Password'

test: reset-db generate
	go test -tags generated -v
