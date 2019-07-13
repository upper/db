SHELL                 := bash

DB_HOST               ?= 127.0.0.1
DB_PORT               ?= 1433

DB_NAME               ?= upperio
DB_USERNAME           ?= upperio_user
DB_PASSWORD           ?= upperio//s3cr37

DB_SA_USERNAME        ?= sa

export DB_HOST
export DB_NAME
export DB_PASSWORD
export DB_PORT
export DB_USERNAME

export DB_SA_USERNAME

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
	SQL+="USE [master]\nGO\n" && \
	SQL+="IF EXISTS (SELECT name FROM sys.databases WHERE name = '$(DB_NAME)') BEGIN DROP DATABASE [$(DB_NAME)] END\nGO\n" && \
	SQL+="CREATE DATABASE [$(DB_NAME)]\nGO\n" && \
	SQL+="DROP ROLE IF EXISTS [$(DB_USERNAME)]\nGO\n" && \
	SQL+="CREATE ROLE [$(DB_USERNAME)]\nGO\n" && \
	SQL+="USE [$(DB_NAME)]\nGO\n" && \
	SQL+="IF NOT EXISTS (SELECT name FROM sys.server_principals WHERE name = '$(DB_USERNAME)') BEGIN CREATE LOGIN $(DB_USERNAME) WITH PASSWORD = '$(DB_PASSWORD)' END\nGO\n" && \
	SQL+="IF NOT EXISTS (SELECT name FROM sys.database_principals WHERE name = '$(DB_USERNAME)') BEGIN CREATE USER $(DB_USERNAME) END\nGO\n" && \
	SQL+="EXEC sp_addrolemember 'db_owner', N'$(DB_USERNAME)'\nGO\n" && \
	SQL+="EXEC sp_change_users_login 'Update_One', '$(DB_USERNAME)', '$(DB_USERNAME)'\nGO\n" && \
	echo -ne $$SQL | tsql -H $(DB_HOST) -p $(DB_PORT) -U $(DB_SA_USERNAME) -P '$(DB_PASSWORD)'

test: reset-db generate
	go test -tags generated -v
