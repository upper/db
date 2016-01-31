TEST_HOST   ?= 127.0.0.1
TEST_PORT   ?= 5432

DB_USERNAME ?= upperio_tests
DB_PASSWORD ?= upperio_secret
DB_NAME     ?= upperio_tests

all: setup reset-db

setup:
	psql -Upostgres -h$(TEST_HOST) -p$(TEST_PORT) < setup.sql

reset-db:
	PGPASSWORD="$(DB_PASSWORD)" psql -U$(DB_USERNAME) $(DB_NAME) -h$(TEST_HOST) -p$(TEST_PORT) < structs.sql
