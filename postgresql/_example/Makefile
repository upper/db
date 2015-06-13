TEST_HOST   ?= 127.0.0.1
TEST_PORT   ?= 5432

DB_USERNAME ?= upperio_tests
DB_PASSWORD ?= upperio_secret
DB_NAME     ?= upperio_tests

test:
	cat example.sql | PGPASSWORD="$(DB_PASSWORD)" psql -U$(DB_USERNAME) $(DB_NAME) -h$(TEST_HOST) -p$(TEST_PORT)
	go run -v main.go
