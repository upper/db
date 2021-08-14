SHELL                 ?= bash
DB_NAME               ?= sqlite3-test.db
TEST_FLAGS            ?=

export DB_NAME

export TEST_FLAGS

build:
	go build && go install

require-client:
	@if [ -z "$$(which sqlite3)" ]; then \
		echo 'Missing "sqlite3" command. Please install SQLite3 and try again.' && \
		exit 1; \
	fi

reset-db: require-client
	rm -f $(DB_NAME)

test: reset-db
	go test -v -failfast -race -timeout 20m $(TEST_FLAGS)

test-no-race:
	go test -v -failfast $(TEST_FLAGS)

test-extended: test
