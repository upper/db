SHELL                 ?= bash

DB_NAME               ?= memory://virtual.db
TEST_FLAGS            ?=

export DB_NAME

build:
	go build && go install

# Temporarily disabled due to https://gitlab.com/cznic/ql/-/issues/225
#test:
#	go test -v -race -timeout 20m $(TEST_FLAGS)

test: test-no-race

test-no-race:
	go test -v -failfast $(TEST_FLAGS)

test-extended: test
