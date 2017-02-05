SHELL := /bin/bash

DB_HOST ?= 127.0.0.1

export DB_HOST

benchmark-lib:
	go test -v -benchtime=500ms -bench=. ./lib/...

benchmark-internal:
	go test -v -benchtime=500ms -bench=. ./internal/...

benchmark: benchmark-lib benchmark-internal

test-lib:
	go test -v ./lib/...

test-internal:
	go test -v ./internal/...

test-libs: test-lib test-internal

test-%:
	$(MAKE) -C $* test || exit 1;

test-adapters: test-postgresql test-mysql test-sqlite test-ql test-mongo

reset-db:
	$(MAKE) -C postgresql reset-db && \
	$(MAKE) -C mysql reset-db && \
	$(MAKE) -C sqlite reset-db && \
	$(MAKE) -C ql reset-db && \
	$(MAKE) -C mongo reset-db

test-main: reset-db
	go test -v

test: test-adapters test-libs test-main
