SHELL := /bin/bash

DB_HOST ?= 127.0.0.1

export DB_HOST

test-lib:
	go test -v -benchtime=500ms -bench=. ./lib/...

test-internal:
	go test -v -benchtime=500ms -bench=. ./internal/...

test-%:
	$(MAKE) -C $* test || exit 1;

test: test-lib test-internal test-postgresql test-mysql test-sqlite test-ql test-mongo
	go test -v
