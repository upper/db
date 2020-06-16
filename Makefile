SHELL                 := /bin/bash

PARALLEL_FLAGS        ?= --halt-on-error 2 --jobs=4 -v -u

TEST_FLAGS            ?=

export TEST_FLAGS
export PARALLEL_FLAGS

test: go-test-sqlbuilder go-test-internal test-adapters

benchmark: go-benchmark-internal go-benchmark-sqlbuilder

go-benchmark-%:
	go test -v -benchtime=500ms -bench=. ./$*/...

go-test-%:
	go test -v ./$*/...

test-adapters: \
	test-adapter-postgresql \
	test-adapter-cockroachdb \
	test-adapter-mysql \
	test-adapter-mssql \
	test-adapter-sqlite \
	test-adapter-ql \
	test-adapter-mongo

test-adapter-%:
	($(MAKE) -C adapter/$* test-extended || exit 1)

test-generic:
	export TEST_FLAGS="-run TestGeneric"; \
	$(MAKE) test-adapters

goimports:
	for FILE in $$(find -name "*.go" | grep -v vendor); do \
		goimports -w $$FILE; \
	done
