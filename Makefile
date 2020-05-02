SHELL                 := /bin/bash

PARALLEL_FLAGS        ?= --halt now,fail=1 --jobs=2 -v -u

TEST_FLAGS            ?=

export TEST_FLAGS
export PARALLEL_FLAGS

test: go-test-sqlbuilder go-test-internal test-adapters

benchmark: go-benchmark-internal go-benchmark-sqlbuilder

go-benchmark-%:
	go test -v -benchtime=500ms -bench=. ./$*/...

go-test-%:
	go test -v ./$*/...

test-adapters:
	for MAKEFILE in $$(grep -Rl test-extended adapters/*/Makefile | sort -u); do \
		ADAPTER=$$(basename $$(dirname $$MAKEFILE)); \
		($(MAKE) test-adapter-$$ADAPTER || exit 1); \
	done

test-adapter-%:
	($(MAKE) -C adapters/$* test-extended || exit 1)

test-generic:
	export TEST_FLAGS="-run TestGeneric"; \
	$(MAKE) test-adapters

goimports:
	for FILE in $$(find -name "*.go" | grep -v vendor); do \
		goimports -w $$FILE; \
	done
