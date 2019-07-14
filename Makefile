SHELL                 := /bin/bash

PARALLEL_FLAGS        ?= --halt now,fail=1 --jobs=2 -v -u

TEST_FLAGS            ?=

export TEST_FLAGS
export PARALLEL_FLAGS

test: test-libs test-adapters

benchmark-lib:
	go test -v -benchtime=500ms -bench=. ./lib/...

benchmark-internal:
	go test -v -benchtime=500ms -bench=. ./internal/...

benchmark: benchmark-lib benchmark-internal

test-lib:
	go test -v ./lib/...

test-internal:
	go test -v ./internal/...

test-libs:
	parallel $(PARALLEL_FLAGS) \
		"$(MAKE) test-{}" ::: \
			lib \
			internal

test-adapters:
	for MAKEFILE in $$(grep -Rl test-extended */Makefile | sort -u); do \
		ADAPTER=$$(dirname $$MAKEFILE); \
		($(MAKE) test-adapter-$$ADAPTER || exit 1); \
	done

test-adapter-%:
	($(MAKE) -C $* test-extended || exit 1)

test-generic:
	export TEST_FLAGS="-run TestGeneric"; \
	$(MAKE) test-adapters

goimports:
	for FILE in $$(find -name "*.go" | grep -v vendor); do \
		goimports -w $$FILE; \
	done
