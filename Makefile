SHELL := /bin/bash

DB_HOST ?= 127.0.0.1

export DB_HOST

test:
	go test -v -benchtime=500ms -bench=. ./lib/... && \
	go test -v -benchtime=500ms -bench=. ./internal/... && \
	for ADAPTER in postgresql mysql sqlite ql mongo; do \
		$(MAKE) -C $$ADAPTER test; \
	done && \
	go test -v
