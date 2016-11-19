SHELL := /bin/bash

DB_HOST ?= 127.0.0.1

export DB_HOST

test:
	go test -v -benchtime=500ms -bench=. ./lib/... && \
	go test -v -benchtime=500ms -bench=. ./internal/... && \
	for ADAPTER in mysql; do \
		$(MAKE) -C $$ADAPTER test; \
	done && \
	WRAPPER=mysql go test -v
