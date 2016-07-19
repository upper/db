SHELL := bash

DB_HOST ?= 127.0.0.1

export DB_HOST

test:
	go test -v ./lib/... && \
	go test -v ./internal/... && \
	$(MAKE) test -C postgresql && \
	$(MAKE) test -C mysql && \
	$(MAKE) test -C sqlite && \
	$(MAKE) test -C ql && \
	$(MAKE) test -C mongo && \
	go test -v
