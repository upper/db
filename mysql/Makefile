TEST_HOST ?= 127.0.0.1

build:
	go build && go install

reset-db:
	$(MAKE) reset-db -C _dumps

test: reset-db
	go test -v
	$(MAKE) -C _example

bench: reset-db
	go test -v -test.bench=. -test.benchtime=10s -benchmem
