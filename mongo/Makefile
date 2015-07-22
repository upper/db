TEST_HOST ?= 127.0.0.1

build:
	go build && go install

test:
	go test -v
	$(MAKE) -C _example

bench:
	go test -v -test.bench=. -test.benchtime=10s -benchmem
