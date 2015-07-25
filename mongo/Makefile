TEST_HOST ?= 127.0.0.1
BENCHTIME ?= 10s

build:
	go build && go install

require-client:
	if [ -z "$$(which mongo)" ]; then \
		echo 'Missing "mongo" command. Please install the MongoDB client and try again.' && \
		exit 1; \
	fi

reset-db: require-client

test: reset-db
	go test -v
	$(MAKE) -C _example

bench: reset-db
	go test -v -test.bench=. -test.benchtime=$(BENCHTIME) -benchmem
