TEST_HOST ?= 127.0.0.1
BENCHTIME ?= 10s

build:
	go build && go install

require-client:
	if [ -z "$$(which mongo)" ]; then \
		echo 'Missing "mongo" command. Please install the MongoDB client and try again.' && \
		exit 1; \
	fi

generate:

reset-db: require-client
	$(MAKE) reset-db -C _dumps

test: reset-db
	go test -v

bench: reset-db
	go test -v -test.bench=. -test.benchtime=$(BENCHTIME) -benchmem
