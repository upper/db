TEST_HOST ?= 127.0.0.1
BENCHTIME ?= 2s

build:
	go build && go install

require-client:
	if [ -z "$$(which psql)" ]; then \
		echo 'Missing "psql" command. Please install the PostgreSQL client and try again.' && \
		exit 1; \
	fi

generate:
	go generate

reset-db: require-client
	$(MAKE) reset-db -C _dumps

test: reset-db generate
	go test -v
	$(MAKE) -C _example

bench: reset-db generate
	go test -v -test.bench=. -test.benchtime=$(BENCHTIME) -benchmem
