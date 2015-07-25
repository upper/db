TEST_HOST ?= 127.0.0.1
BENCHTIME ?= 10s

build:
	go build && go install

require-client:
	if [ -z "$$(which psql)" ]; then \
		echo 'Missing "psql" command. Please install the PostgreSQL client and try again.' && \
		exit 1; \
	fi

reset-db: require-client
	$(MAKE) reset-db -C _dumps

test: reset-db
	go test -v
	$(MAKE) -C _example

bench: reset-db
	go test -v -test.bench=. -test.benchtime=$(BENCHTIME) -benchmem
