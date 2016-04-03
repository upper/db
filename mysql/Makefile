TEST_HOST ?= 127.0.0.1
BENCHTIME ?= 10s

build:
	go build && go install

require-client:
	if [ -z "$$(which mysql)" ]; then \
		echo 'Missing "mysql" command. Please install the MySQL client and try again.' && \
		exit 1; \
	fi

generate:
	go generate

reset-db: require-client
	$(MAKE) reset-db -C _dumps

test: reset-db generate
	go test -v

bench: reset-db generate
	go test -v -test.bench=. -test.benchtime=$(BENCHTIME) -benchmem
