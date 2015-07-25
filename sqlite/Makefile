BENCHTIME ?= 10s

build:
	go build && go install

require-client:
	if [ -z "$$(which sqlite3)" ]; then \
		echo 'Missing "sqlite3" command. Please install SQLite3 and try again.' && \
		exit 1; \
	fi

reset-db: require-client
	$(MAKE) reset-db -C _dumps

test: reset-db
	go test -v
	$(MAKE) -C _example

bench: reset-db
	go test -v -test.bench=. -test.benchtime=$(BENCHTIME) -benchmem
