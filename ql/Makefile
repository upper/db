#BENCHTIME ?= 10s
BENCHTIME := 1s

build:
	go build && go install

require-client:
	if [ -z "$$(which ql)" ]; then \
		echo 'Missing "ql" command. Please install the QL cli tool and try again.' && \
		exit 1; \
	fi

reset-db: require-client
	$(MAKE) reset-db -C _dumps

test: reset-db
	go test -v
	$(MAKE) -C _example

bench: reset-db
	go test -v -bench=. -benchtime=$(BENCHTIME) -benchmem -test.parallel=1 -short -cpu 1
