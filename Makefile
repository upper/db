UPPER_DB_LOG          ?= ERROR

export UPPER_DB_LOG

bench:
	go test -v -benchtime=500ms -bench=. ./internal/...

test:
	$(MAKE) -C tests
