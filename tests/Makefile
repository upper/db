TEST_FLAGS  ?=

GODEBUG    ?=x509negativeserial=1

export GODEBUG

all: test-all

venv:
	$(MAKE) -C ansible venv

test: venv
	go test \
		-failfast \
		-timeout 30m \
		-race \
		-v \
		$(TEST_FLAGS)

test-all:
	TEST_FLAGS="-run ." $(MAKE) test
