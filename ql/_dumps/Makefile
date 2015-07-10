all: reset-db

reset-db:
	rm -f test.db
	cat structs.sql | $$GOPATH/bin/ql -db test.db
