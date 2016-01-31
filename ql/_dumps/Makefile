DB_NAME     ?= test.db

reset-db: clean
	$$GOPATH/bin/ql -db $(DB_NAME) < structs.sql

clean:
	rm -f $(DB_NAME)
