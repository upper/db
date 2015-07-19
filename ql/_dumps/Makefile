DB_NAME     ?= test.db

load: clean
	cat structs.sql | $$GOPATH/bin/ql -db $(DB_NAME)

clean:
	rm -f $(DB_NAME)
