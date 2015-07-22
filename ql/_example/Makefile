DB_NAME     ?= example.db

test:
	rm -f $(DB_NAME)
	cat example.sql | $$GOPATH/bin/ql -db $(DB_NAME)
	go run -v main.go
