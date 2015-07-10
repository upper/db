all: reset-db

reset-db: clean
	cat structs.sql | sqlite3 gotest.sqlite3.db

clean:
	rm -f gotest.sqlite3.db
