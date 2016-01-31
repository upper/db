all: reset-db

reset-db: clean
	sqlite3 gotest.sqlite3.db < structs.sql

clean:
	rm -f gotest.sqlite3.db
