#!/bin/sh
rm -f example.db
cat example.sql | sqlite3 example.db
go run main.go
