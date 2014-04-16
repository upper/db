#!/bin/sh
rm -f example.db
cat example.sql | ql -db example.db
go run main.go
