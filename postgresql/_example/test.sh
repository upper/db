#!/bin/sh
cat example.sql | PGPASSWORD=upperio psql -Uupperio upperio_tests
go run main.go
