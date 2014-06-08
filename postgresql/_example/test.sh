#!/bin/sh
cat example.sql | PGPASSWORD=upperio psql -Uupperio upperio_tests -htestserver.local
go run main.go
