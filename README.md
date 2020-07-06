<p align="center">
  <img src="https://upper.io/db.v3/images/gopher.svg" width="256" />
</p>

# upper/db [![Build Status](https://travis-ci.org/upper/db.svg?branch=v4)](https://travis-ci.org/upper/db) [![GoDoc](https://godoc.org/github.com/upper/db?status.svg)](https://godoc.org/github.com/upper/db)

`upper/db` is a productive data access layer (DAL) for [Go](https://golang.org)
that provides agnostic tools to work with different data sources, such as:
* PostgreSQL
* MySQL
* MSSQL
* CockroachDB
* MongoDB
* QL
* SQLite

## Install

```
go get github.com/upper/db/v4
```

## The tour

![screen shot 2017-05-01 at 19 23 22](https://cloud.githubusercontent.com/assets/385670/25599675/b6fe9fea-2ea3-11e7-9f76-002931dfbbc1.png)

Take the [tour](https://tour.upper.io) to see real live examples in your
browser.

## Live demos

You can run the following example on our [playground](https://demo.upper.io):

```go
package main

import (
	"log"

	"github.com/upper/db/v4/adapter/postgresql"
)

var settings = postgresql.ConnectionURL{
	Host:     "demo.upper.io",
	Database: "booktown",
	User:     "demouser",
	Password: "demop4ss",
}

type Book struct {
	ID        int    `db:"id"`
	Title     string `db:"title"`
	AuthorID  int    `db:"author_id"`
	SubjectID int    `db:"subject_id"`
}

func main() {
	sess, err := postgresql.Open(settings)
	if err != nil {
		log.Fatalf("db.Open(): %q\n", err)
	}
	defer sess.Close()

	var books []Book
	err = sess.Collection("books").Find().All(&books)
	if err != nil {
		log.Fatalf("Find(): %q\n", err)
	}

	for i, book := range books {
		log.Printf("Book %d: %#v\n", i, book)
	}
}
```

## License

Licensed under [MIT License](./LICENSE)

## Contributors

See the [list of contributors](https://github.com/upper/db/graphs/contributors).
