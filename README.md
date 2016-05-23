# upper.io/db [![Build Status](https://travis-ci.org/upper/db.svg?branch=v2)](https://travis-ci.org/upper/db) [![GoDoc](https://godoc.org/upper.io/db.v2?status.svg)](https://godoc.org/upper.io/db.v2)

<center>
<img src="http://beta.upper.io/db.v2/images/gopher.svg" width="256" />
</center>

## The `db.v2` package

![upper.io](http://beta.upper.io/db.v2/res/general.png)

The `upper.io/db.v2` package for [Go][2]  is a non-opinionated database access
layer for Go that provides a common interface to work with different data
sources such as PostgreSQL, MySQL, SQLite, QL and MongoDB.

## User documentation

This is the source code repository, see examples and documentation at
[upper.io/db.v2][1].

## Demo

```go
package main

import (
	"log"

	"upper.io/db.v2/postgresql"
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

	booksCol := sess.Collection("books")

	var books []Book
	err = booksCol.Find().All(&books)
	if err != nil {
		log.Fatalf("Find(): %q\n", err)
	}

	for i, book := range books {
		log.Printf("Book %d: %#v\n", i, book)
	}
}
```

## License

This project is licensed under the terms of the **MIT License**.

> Copyright (c) 2012-2016 The upper.io/db authors. All rights reserved.
>
> Permission is hereby granted, free of charge, to any person obtaining
> a copy of this software and associated documentation files (the
> "Software"), to deal in the Software without restriction, including
> without limitation the rights to use, copy, modify, merge, publish,
> distribute, sublicense, and/or sell copies of the Software, and to
> permit persons to whom the Software is furnished to do so, subject to
> the following conditions:
>
> The above copyright notice and this permission notice shall be
> included in all copies or substantial portions of the Software.
>
> THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
> EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
> MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
> NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
> LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
> OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
> WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

## Authors and contributors

* José Carlos Nieto <jose.carlos@menteslibres.net>
* Peter Kieltyka <peter@pressly.com>
* Maciej Lisiewski <maciej.lisiewski@gmail.com>
* Paul Xue <paul.xue@pressly.com>
* Max Hawkins <maxhawkins@google.com>
* Kevin Darlington <kdarlington@gmail.com>
* icattlecoder <icattlecoder@gmail.com>
* Lars Buitinck <l.buitinck@esciencecenter.nl>
* wei2912 <wei2912_support@hotmail.com>
* rjmcguire <rjmcguire@gmail.com>
* achun <achun.shx@qq.com>
* Piotr "Orange" Zduniak <piotr@zduniak.net>
* Max Hawkins <maxhawkins@gmail.com>
* Julien Schmidt <github@julienschmidt.com>
* Hiram J. Pérez <worg@linuxmail.org>
* Aaron <aaron.l.france@gmail.com>

[1]: https://upper.io/db.v2
[2]: http://golang.org
[3]: http://en.wikipedia.org/wiki/Create,_read,_update_and_delete
