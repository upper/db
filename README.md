# upper.io/db.v2 [![Build Status](https://travis-ci.org/upper/db.svg?branch=v2)](https://travis-ci.org/upper/db) [![GoDoc](https://godoc.org/upper.io/db.v2?status.svg)](https://godoc.org/upper.io/db.v2)

<center>
<img src="http://beta.upper.io/db.v2/images/gopher.svg" width="256" />
</center>

## The `db.v2` package

The `upper.io/db.v2` package for [Go][2]  is a productive database access layer
for Go that provides a common interface to work with different data sources
such as PostgreSQL, MySQL, SQLite, QL and MongoDB.

```
go get upper.io/db.v2
```

## User documentation

This is the source code repository, see examples and documentation at
[beta.upper.io/db.v2][1].

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

```
go run _examples/booktown-books/main.go
2016/05/23 18:08:03 Book 0: main.Book{ID:7808, Title:"The Shining", AuthorID:4156, SubjectID:9}
2016/05/23 18:08:03 Book 1: main.Book{ID:4513, Title:"Dune", AuthorID:1866, SubjectID:15}
2016/05/23 18:08:03 Book 2: main.Book{ID:4267, Title:"2001: A Space Odyssey", AuthorID:2001, SubjectID:15}
2016/05/23 18:08:03 Book 3: main.Book{ID:1608, Title:"The Cat in the Hat", AuthorID:1809, SubjectID:2}
2016/05/23 18:08:03 Book 4: main.Book{ID:1590, Title:"Bartholomew and the Oobleck", AuthorID:1809, SubjectID:2}
2016/05/23 18:08:03 Book 5: main.Book{ID:25908, Title:"Franklin in the Dark", AuthorID:15990, SubjectID:2}
2016/05/23 18:08:03 Book 6: main.Book{ID:1501, Title:"Goodnight Moon", AuthorID:2031, SubjectID:2}
2016/05/23 18:08:03 Book 7: main.Book{ID:190, Title:"Little Women", AuthorID:16, SubjectID:6}
2016/05/23 18:08:03 Book 8: main.Book{ID:1234, Title:"The Velveteen Rabbit", AuthorID:25041, SubjectID:3}
2016/05/23 18:08:03 Book 9: main.Book{ID:2038, Title:"Dynamic Anatomy", AuthorID:1644, SubjectID:0}
2016/05/23 18:08:03 Book 10: main.Book{ID:156, Title:"The Tell-Tale Heart", AuthorID:115, SubjectID:9}
2016/05/23 18:08:03 Book 11: main.Book{ID:41473, Title:"Programming Python", AuthorID:7805, SubjectID:4}
2016/05/23 18:08:03 Book 12: main.Book{ID:41477, Title:"Learning Python", AuthorID:7805, SubjectID:4}
2016/05/23 18:08:03 Book 13: main.Book{ID:41478, Title:"Perl Cookbook", AuthorID:7806, SubjectID:4}
2016/05/23 18:08:03 Book 14: main.Book{ID:41472, Title:"Practical PostgreSQL", AuthorID:1212, SubjectID:4}
```

## License

This project is licensed under the terms of the **MIT License**.

> Copyright (c) 2012-present The upper.io/db authors. All rights reserved.
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

[1]: http://beta.upper.io/db.v2
[2]: http://golang.org
[3]: http://en.wikipedia.org/wiki/Create,_read,_update_and_delete
