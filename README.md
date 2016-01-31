# upper.io/db [![Build Status](https://travis-ci.org/upper/db.svg?branch=v2)](https://travis-ci.org/upper/db) [![GoDoc](https://godoc.org/upper.io/db?status.svg)](https://godoc.org/upper.io/db.v2)

<center>
<img src="https://upper.io/db.v2/images/icon.svg" width="256" />
</center>

## The `db` package

![Upper.io](https://upper.io/db.v2/res/general.png)

`db` is a [Go][2] package that allows you to communicate with different
databases through special *adapters* that wrap well-supported database drivers.

```
go get -u upper.io/db.v2
```

## Is `db` an ORM?

`db` is not an ORM in the sense that it does not tell you how to design your
application or how to validate your data. Instead of trying to lecture you,
`db` focuses on being a easy to user tool that helps you dealing with common
database operations.

```go
var people []Person

res = col.Find(db.Cond{"name": "Max"}).Limit(10).Sort("-last_name")

err = res.All(&people)
...
```

`db` can be considered a non-opinionated ORM that rather stays out of your way.

## Supported databases

![Adapters](https://upper.io/db.v2/res/adapters.png)

`db` attempts to provide full compatiblity for [CRUD][2] operations across all
its adapters. Some other operations (such *transactions*) are supported only
with specific database adapters, such as MySQL, PostgreSQL and SQLite.

* [MongoDB](https://upper.io/db.v2/mongo) via [mgo](http://godoc.org/labix.org/v2/mgo)
* [MySQL](https://upper.io/db.v2/mysql) via [mysql](https://github.com/go-sql-driver/mysql)
* [PostgreSQL](https://upper.io/db.v2/postgresql) via [pq](https://github.com/lib/pq)
* [QL](https://upper.io/db.v2/ql) via [ql](https://github.com/cznic/ql)
* [SQLite3](https://upper.io/db.v2/sqlite) via [go-sqlite3](https://github.com/mattn/go-sqlite3)

## User documentation

See documentation for users at [upper.io/db.v2][1].

## License

This project is licensed under the terms of the **MIT License**.

> Copyright (c) 2012-2015 The upper.io/db authors. All rights reserved.
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
