# upper.io/db.v1

<center>
<img src="https://upper.io/images/icon.svg" width="256" />
</center>

[![Build Status](https://travis-ci.org/upper/db.svg?branch=v1)](https://travis-ci.org/upper/db)

## The `db` package

![Upper.io](https://upper.io/db.v1/res/general.png)

`upper.io/db.v1` is a [Go][2] package that allows developers to communicate
with different databases through the use of *adapters* that wrap well-supported
database drivers.

```
go get -u upper.io/db.v1
```

## Is `db` an ORM?

`db` is not an ORM in the sense that it does not tell you how to design your
software or how to validate your data, instead it only focuses on being a tool
that deals with common operations on different databases:

```go
// This code works the same for all supported databases.
var people []Person

res = col.Find(db.Cond{"name": "Max"}).Limit(10).Sort("-last_name")

err = res.All(&people)
```

In strict sense `upper.io/db.v1` could be considered a non-opinionated ORM that
rather stays out of the way.

## Supported databases

![Adapters](https://upper.io/db.v1/res/adapters.png)

`upper.io/db.v1` attempts to provide full compatiblity for [CRUD][2] operations
across adapters. Some other operations (such *transactions*) are supported only
on specific database adapters, such as MySQL, PostgreSQL and SQLite.

* [MongoDB](https://upper.io/db.v1/mongo) via [mgo](http://godoc.org/labix.org/v2/mgo)
* [MySQL](https://upper.io/db.v1/mysql) via [mysql](https://github.com/go-sql-driver/mysql)
* [PostgreSQL](https://upper.io/db.v1/postgresql) via [pq](https://github.com/lib/pq)
* [QL](https://upper.io/db.v1/ql) via [ql](https://github.com/cznic/ql)
* [SQLite3](https://upper.io/db.v1/sqlite) via [go-sqlite3](https://github.com/mattn/go-sqlite3)

## User documentation

See the project page, recipes and user documentation at [upper.io/db.v1][1].

## License

This project is licensed under the terms of the **MIT License**.

> Copyright (c) 2012-2016 The upper.io/db.v1 authors. All rights reserved.
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

## Authors

* [José Carlos Nieto](https://github.com/xiam)
* [Peter Kieltyka](https://github.com/pkieltyka)

[1]: https://upper.io/db.v1
[2]: http://golang.org
[3]: http://en.wikipedia.org/wiki/Create,_read,_update_and_delete
