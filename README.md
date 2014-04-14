# upper.io/db

`upper.io/db` is a [Go][2] package that allows developers to store and retrive
data to and from different kinds of databases through the use of adapters that
wrap well supported database drivers.

`upper.io/db` is not an ORM, but you may not need one at all:

```go
// This code works the same for all supported databases.
var people []Person
res = col.Find(db.Cond{"name": "Max"}).Skip(1).Limit(2).Sort("-input")
err = res.All(&people)
```

See the project page, recipes and user documentation at [upper.io/db][1].

[![Build Status](https://travis-ci.org/upper/db.png)](https://travis-ci.org/upper/db)

## Supported databases

* [MongoDB](https://upper.io/db/mongo) via [mgo](http://godoc.org/labix.org/v2/mgo)
* [MySQL](https://upper.io/db/mysql) via [mysql](https://github.com/go-sql-driver/mysql)
* [PostgreSQL](https://upper.io/db/postgresql) via [pq](https://github.com/lib/pq)
* [QL](https://upper.io/db/ql) via [ql](https://github.com/cznic/ql)
* [SQLite3](https://upper.io/db/sqlite) via [go-sqlite3](https://github.com/mattn/go-sqlite3)

## License

> Copyright (c) 2012-2014 José Carlos Nieto, https://menteslibres.net/xiam
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

[1]: https://upper.io/db
[2]: http://golang.org
