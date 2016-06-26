# upper.io/db.v2 [![Build Status](https://travis-ci.org/upper/db.svg?branch=v2)](https://travis-ci.org/upper/db) [![GoDoc](https://godoc.org/upper.io/db.v2?status.svg)](https://godoc.org/upper.io/db.v2)

<center>
<img src="https://upper.io/db.v2/images/gopher.svg" width="256" />
</center>

## The `db.v2` package

The `upper.io/db.v2` package for [Go][2]  it's *not* an ORM, just a productive
data access layer for Go which provides a common interface to work with
different data sources such as [PostgreSQL](https://upper.io/db.v2/postgresql),
[MySQL](https://upper.io/db.v2/mysql), [SQLite](https://upper.io/db.v2/sqlite),
[QL](https://upper.io/db.v2/ql) and [MongoDB](https://upper.io/db.v2/mongodb).

```
go get upper.io/db.v2
```

## User documentation

This is the source code repository, see examples and documentation at
[upper.io/db.v2][1].

This is not the most recent version of our source code, if you're looking for
`v2`'s source check out the [v2 branch](https://github.com/upper/db/tree/v2).

## Looking for v1?

We're phasing out `v1`, if you're a new user we encourage you to use [v2][1]
instead.

Please note that the old import path `upper.io/db` will keep on working for a
while, but we'll remove it eventually in favor of the versioned import path:
`upper.io/db.v2` or `upper.io/db.v1`.

Nevertheless, v1's docs could still be consulted at https://upper.io/db.v1.

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

## Authors & contributors

* [José Carlos Nieto](https://github.com/xiam)
* [Peter Kieltyka](https://github.com/pkieltyka)
* [Maciej Lisiewski](maciej.lisiewski@gmail.com)
* [Max Hawkins](maxhawkins@google.com)
* [Paul Xue](paul.xue@pressly.com)
* [Kevin Darlington](kdarlington@gmail.com)
* [Lars Buitinck](l.buitinck@esciencecenter.nl)
* [icattlecoder](icattlecoder@gmail.com)
* [Aaron](aaron.l.france@gmail.com)
* [Hiram J. Pérez](worg@linuxmail.org)
* [Julien Schmidt](github@julienschmidt.com)
* [Max Hawkins](maxhawkins@gmail.com)
* [Piotr "Orange" Zduniak](piotr@zduniak.net)
* [achun](achun.shx@qq.com)
* [rjmcguire](rjmcguire@gmail.com)
* [wei2912](wei2912_support@hotmail.com)

[1]: https://upper.io/db
[2]: http://golang.org
[3]: http://en.wikipedia.org/wiki/Create,_read,_update_and_delete
