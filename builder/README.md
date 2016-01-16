# upper.io/db.v2/builder [![GoDoc](https://godoc.org/upper.io/db.v2/builder?status.png)](https://godoc.org/upper.io/db.v2/builder) [![Build Status](https://travis-ci.org/upper/builder.svg?branch=master)](https://travis-ci.org/upper/builder)

![builder](http://btbnursery.com/wp-content/uploads/2013/05/bob_homepage_bg.png)

Package `upper.io/db.v2/builder` provides tools to build and execute SQL queries and
map their results to Go structs.

`upper.io/db.v2/builder` is the engine that powers `upper.io/db`.

## Mapping structs to tables

`builder` reads the `db` field tag to determine how to map Go fields to table
columns and vice versa.

```go
// Book is a struct that uses the `db` tag to map each one of its fields to
// table columns.
type Book struct {
  // The ID fields uses "omitempty" to skip it when inserting it to database
  // whenever it has the zero value.
  ID         int       `db:"id,omitempty"`
  Title      string    `db:"title"`
  Author     string    `db:"author"`
  CatalogID  int       `db:"catalog_id"`
  CategoryID int       `db:"category_id"`
  DateAdded  time.Time `db:"date_added"`
}
```

Field types are converted automatically to the type the database expects, so
you can safely use types like `time.Time` and expect them to be mapped to their
equivalent database representation when saving.

## Instantiating a builder

`builder` is composed of many different packages that perform specific tasks:

* The `upper.io/db.v2/builder` package defines abstract interfaces and functions.
* The`upper.io/db.v2/builder/sqlbuilder` package is ready to represent SQL queries
  but it does not know what template to use to build SQL query strings.
* Packages under the `upper.io/db.v2/builder/template/` prefix are template packages
  that can be used to tell `sqlbuilder` how to generate SQL strings.

In order to have a working program that actually generates SQL you'll need at
least two of the above packages.

```go
import (
  "upper.io/db.v2/builder/sqlbuilder"
  "upper.io/db.v2/builder/template/sqlite"
)
...
```

The `builder` package does not handle SQL sessions by itself.  Use the
`sqlbuilder.New()` function to create a builder and tie it to a database
session and a template:

```go
// Let's open a sqlite3 database.
db, err = sql.Open("sqlite3", "./test.db")
...

// Then create a SQL builder with it by passing the database and a template.
bob, err := sqlbuilder.New(db, sqlite.Template)
...
```

If you're ussing `upper.io/db` you can use the `Builder()` method on your
`db.Session` variable to create a builder:

```go
sess, err = db.Open(...)
...
bob = sess.Builder() // look ma! no need to pass a template or a session.
...
```

## Selector

`builder.Selector` provides methods to represent SELECT queries.

```go
q := bob.Select("id", "name")
```

The above statement is somewhat incomplete because it does not actually say
which table to select from, that can be easily fixed by chaining the `From()`
method.

```go
q := bob.Select("id", "name").From("accounts")
```

The above statement is equivalent to `SELECT "id", "name" FROM "accounts"`. We
can also add conditions to reduce the number of matches:

```go
q := bob.Select("id", "name").From("accounts").
  Where("last_name = ?", "Smith")
```

And change the way the results are sorted:

```go
q := bob.Select("id", "name").From("accounts").
  Where("last_name = ?", "Smith").
  OrderBy("name").Limit(10)
```

`builder.Selector` implements `fmt.Stringer`, you can call the `String()` to
build the SQL query:

```go
// SELECT "id", "name" FROM "accounts"
// WHERE "last_name" = ? ORDER BY "name"
// LIMIT 10
s := q.String()
```

You can then pass it to `sql.DB.Query()` if you want, but maybe you want to
build and execute the SQL query without handling it yourself:
`builder.Selector` implements the `builder.Getter` interface which provides
`Query() (*sql.Rows, error)` and `QueryRow() (*sql.Row, error)` which are
similar to the `database/sql` methods.

```go
rows, err := q.Query()
...
defer rows.Close()

for rows.Next() {
  var id int
  var name string
  err = rows.Scan(&id, &name)
  ...
}
err = rows.Err() // get any error.
...
```

The real power of `builder.Selector` comes with the `builder.Iterator`
interface, `builder.Iterator` provides convenient methods to iterate and map
query results to Go structs.

```go
var accounts []Account
...
iter := q.Iterator()
err = iter.All(&accounts) // map all results from the query
...

var account Account
...
iter := q.Iterator()
err = iter.One(&account) // maps the first result from the query
...

// no need to close it when using One()/All()
```

If you rather walk over each row step by step instead of using `All()` you can
use `Next` and `Scan()` or `One()`.

```
iter := q.Iterator()
defer iter.Close() // remember to close it when you're done.

for iter.Next() {
  err = iter.One(&account)
  ...
  // err = iter.Scan(&id, &name)
  ...
}

err = iter.Err() // get any error.
...
```

`builder.Builder` provides different ways to create a `builder.Selector`, for
instance this shortcut:

```go
q := bob.SelectAllFrom("users")
```

## Inserter

`builder.Inserter` provides methods to represent INSERT queries.

Use the `InsertInto()` method on a builder to create a `builder.Inserter`:

```go
q := bob.InsertInto("accounts")
```

You can provide columns and values with the `Columns()` and `Values()` methods:

```go
q := bob.InsertInto("accounts").Columns("name").Values("John")
```

The `Values()` method is smart enough to accept a struct and map it, without
the need for us to specify columns explicitly:

```go
account := Account{
  Name:     "John",
  LastName: "Smith",
}
q := bob.InsertInto("accounts").Values(account)
```

In order to build and execute the query use the `Exec()` method.

```go
res, err = q.Exec()
...
```

`builder.Inserter` also satisfies `fmt.Stringer`, so it's easy to see the
computed SQL:

```go
// INSERT INTO "accounts" COLUMNS("name", "last_name")
// VALUES($1, $2)
log.Printf("sql: %s", q)
```

## Updater

`builder.Updater` provides methods to represent UPDATE queries.

The `builder.Updater` interface provides the `Set()` method which you can use
to define what to update:

```go
q := bob.Update("accounts").Set("name", "María").Where("id", 5)
```

The `Set()` method can also accept a struct or a map and infer column names
from it:

```go
account := Account{
  Name:     "María",
  LastName: "López",
}
res, err := bob.Update("accounts").Set(account).
  Where("id", 5).Exec()
```

Which would be equivalent to:

```go
q := bob.Update("accounts").Set(
  "name", "María",
  "last_name", "López",
).Where("id", 5)
```

## Deleter

`builder.Deleter` provides methods to represent DELETE queries.

```go
q := bob.DeleteFrom("accounts").Where("id", 5)
```

As you would expect, no operation will be executed until calling `Exec()`:

```go
res, err = q.Exec()
```

## Joins

Another powerful feature of `builder.Selector` is its ability to build SQL
queries that include joins.

The `Join()` method expects the tables you want to join with and the `On()`
methods expects the join conditions.

```go
q := bob.Select("a.name").From("accounts AS a").
  Join("profiles AS p").
  On("p.account_id = a.id")
```

The `Using()` method can also be used to specify which columns should be tested
for equality when using joins:

```go
q := bob.Select("name").From("accounts").
  Join("owners").
  Using("employee_id")
```

In addition to `Join()` you can also use `FullJoin()`, `CrossJoin()`,
`RightJoin()` and `LeftJoin()`.

## Raw SQL queries

Sometimes the builder won't be able to represent complex queries, if this
happens it may be more effective to use plain ol' SQL:

```go
rows, err = bob.Query(`SELECT * FROM accounts WHERE id = ?`, 5)
...
row, err = bob.QueryRow(`SELECT * FROM accounts WHERE id = ? LIMIT ?`, 5, 1)
...
res, err = bob.Exec(`DELETE FROM accounts WHERE id = ?`, 5)
...
```

You can create an iterator with any `*sql.Rows` value:

```go
rows, err = bob.Query(`SELECT * FROM accounts WHERE last_name = ?`, "Smith")
...
var accounts []Account
iter := sqlbuilder.NewIterator(rows)
iter.All(&accounts)
...
```

## The Where() method

The `Where()` method can be used to define conditions on a query and it can be
chained easily to the `Selector`, `Deleter` and `Updater` interfaces:

Let's suppose we have a `Selector`:

```go
q := bob.SelectAllFrom("accounts")
```

We can use the `Where()` method to add conditions to the above query. What
about constraining the results to the rows that match `id = 5`?:

```go
q.Where("id = ?", 5)
```

We use a `?` as a placeholder for the argument, this is required in order to
sanitize arguments and prevent SQL injections. You can use as many arguments as
you need as long as you provide a value for each one of them:

```go
q.Where("id = ? OR id = ?", 5, 4)
```

The above condition can be easily rewritten into:


```go
q.Where("id IN ?", []int{5,4})
```

And in fact, we can drop the `?` at the end if we only want to test an
equalility:

```go
q.Where("id", 5)
...
q.Where("id IN", []int{5,4})
...
```

It is also possible to use other operators besides the equality, but you have
to be explicit about them:

```go
q.Where("id >", 5)
...
q.Where("id > ? AND id < ?", 5, 10)
...
```

If you import `upper.io/db.v2/builder` you can also use `builder.M` to define
conditions:

```go
// ...WHERE "id" > 5
q.Where(builder.M{
  "id >": 5,
})
...
// ...WHERE "id" > 5 AND "id" < 10
q.Where(builder.M{"id >": 5, "id <": 10})
...
```

You can also use `builder.Or()` and `builder.And()` to join conditions:

```go
// ...WHERE ("id" = 5 OR "id" = 9 OR "id" = 12)
q.Where(builder.Or(
  builder.M{"id": 5},
  builder.M{"id": 9},
  builder.M{"id": 12},
))
```

## License

This project is licensed under the terms of the **MIT License**.

> Copyright (c) 2012-2015 The upper.io authors. All rights reserved.
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
