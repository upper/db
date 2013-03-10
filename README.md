# gosexy/db

`gosexy/db` is a set of wrappers for SQL and NoSQL database drivers for Go
currently compatible with [MongoDB][1], [MySQL][2], [PostgreSQL][3]
and [SQLite3][4].

The goal of this package is to provide a common, simple, consistent layer of
abstraction for performing mundane operations such as create, read, update,
and delete rows (CRUD) on different databases.

While `gosexy/db` is *not* an ORM it can be used as the base for one, that's
up to the final user, but `gosexy/db` prefers to stay out of the way.

Let me show you an example, this chunk of code searches on the "people"
table/collection. It does not matter whether we are querying a NoSQL database
like [MongoDB][1] or an SQL database like [MySQL][2], [PostgreSQL][3] or
[SQLite3][4], `gosexy/db` talks to the database in the language the database
expects and returns you a set of results.

```go
items, err := people.FindAll(
  db.Cond{"name": "john"},
)

for i, item := range items {
  // ...
}
```

`db.Collection.FindAll()` will accept different structures in no special order,
in the above example we are passing a `db.Cond{}` type, that's a condition,
you could also use `db.And{}`, `db.Or{}`, `db.Limit(n)`, `db.Offset(n)`, etc.
each one of them having different meanings.

While this level of abstraction would not be able to represent a complex query
or to use any database-specific features it's fairly convenient for doing the
simple CRUD stuff, and for advanced queries the underlying driver is always
exposed as a `*sql.DB` or a `*mgo.Session` so you can still be able to use
any database-pro spells.

Fetching all rows may be not so adequate for processing large datasets, in that
case we can use `db.Collection.Query()` instead of `db.Collection.FindAll()` and
then iterate over the results.

```go
// Makes a query and stores the result.
res, err = people.Query(
  db.Cond{"name": "john"},
)

if err != nil {
  panic(err.Error())
}

person := struct{ Name string }{}

for true {
  // res.Next() will accept a pointer to map or struct.
  err = res.Next(&person)
  if err != nil {
    break
  }
  // fmt.Printf("%v\n", person)
}
```

One of the features you may find useful is the ability of `gosexy/db` to make
relations between different databases that talk different protocols with ease:

```go
items, err := peopleCollection.FindAll(
  db.RelateAll{
    "works": db.On{
      worksCollection,
      db.Cond{"author_id": "{id}"},
    },
  },
)
```

In the above example, `peopleCollection` and `worksCollection` are
`db.Collection` objects and they could be collections or tables of any of the
supported databases. You can even relate NoSQL collections to SQL tables!

`gosexy/db` is a work in progress but its core features are ready for use.

## Installation

Use `go get` to download and install `gosexy/db`.

```sh
go get github.com/gosexy/db
```

The `gosexy/db` package provides shared interfaces and datatypes only, it can't
connect to any database by itself, in order to connect to an actual database
a database wrapper is required.

## Database wrappers

Database wrappers may have special installation requirements, please refer to
the appropriate documentation reference on the following list.

* [mongo](http://gosexy.org/db/wrappers/mongo)
* [mysql](http://gosexy.org/db/wrappers/mysql)
* [postgresql](http://gosexy.org/db/wrappers/postgresql)
* [sqlite](http://gosexy.org/db/wrappers/sqlite)

## Usage example

Let's suppose we want to use the "mongo" wrapper for [MongoDB][1].

Use `go get` to retrieve and install the "mongo" wrapper.

```sh
go get github.com/gosexy/db/mongo
```

Import `gosexy/db` and the wrapper into your project.

```go
import (
  "github.com/gosexy/db"
  // The wrapper goes to the blank namespace.
  _ "github.com/gosexy/db/mongo"
)
```

Set up a variable to configure your database connection.

```go
var settings = db.DataSource{
  Host:     "localhost",
  Database: "dbname",
  User:     "myusername",
  Password: "mysecret",
}
```

Use `db.Open` to connect to the database you've just set up.

```go
// Connect using the mongo driver.
sess, err := db.Open("mongo", settings)

if err != nil {
  panic(err)
}

defer sess.Close()
```

Insert some items and retrieve rows.

```go
animals, _ := sess.Collection("animals")

animals.Append(db.Item{
  "animal": "Bird",
  "young":  "Chick",
  "female": "Hen",
  "male":   "Cock",
  "group":  "flock",
})

animals.Append(db.Item{
  "animal": "Bovidae",
  "young":  "Calf",
  "female": "Cow",
  "male":   "Bull",
  "group":  "Herd",
})

animals.Append(db.Item{
  "animal": "Canidae",
  "young":  sugar.List{"Puppy", "Pup"},
  "female": "Bitch",
  "male":   "Dog",
  "group":  "Pack",
})

items := animals.FindAll()

for _, item := range items {
  fmt.Printf("animal: %s, young: %s\n", item["animal"], item["young"])
}
```

The same example goes for other wrappers, you just change the driver name to
`mysql`, `postgresql` or `sqlite`.

## Documentation

If you're in trouble, you may want to try the
[online reference](http://gosexy.org/db) and the [documentation page][5].

Speak IRC? you can contact [the author][6] at #menteslibres on freenode.


## Things to do

This is an evolving project, there are still some things to do:

* Add db.Upsert and db.Modify for SQL databases.
* Add CouchDB support.

## Changelog

    2013/03/10 - Breaking change: Find() and FindAll() now will return ([item],
                 error).
               - Adding db.Result and methods for iterating over results.
               - Adding the ability to use struct{} for fetching and inserting
                 rows.
               - Adding helper package gosexy/db/util for internal usage.
    2012/12/02 - Changing db.Table.Collection and adding db.Open().
    2012/09/21 - Changing some methods parameters and return values, improving
                 error handling and testing many data types.
    2012/08/29 - Created the main site docs and moved the repo to
                 "http://github.com/gosexy".
    2012/07/23 - Splitted database wrappers into packages. Changed db.Where to
                 db.Cond.
    2012/07/09 - First public beta with MySQL, MongoDB, PostgreSQL and SQLite3.

[1]: http://mongodb.org
[2]: http://mysql.com
[3]: http://postgresql.org
[4]: http://sqlite.com
[5]: http://godoc.org/github.com/gosexy/db
[6]: http://xiam.menteslibres.org
