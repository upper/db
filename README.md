# gosexy/db

`gosexy/db` is a set of wrappers for SQL and NoSQL database drivers for the
[Go][7] programming languaje, it's currently compatible with [MongoDB][1],
[MySQL][2], [PostgreSQL][3] and [SQLite3][4].

If you're interested on a quick tour see the [project page][8].

## The project

The goal of this package is to provide a common, simple, consistent layer of
abstraction for performing mundane operations such as *create*, *read*,
*update*, and *delete* rows (CRUD) on different databases.

While `gosexy/db` is *not* an ORM *per se* it can be used as the base for one,
we leave that up to you, `gosexy/db` prefers to stay out of the way and just
focusing in providing a layer of compatibility between databases.

### An introductory example

Let me show you an example, this chunk of code searches on the "people"
table/collection. It does not matter whether we are querying a NoSQL database
like [MongoDB][1] or a SQL database like [MySQL][2], [PostgreSQL][3] or
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
in the above example we passed a `db.Cond{}` type, that's a condition, you could
also use other statemets such as `db.And{}`, `db.Or{}`, `db.Limit(n)`,
`db.Offset(n)`, etc. to customize your query.

While this level of abstraction would not be able to represent a complex query
or to use any database-specific features it's fairly convenient for doing most
of the operations you would do with a database, and if you ever need to do more
advanced queries, the underlying driver could always be retrieved as a `*sql.DB`
or a `*mgo.Session` so you can still be able to use any database-specific
pro spells.

### Iterating over results

Fetching all rows may be not so adequate for processing large datasets, in that
case we could use `db.Collection.Query()` instead of `db.Collection.FindAll()`
and then iterate over the results.

```go
// Makes a query and stores the result.
res, err = people.Query(
  db.Cond{"name": "john"},
)

if err != nil {
  panic(err.Error())
}

person := struct{
  PersonName string `field:"name"` // Supports struct tags.
}{}

for {
  // res.Next() accepts a pointer to map or a pointer to struct.
  err = res.Next(&person)

  if err == nil {
    // If there is no error we can use person.
    fmt.Printf("%v\n", person)
  } else if err == db.ErrNoMoreRows {
    // This error means we have read all rows.
    break
  } else {
    // Another kind of error needs proper management.
    panic(err.Error())
  }

}
```

### Inter-database relations

One of the features you may find convenient is the ability of `gosexy/db` to
make relations between different databases that talk different protocols with
ease:

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

## Installation

Use `go get` to download and install `gosexy/db`.

```sh
go get menteslibres.net/gosexy/db
```

## Usage example

Let's suppose we want to use the "mongo" wrapper for [MongoDB][1].

Use `go get` to retrieve and install the "mongo" wrapper.

```sh
go get menteslibres.net/gosexy/db/mongo
```

Import `gosexy/db` and the wrapper into your project.

```go
import (
  "menteslibres.net/gosexy/db"
  // The wrapper goes to the blank identifier.
  _ "menteslibres.net/gosexy/db/mongo"
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
  "young":  []string{"Puppy", "Pup"},
  "female": "Bitch",
  "male":   "Dog",
  "group":  "Pack",
})

items, err := animals.FindAll()

if err != nil {
  panic(err.Error())
}

for _, item := range items {
  fmt.Printf("animal: %s, young: %s\n", item["animal"], item["young"])
}
```

Remember this example uses the `mongo` wrapper, but there are other wrappers
available and they work with the same code.

## Documentation

We have an [online reference][8] and you can always have a quick look to the
API at our [documentation page][5].

Got problems? try to use the
[forum](https://groups.google.com/forum/?fromgroups=#!forum/gosexy).

## Things to do

This is an evolving project, there are still lots of things to do:

* Add db.Upsert and db.Modify for SQL databases.
* Add CouchDB support.

## Changelog
    2013/04/15 - Adding support for Update() with structs and maps.
    2013/04/06 - Adding support for struct tags.
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
                 "http://menteslibres.net/gosexy".
    2012/07/23 - Splitted database wrappers into packages. Changed db.Where to
                 db.Cond.
    2012/07/09 - First public beta with MySQL, MongoDB, PostgreSQL and SQLite3.

[1]: http://mongodb.org
[2]: http://mysql.com
[3]: http://postgresql.org
[4]: http://sqlite.com
[5]: http://godoc.org/menteslibres.net/gosexy/db
[6]: https://menteslibres.net/xiam
[7]: http://www.golang.org
[8]: https://menteslibres.net/gosexy/db

