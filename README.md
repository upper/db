# gosexy/db

This package is a wrapper of many third party database drivers. The goal of this abstraction is to provide a common,
simplified and consistent layer for working with different databases without the need of SQL statements.

## Installation

Use `go get` to download and install `gosexy/db`.

```sh
# Getting gosexy/db
$ go get github.com/gosexy/db
```

The `gosexy/db` package provides shared interfaces and datatypes only, in order to connect to an actual database
a wrapper is required.

## Available wrappers

* [mongo](http://gosexy.org/db/wrappers/mongo)
* [mysql](http://gosexy.org/db/wrappers/mysql)
* [postgresql](http://gosexy.org/db/wrappers/postgresql)
* [sqlite](http://gosexy.org/db/wrappers/sqlite)

## Usage example

Let's suppose we want to use the `mongo` driver for [MongoDB][1].

```sh
# Installing the driver
$ go get github.com/gosexy/db/mongo
```

Now that the driver is installed, import it into your project.

```
# Importing driver and abstraction layer
import (
  "github.com/gosexy/db"
  /* Import the driver to the blank namespace */
  _ "github.com/gosexy/db/mongo"
)
```

Prepare your connection data.

```go
settings := db.DataSource{
  Host:     "localhost",
  Database: "dbname",
  User:     "myusername",
  Password: "mysecret",
}
```

Then use `db.Open` to connect to the database.

```go
# Connect using the mongo driver.
sess, err := db.Open("mongo", settings)
if err != nil {
  panic(err)
}
defer sess.Close()
```

Now go and query stuff.

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

The same example goes for other drivers with few modifications, just change the driver name to
`mysql`, `postgresql` or `sqlite`.

### Full example

```go
# _examples/mongo.go
package main

import (
	"fmt"
	"github.com/gosexy/db"
	_ "github.com/gosexy/db/mongo"
	"github.com/gosexy/sugar"
)

const host = "debian"
const dbname = "dev"

func main() {

	sess, err := db.Open("mongo", db.DataSource{Host: host, Database: dbname})

	if err != nil {
		panic(err)
	}

	defer sess.Close()

	sess.Drop()

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

}
```

## Documentation

To know how to query the database you've just connected, please read the [online reference](http://gosexy.org/db).

You can also read ``gosexy/db`` documentation from a terminal

```sh
$ go doc github.com/gosexy/db
```

## Things to do

This is an evolving project, there are still some things to do:

* Add db.Upsert and db.Modify for SQL databases.
* Add CouchDB support.

## Changelog

    2012/12/02 - Changing db.Table.Collection and adding db.Open().
    2012/09/21 - Changing some methods parameters and return values, improving error handling and testing many data types.
    2012/08/29 - Created the main site docs and moved the repo to "http://github.com/gosexy".
    2012/07/23 - Splitted database wrappers into packages. Changed ``db.Where`` to ``db.Cond``.
    2012/07/09 - First public beta with MySQL, MongoDB, PostgreSQL and SQLite3.

[1]: http://mongodb.org
