# upper.io/db

[Upper DB][1] is a [Go][2] package for saving and retrieving [Go][2] structs
to and from permanent storage with ease.

[Upper DB][1] performs the most common operations on SQL and NoSQL databases
such as appending, searching, updating and removing items.

## Database compatibility

Wrappers are provided for the following databases:

* [SQLite3](./sqlite)
* [MySQL](./mysql)
* [PostgreSQL](./postgresql)
* [MongoDB](./mongo)

## Installation

Get the main package.

```sh
go get upper.io/db
```

Then, get the wrapper you want to use. Choose one among `mysql`, `sqlite`,
`mongo` or `postgresql`.

```sh
go get upper.io/db/sqlite
```

## Code example

### Defining a type struct

Use Go datatypes and define column names within field tags.

```go
type Birthday struct {
  Name string    `field:"name"`
  Born time.Time `field:"born"`
}
```

### Openning a database session

Define your database settings using the `db.Settings` struct.

```go
var settings = db.Settings{
  Database: `example.db`,
}
```

Open a connection to a database using a driver (`sqlite` in this example).

```go
sess, err = db.Open("sqlite", settings)
```

### Using a table/collection

Get a collection reference.

```go
birthdayCollection, err = sess.Collection("birthdays")
```

### Adding a new item

Use the `Collection.Append` method in the collection reference to save a new
item.

```go
id, err = birthdayCollection.Append(Birthday{
  Name: "Hayao Miyazaki",
  Born: time.Date(1941, time.January, 5, 0, 0, 0, 0, time.UTC),
})
```

### Searching for items

Use the `Collection.Find` method to search for the recently appended item and
create a result set.

```
res = birthdayCollection.Find(db.Cond{"id": id})
```

### Fetching an item

Use the `Result.One` method from the result set to fetch just one result and
populate an empty struct of the same type.

```go
var birthday Birthday
err = res.One(&birthday)
```

### Updating an item

Modify the struct and commit the update to all the items within the result set
(just one in this example) to permanent storage.

```go
birthday.Name = `Miyazaki Hayao`
err = res.Update(birthday)
```

### Removing an item

Remove all the items within the result set (just one in this example).

```go
err = res.Remove()
```

### Closing a result set
```go
res.Close()
```

### Ending session

You can also use `defer` after a successful `Database.Open` for closing a
database session.

```go
sess.Close()
```

## License

> Copyright (c) 2012-2013 José Carlos Nieto, https://menteslibres.net/xiam
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

[1]: http://upper.io/db
[2]: http://golang.org
