# upper.io/db

[Upper DB][1] is a [Go][2] package for saving and retrieving [Go][2] structs
to permanent storage with ease.

[Upper DB][1] is able to comunicate with SQL and NoSQL databases through a
simplified API and perform the most common operations on database systems such
as appending, searching, updating and removing items.

## Database compatibility

Wrappers are provided for the following databases:

* SQLite3
* MySQL
* PostgreSQL
* MongoDB

Wrappers are based on popular SQL drivers for `database/sql` and in the MongoDB
driver `labix.org/v2/mgo`.

## Installation

Get the main package.

```sh
go get upper.io/db
```

Then, get the wrapper you want to use. Choose one of `mysql`, `sqlite`, `mongo`
or `postgresql`.

```sh
go get upper.io/db/sqlite
```

## An actual code example

### Defining a struct

Define a Go struct, use Go datatypes and define column names within field tags.

```go
type Birthday struct {
  Name string    `field:"name"`
  Born time.Time `field:"born"`
}
```

### Open a database session

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

### Use a table/collection

Get a collection reference.

```go
birthdayCollection, err = sess.Collection("birthdays")
```

### Insert a new item

Use the `Collection.Append` method in the collection reference to save a new
item.

```go
id, err = birthdayCollection.Append(Birthday{
  Name: "Hayao Miyazaki",
  Born: time.Date(1941, time.January, 5, 0, 0, 0, 0, time.UTC),
})
```

### Search for items

Use the `Collection.Find` method to search for the recently appended item and
create a result set.

```
res = birthdayCollection.Find(db.Cond{"id": id})
```

### Fetch an item

Use the `Result.One` method from the result set to fetch just one result and
populate an empty struct of the same type.

```go
var birthday Birthday
err = res.One(&birthday)
```

### Update an item

Modify the struct and commit the update to all the items within the result set
(just one in this example) to permanent storage.

```go
birthday.Name = `Miyazaki Hayao`
err = res.Update(birthday)
```

### Remove an iem

Remove all the items within the result set (just one in this example).

```go
err = res.Remove()
```

Close this session, you can also use `defer` after a successful `Database.Open`
for closing.

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
