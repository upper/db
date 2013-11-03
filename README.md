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

```
go get upper.io/db
```

Then, get the wrapper you want to use. Choose one of `mysql`, `sqlite`, `mongo`
or `postgresql`.

```
go get upper.io/db/sqlite
```

## An actual code example

Define a Go struct, use Go datatypes and define column names within field tags.

```
type Birthday struct {
	Name string    `field:"name"`
	Born time.Time `field:"born"`
}
```

Define your database settings.

```
var settings = db.Settings{
	Database: `example.db`,
}
```

Open a connection to a database using a driver (`sqlite` in this example).

```
sess, err = db.Open("sqlite", settings)
```

Get a collection reference.

```
 birthdayCollection, err = sess.Collection("birthdays")
```

Use the `Collection.Append` method to insert some data into the collection.

```
birthdayCollection.Append(Birthday{
  Name: "Hayao Miyazaki",
  Born: time.Date(1941, time.January, 5, 0, 0, 0, 0, time.UTC),
})
```

Use the `Collection.Filter` method without arguments to retrieve all the rows
within the collection.

```
res, err = birthdayCollection.Filter()

var birthday Birthday

for {
  err = res.Next(&birthday)
  if err == nil {
    fmt.Printf("%s was born in %s.\n", birthday.Name, birthday.Born.Format("January 2, 2006"))
  } else if err == db.ErrNoMoreRows {
    break
  } else {
    panic(err.Error())
  }
}
```

Close this session, you can also use `defer` for closing.

```
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
