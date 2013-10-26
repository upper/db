# upper.io/db/sqlite

This is a wrapper of the [github.com/mattn/go-sqlite3][1] package by
[Yasuhiro Matsumoto][2].

Some changes had to be made to the original driver in order to work with
`upper.io/db`, such as removing data conversion features, you can see and
contribute to the forked repository at [github.com/xiam/gosqlite3][3].

## Installation

First, make sure you can install [go-sqlite3][1]

```go
# OSX
brew install pkg-config sqlite3
# Debian
sudo apt-get install pkg-config libsqlite3-dev
# Getting the package
go get github.com/mattn/go-sqlite3
```

If you succeed, installing the wrapper won't be any difficult.

```go
go get upper.io/db/sqlite
```

## Usage

Import [db][4] and [sqlite][3].

```go
import (
	"upper.io/db"
	// Import the wrapper to the blank identifier.
	_ "upper.io/db/sqlite"
)
```

Open a database file.

```go
settings := db.Settings{
	Database: "./database.sqlite3",
}
sess, err := db.Open("sqlite", settings)
```

That's all! see the manual at [upper.io][4] for further documentation on
collections and how to create and query result sets.

[1]: https://github.com/mattn/go-sqlite3
[2]: http://mattn.kaoriya.net/
[3]: https://github.com/xiam/gosqlite3
[4]: https://upper.io

