# gosexy/db/sqlite

This driver is a wrapper of [sqlite3](https://github.com/mattn/go-sqlite3). In order to work with ``gosexy/db`` the original driver had to be [forked](https://github.com/xiam/gosqlite3) as the changes made to it are incompatible with some of sqlite3's own features.

## Requirements

The sqlite3 driver uses cgo, and it requires ``pkg-config`` and the sqlite3 header files in order to be installed.

If you're using ``brew`` and OSX, you can install them like this

    $ brew install pkg-config
    $ brew install sqlite3

On ArchLinux you could use

    $ sudo pacman -S pkg-config
    $ sudo pacman -S sqlite3

And on Debian based distros

    $ sudo aptitude install pkg-config
    $ sudo aptitude install libsqlite3-dev

## Installation

    $ go get github.com/gosexy/db/sqlite

## Usage

    import (
      "github.com/gosexy/db"
      "github.com/gosexy/db/sqlite"
    )

## Connecting to a SQLite3 database

    sess := sqlite.Session(db.DataSource{Database: "/path/to/sqlite3.db"})

    err := sess.Open()
    defer sess.Close()

Read full documentation and examples on the [gosexy/db](/xiam/gosexy/tree/master/db) manual.
