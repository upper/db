# What is gosexy?

gosexy is a general purpose framework for Go that provides [sugar](http://en.wikipedia.org/wiki/Syntactic_sugar) methods, types and abstractions.

## Getting sexy

### Before pulling the source

Make sure you have the ``git``, ``hg`` (mercurial) and ``bzr`` (bazaar) source control systems installed on your system, those packages are available for many linux distros and also in [homebrew](http://mxcl.github.com/homebrew/) for OSX. You'll also require ``sqlite3`` and ``pkg-config``. All those packages are required for building some third party dependencies.

Here's how you would install them all on OSX using brew

    $ brew install git
    $ brew install hg
    $ brew install bzr
    $ brew install sqlite3
    $ brew install pkg-config

Or, let's suppose you want to bring them to ArchLinux

    # sudo pacman -S mercurial bzr sqlite3 git pkg-config

### Using gosexy in your Go program

First, get the source using ``go``

    $ go get github.com/xiam/gosexy

Then import ``gosexy`` into your actual source code

    import . "github.com/xiam/gosexy"

## Sugar wrappers

* [gosexy/db](https://github.com/xiam/gosexy/tree/master/db) - A wrapper of [database/sql](http://golang.org/pkg/database/sql), [mgo](http://launchpad.net/mgo) and friends for querying to MongoDB, MySQL, PostgreSQL or SQLite3 databases over a single, consistent interface.
* [gosexy/yaml](https://github.com/xiam/gosexy/tree/master/yaml) - A wrapper of [goyaml](http://launchpad.net/goyaml) for working with [YAML](http://www.yaml.org) formatted files.

## Sugar types

* ``Tuple`` is a shortcut for ``map[string]interface{}`` (generic dictionaries).
* ``List`` is a shortcut for ``[]interface{}`` (generic arrays).

## License

gosexy and friends are all released under the terms of the [MIT License](https://raw.github.com/xiam/gosexy/master/LICENSE).
