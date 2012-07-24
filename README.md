# What is gosexy?

gosexy is a general purpose framework for Go that provides [sugar](http://en.wikipedia.org/wiki/Syntactic_sugar) methods, types and abstractions.

## Getting sexy

In order to get sexy, you should pull the source

    $ go get github.com/xiam/gosexy

Then import ``gosexy`` into your program

    import "github.com/xiam/gosexy"

## Sugar wrappers

* [gosexy/db](https://github.com/xiam/gosexy/tree/master/db)
* [gosexy/yaml](https://github.com/xiam/gosexy/tree/master/yaml)

## Sugar types

* ``gosexy.Tuple`` is a shortcut for ``map[string]interface{}`` (generic dictionaries).
* ``gosexy.List`` is a shortcut for ``[]interface{}`` (generic arrays).

## License

``gosexy`` and friends are all released under the terms of the [MIT License](https://raw.github.com/xiam/gosexy/master/LICENSE).
