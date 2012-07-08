# gosexy/yaml

This package is a wrapper of [goyaml](http://launchpad.net/goyaml) that provides methods for loading, reading and writing to and from [YAML](http://www.yaml.org/) formatted files.

## Installation

    $ go get github.com/xiam/gosexy/yaml

## Usage

    package main

    import "github.com/xiam/gosexy/yaml"

    func main() {
      settings := yaml.New()
      defer settings.Write("test.yaml")
      settings.Set("success", true)
    }

## Documentation

You can read ``gosexy/yaml`` documentation from a terminal

    $ go doc github.com/xiam/gosexy/yaml

Or you can [browse it](http://go.pkgdoc.org/github.com/xiam/gosexy/yaml) online.
