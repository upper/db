/*
  Copyright (c) 2012 José Carlos Nieto, http://xiam.menteslibres.org/

  Permission is hereby granted, free of charge, to any person obtaining
  a copy of this software and associated documentation files (the
  "Software"), to deal in the Software without restriction, including
  without limitation the rights to use, copy, modify, merge, publish,
  distribute, sublicense, and/or sell copies of the Software, and to
  permit persons to whom the Software is furnished to do so, subject to
  the following conditions:

  The above copyright notice and this permission notice shall be
  included in all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

package yaml

import (
  "github.com/xiam/gosexy"
  "launchpad.net/goyaml"
  "os"
  "strings"
)

type Storage struct {
  storage *gosexy.Tuple
}

func NewYAML() *Storage {
  c := &Storage{ }
  return c
}

func (c *Storage) Get(path string, def interface { }) interface { } {
  var p gosexy.Tuple

  path = strings.ToLower(path)

  p = *c.storage

  chunks := strings.Split(path, ".")

  length := len(chunks)

  for i := 0; i < length; i++ {
    
    value, ok := p[chunks[i]]

    if i + 1 == length {
      if ok {
        return value
      }
    } else {

      if ok == true {
        switch value.(type) {
          case gosexy.Tuple: {
            p = value.(gosexy.Tuple)
          }
          default: {
            return def
          }
        }
      } else {
        return def
      }
    }

  }

  return def
}

func (c *Storage) Set(path string, value interface { }) {
  var p gosexy.Tuple

  path = strings.ToLower(path)

  p = *c.storage

  chunks := strings.Split(path, ".")

  length := len(chunks)

  for i := 0; i < length; i++ {

    current, ok := p[chunks[i]]

    if i + 1 == length {
      delete(p, chunks[i])
      p[chunks[i]] = value
    } else {
      // Searching.
      if ok == true {
        switch current.(type) {
          case gosexy.Tuple: {
            // Just skip.
          }
          default: {
            delete(p, chunks[i])
            p[chunks[i]] = gosexy.Tuple{}
          }
        }
      } else {
        p[chunks[i]] = gosexy.Tuple{}
      }

      p = p[chunks[i]].(gosexy.Tuple)
    }
  }

}

func (c *Storage) Map(data interface { }, parent *gosexy.Tuple) {

  var name string

  for key, value := range data.(map[interface { }]interface{ }) {
    name = strings.ToLower(key.(string))

    switch value.(type) {
      case []interface {}: {
        (*parent)[name] = value.([]interface {})
      }
      case string: {
        (*parent)[name] = value.(string)
      }
      case int: {
        (*parent)[name] = value.(int)
      }
      case bool: {
        (*parent)[name] = value.(bool)
      }
      case float64: {
        (*parent)[name] = value.(float64)
      }
      case interface {}: {
        values := &gosexy.Tuple{ }
        c.Map(value, values)
        (*parent)[name] = *values
      }
    }

  }

}

func (c *Storage) Write(filename string) {

  out, err := goyaml.Marshal(c.storage)
  if err != nil {
    panic(err)
  }

  fp, err := os.Create(filename)
  if err != nil {
    panic(err)
  }
  defer fp.Close()

  fp.Write(out)
}

func (c *Storage) Read(filename string) {
  var err error
  var data interface { }

  fileinfo, err := os.Stat(filename)

  if err != nil {
    panic(err)
  }

  filesize := fileinfo.Size()

  fp, err := os.Open(filename)

  if err != nil {
    panic(err)
  }

  defer fp.Close()

  buf := make([]byte, filesize)
  fp.Read(buf)

  err = goyaml.Unmarshal(buf, &data)

  if err == nil {

    c.storage = &gosexy.Tuple{ }
    c.Map(data, c.storage)

  } else {

    panic(err.Error())

  }

}


