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
	"fmt"
	. "github.com/xiam/gosexy"
	"launchpad.net/goyaml"
	"os"
	"strings"
)

const SEPARATOR = "/"

type Yaml struct {
	file   string
	values *Tuple
}

// Creates and returns a new YAML structure.
func New() *Yaml {
	yaml := &Yaml{}
	yaml.values = &Tuple{}
	return yaml
}

// Creates and returns a YAML structure from a file.
func Open(file string) *Yaml {
	yaml := New()
	yaml.file = file
	yaml.Read(yaml.file)
	return yaml
}

// Returns the string value of the YAML path or an empty string, if the path cannot be found.
func (y *Yaml) GetString(path string) string {
	return y.Get(path, "").(string)
}

// Returns the integer value of the YAML path or 0, if the path cannot be found.
func (y *Yaml) GetInt(path string) int {
	return y.Get(path, 0).(int)
}

// Returns the float value of the YAML path or 0.0, if the path cannot be found.
func (y *Yaml) GetFloat(path string) float64 {
	return y.Get(path, 0).(float64)
}

// Returns the boolean value of the YAML path or false, if the path cannot be found.
func (y *Yaml) GetBool(path string) bool {
	return y.Get(path, false).(bool)
}

// Returns the sequenced value of the YAML path or an empty sequence, if the path cannot be found.
func (y *Yaml) GetSequence(path string) []interface{} {
	return y.Get(path, nil).([]interface{})
}

// Returns a YAML setting (or defaultValue if the referred name does not exists). Read nested values by using a dot (.) between labels.
//
// Example:
//
//	yaml.Get("foo.bar", "default")
func (y *Yaml) Get(path string, defaultValue interface{}) interface{} {
	var p Tuple

	path = strings.ToLower(path)

	p = *y.values

	chunks := strings.Split(path, SEPARATOR)

	length := len(chunks)

	for i := 0; i < length; i++ {

		value, ok := p[chunks[i]]

		if i+1 == length {
			if ok {
				return value
			}
		} else {

			if ok == true {
				switch value.(type) {
				case Tuple:
					{
						p = value.(Tuple)
					}
				default:
					{
						return defaultValue
					}
				}
			} else {
				return defaultValue
			}
		}

	}

	return defaultValue
}

// Sets a YAML setting, use diagonals (/) to nest values inside values.
func (y *Yaml) Set(path string, value interface{}) {
	var p Tuple

	path = strings.ToLower(path)

	p = *y.values

	chunks := strings.Split(path, SEPARATOR)

	length := len(chunks)

	for i := 0; i < length; i++ {

		current, ok := p[chunks[i]]

		if i+1 == length {
			delete(p, chunks[i])
			p[chunks[i]] = value
		} else {
			// Searching.
			if ok == true {
				switch current.(type) {
				case Tuple:
					{
						// Just skip.
					}
				default:
					{
						delete(p, chunks[i])
						p[chunks[i]] = Tuple{}
					}
				}
			} else {
				p[chunks[i]] = Tuple{}
			}

			p = p[chunks[i]].(Tuple)
		}
	}

}

func (y *Yaml) mapValues(data interface{}, parent *Tuple) {

	var name string

	for key, value := range data.(map[interface{}]interface{}) {
		name = strings.ToLower(key.(string))

		switch value.(type) {
		case []interface{}:
			{
				(*parent)[name] = value.([]interface{})
			}
		case string:
			{
				(*parent)[name] = value.(string)
			}
		case int:
			{
				(*parent)[name] = value.(int)
			}
		case bool:
			{
				(*parent)[name] = value.(bool)
			}
		case float64:
			{
				(*parent)[name] = value.(float64)
			}
		case interface{}:
			{
				values := &Tuple{}
				y.mapValues(value, values)
				(*parent)[name] = *values
			}
		}

	}

}

// Saves changes made to the latest Open()'ed YAML file.
func (y *Yaml) Save() {
	if y.file != "" {
		y.Write(y.file)
	} else {
		panic(fmt.Errorf("No file specified."))
	}
}

// Writes the current YAML structure into an arbitrary file.
func (y *Yaml) Write(filename string) {

	out, err := goyaml.Marshal(y.values)
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

// Reads a YAML file and stores it into the current YAML structure.
func (y *Yaml) Read(filename string) {
	var err error
	var data interface{}

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

		y.mapValues(data, y.values)

	} else {

		panic(err.Error())

	}

}
