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

package db

import (
	"fmt"
	"github.com/gosexy/sugar"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Returns the item value as a string.
func (item Item) GetString(name string) string {
	return fmt.Sprintf("%v", item[name])
}

// Returns the item value as a Go date.
func (item Item) GetDate(name string) time.Time {
	var date time.Time

	switch item[name].(type) {
	case time.Time:
		date = item[name].(time.Time)
	case string:
		value := item[name].(string)
		date, _ = time.Parse("2006-01-02 15:04:05", value)
	}

	return date
}

// Returns the item value as a Go duration.
func (item Item) GetDuration(name string) time.Duration {
	duration, _ := time.ParseDuration("0h0m0s")

	switch item[name].(type) {
	case time.Duration:
		duration = item[name].(time.Duration)
	case string:
		var matched bool
		var re *regexp.Regexp
		value := item[name].(string)

		matched, _ = regexp.MatchString(`^\d{2}:\d{2}:\d{2}$`, value)

		if matched {
			re, _ = regexp.Compile(`^(\d{2}):(\d{2}):(\d{2})$`)
			all := re.FindAllStringSubmatch(value, -1)

			formatted := fmt.Sprintf("%sh%sm%ss", all[0][1], all[0][2], all[0][3])
			duration, _ = time.ParseDuration(formatted)
		}
	}
	return duration
}

// Returns the item value as a Map.
func (item Item) GetMap(name string) sugar.Map {
	dict := sugar.Map{}

	switch item[name].(type) {
	case map[string]interface{}:
		for k, _ := range item[name].(map[string]interface{}) {
			dict[k] = item[name].(map[string]interface{})[k]
		}
	case sugar.Map:
		dict = item[name].(sugar.Map)
	}

	return dict
}

// Returns the item value as an array.
func (item Item) GetList(name string) sugar.List {
	list := sugar.List{}

	switch item[name].(type) {
	case []interface{}:
		list = make(sugar.List, len(item[name].([]interface{})))

		for k, _ := range item[name].([]interface{}) {
			list[k] = item[name].([]interface{})[k]
		}
	}

	return list
}

// Returns the item value as an integer.
func (item Item) GetInt(name string) int64 {
	i, _ := strconv.ParseInt(fmt.Sprintf("%v", item[name]), 10, 64)
	return i
}

// Returns the item value as a floating point number.
func (item Item) GetFloat(name string) float64 {
	f, _ := strconv.ParseFloat(fmt.Sprintf("%v", item[name]), 64)
	return f
}

// Returns the item value as a boolean.
func (item Item) GetBool(name string) bool {

	if item[name] == nil {
		return false
	}

	switch item[name].(type) {
	default:
		b := strings.ToLower(fmt.Sprintf("%v", item[name]))
		if b == "" || b == "0" || b == "false" {
			return false
		}
	}

	return true
}
