package sqlutil

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/gosexy/db"
	"github.com/gosexy/to"
	"reflect"
	"regexp"
	"strings"
	"time"
)

var extRelationPattern = regexp.MustCompile(`\{(.+)\}`)

type Table struct {
	DB          db.Database
	TableName   string
	ColumnTypes map[string]reflect.Kind
}

type QueryChunks struct {
	Fields     []string
	Limit      string
	Offset     string
	Sort       string
	Relate     db.Relate
	RelateAll  db.RelateAll
	Relations  []db.Relation
	Conditions string
	Arguments  db.SqlArgs
}

var durationType = reflect.TypeOf(time.Duration(0))
var timeType = reflect.TypeOf(time.Time{})

var columnComparePattern = regexp.MustCompile(`[^a-zA-Z0-9]`)

func (self *Table) ColumnLike(s string) string {
	for col, _ := range self.ColumnTypes {
		if compareColumnToField(s, col) == true {
			return col
		}
	}
	return s
}

func (self *Table) RelationCollection(name string, terms db.On) (db.Collection, error) {

	var err error
	var col db.Collection

	for _, v := range terms {

		switch t := v.(type) {
		case db.Collection:
			col = t
		}
	}

	if col == nil {
		fmt.Printf("what? %v\n", self.DB)
		col, err = self.DB.Collection(name)
		if err != nil || col == nil {
			return nil, fmt.Errorf("Failed relation %s: %s", name, err.Error())
		}
	}

	return col, nil
}

func convertValue(src string, dstk reflect.Kind) (reflect.Value, error) {
	var srcv reflect.Value

	// Destination type.
	switch dstk {
	case reflect.Interface:
		// Destination is interface, nuff said.
		srcv = reflect.ValueOf(src)
	case durationType.Kind():
		// Destination is time.Duration
		srcv = reflect.ValueOf(to.Duration(src))
	case timeType.Kind():
		// Destination is time.Time
		srcv = reflect.ValueOf(to.Time(src))
	default:
		// Destination is of an unknown type.
		cv, _ := to.Convert(src, dstk)
		srcv = reflect.ValueOf(cv)
	}

	return srcv, nil
}

/*
	Returns true if a table column looks like a struct field.
*/
func compareColumnToField(s, c string) bool {
	s = columnComparePattern.ReplaceAllString(s, "")
	c = columnComparePattern.ReplaceAllString(c, "")
	return strings.ToLower(s) == strings.ToLower(c)
}

/*
	Copies *sql.Rows into the slice of maps or structs given by the pointer dst.
*/
func (self *Table) FetchRows(dst interface{}, rows *sql.Rows) error {

	// Destination.
	dstv := reflect.ValueOf(dst)

	if dstv.Kind() != reflect.Ptr || dstv.Elem().Kind() != reflect.Slice || dstv.IsNil() {
		return errors.New("fetchRows expects a pointer to slice.")
	}

	// Column names.
	columns, err := rows.Columns()

	if err != nil {
		return err
	}

	// Column names to lower case.
	for i, _ := range columns {
		columns[i] = strings.ToLower(columns[i])
	}

	expecting := len(columns)

	slicev := dstv.Elem()
	itemt := slicev.Type().Elem()

	for rows.Next() {

		// Allocating results.
		values := make([]*sql.RawBytes, expecting)
		scanArgs := make([]interface{}, expecting)

		for i := range columns {
			scanArgs[i] = &values[i]
		}

		var item reflect.Value

		switch itemt.Kind() {
		case reflect.Map:
			item = reflect.MakeMap(itemt)
		case reflect.Struct:
			item = reflect.New(itemt)
		default:
			return fmt.Errorf("Don't know how to deal with %s, use either map or struct.", itemt.Kind())
		}

		err := rows.Scan(scanArgs...)

		if err != nil {
			return err
		}

		// Range over row values.
		for i, value := range values {
			if value != nil {
				column := columns[i]
				svalue := string(*value)

				var cv reflect.Value

				if _, ok := self.ColumnTypes[column]; ok == true {
					v, _ := to.Convert(string(*value), self.ColumnTypes[column])
					cv = reflect.ValueOf(v)
				} else {
					v, _ := to.Convert(string(*value), reflect.String)
					cv = reflect.ValueOf(v)
				}

				switch itemt.Kind() {
				// Destination is a map.
				case reflect.Map:
					if cv.Type().Kind() != itemt.Elem().Kind() {
						if itemt.Elem().Kind() != reflect.Interface {
							// Converting value.
							cv, _ = convertValue(svalue, itemt.Elem().Kind())
						}
					}
					if cv.IsValid() {
						item.SetMapIndex(reflect.ValueOf(column), cv)
					}
				// Destionation is a struct.
				case reflect.Struct:
					// Get appropriate column.
					f := func(s string) bool {
						return compareColumnToField(s, column)
					}
					// Destination field.
					destf := item.Elem().FieldByNameFunc(f)
					if destf.IsValid() {
						if cv.Type().Kind() != destf.Type().Kind() {
							if destf.Type().Kind() != reflect.Interface {
								// Converting value.
								cv, _ = convertValue(svalue, destf.Type().Kind())
							}
						}
						// Copying value.
						if cv.IsValid() {
							destf.Set(cv)
						}
					}
				}
			}
		}

		slicev = reflect.Append(slicev, reflect.Indirect(item))
	}

	dstv.Elem().Set(slicev)

	return nil
}

/*
	Returns the table name as a string.
*/
func (self *Table) Name() string {
	return self.TableName
}

func (self *Table) FieldValues(item interface{}, convertFn func(interface{}) string) ([]string, []string, error) {

	fields := []string{}
	values := []string{}

	itemv := reflect.ValueOf(item)
	itemt := itemv.Type()

	switch itemt.Kind() {
	case reflect.Struct:
		nfields := itemv.NumField()
		values = make([]string, nfields)
		fields = make([]string, nfields)
		for i := 0; i < nfields; i++ {
			fields[i] = self.ColumnLike(itemt.Field(i).Name)
			values[i] = convertFn(itemv.Field(i).Interface())
		}
	case reflect.Map:
		nfields := itemv.Len()
		values = make([]string, nfields)
		fields = make([]string, nfields)
		mkeys := itemv.MapKeys()
		for i, keyv := range mkeys {
			valv := itemv.MapIndex(keyv)
			fields[i] = self.ColumnLike(to.String(keyv.Interface()))
			values[i] = convertFn(valv.Interface())
		}
	default:
		return nil, nil, fmt.Errorf("Expecting Struct or Map, received %v.", itemt.Kind())
	}

	return fields, values, nil
}

/*
	Converts a Go value into internal database representation.
*/
func (self *Table) ToInternal(val interface{}) string {
	return to.String(val)
}

func Fetch(dst interface{}, item db.Item) error {

	/*
		At this moment it is not possible to create a slice of a given element
		type: https://code.google.com/p/go/issues/detail?id=2339

		When it gets available this function should change, it must rely on
		FetchAll() the same way Find() relies on FindAll().
	*/

	dstv := reflect.ValueOf(dst)

	if dstv.Kind() != reflect.Ptr || dstv.IsNil() {
		return fmt.Errorf("Fetch() expects a pointer.")
	}

	el := dstv.Elem().Type()

	switch el.Kind() {
	case reflect.Struct:
		for column, _ := range item {
			f := func(s string) bool {
				return compareColumnToField(s, column)
			}
			v := dstv.Elem().FieldByNameFunc(f)
			if v.IsValid() {
				v.Set(reflect.ValueOf(item[column]))
			}
		}
	case reflect.Map:
		dstv.Elem().Set(reflect.ValueOf(item))
	default:
		return fmt.Errorf("Expecting a pointer to map or struct, got %s.", el.Kind())
	}

	return nil
}

func NewQueryChunks() *QueryChunks {
	self := &QueryChunks{
		Relate:    make(db.Relate),
		RelateAll: make(db.RelateAll),
	}
	return self
}

func (self *Table) FetchRelations(dst interface{}, queryChunks *QueryChunks, convertFn func(interface{}) string) error {
	var err error

	var dstv reflect.Value
	var itemv reflect.Value
	var itemk reflect.Kind

	// Checking input
	dstv = reflect.ValueOf(dst)

	if dstv.Kind() != reflect.Ptr || dstv.IsNil() || dstv.Elem().Kind() != reflect.Slice {
		return errors.New("FetchAll() expects a pointer to slice.")
	}

	itemv = dstv.Elem()
	itemk = itemv.Type().Elem().Kind()

	if itemk != reflect.Struct && itemk != reflect.Map {
		return errors.New("FetchAll() expects a pointer to slice of maps or structs.")
	}

	if len(queryChunks.Relations) > 0 {

		// Iterate over results.
		for i := 0; i < dstv.Elem().Len(); i++ {

			item := itemv.Index(i)

			for _, relation := range queryChunks.Relations {

				terms := make([]interface{}, len(relation.On))

				for j, term := range relation.On {
					switch t := term.(type) {
					// Just waiting for db.Cond statements.
					case db.Cond:
						for k, v := range t {
							switch s := v.(type) {
							case string:
								matches := extRelationPattern.FindStringSubmatch(s)
								if len(matches) > 1 {
									extkey := matches[1]
									var val reflect.Value
									switch itemk {
									case reflect.Struct:
										f := func(s string) bool {
											return compareColumnToField(s, extkey)
										}
										val = item.FieldByNameFunc(f)
									case reflect.Map:
										val = item.MapIndex(reflect.ValueOf(extkey))
									}
									if val.IsValid() {
										term = db.Cond{k: convertFn(val.Interface())}
									}
								}
							}
						}
					case db.Collection:
						relation.Collection = t
					}
					terms[j] = term
				}

				keyv := reflect.ValueOf(relation.Name)

				switch itemk {
				case reflect.Struct:

					f := func(s string) bool {
						return compareColumnToField(s, relation.Name)
					}

					val := item.FieldByNameFunc(f)

					if val.IsValid() {
						p := reflect.New(val.Type())
						q := p.Interface()
						if relation.All == true {
							err = relation.Collection.FetchAll(q, terms...)
						} else {
							err = relation.Collection.Fetch(q, terms...)
						}
						if err != nil {
							return err
						}
						val.Set(reflect.Indirect(p))
					}
				case reflect.Map:
					// Executing external query.
					if relation.All == true {
						item.SetMapIndex(keyv, reflect.ValueOf(relation.Collection.FindAll(terms...)))
					} else {
						item.SetMapIndex(keyv, reflect.ValueOf(relation.Collection.Find(terms...)))
					}
				}

			}
		}
	}
	return nil
}

func ValidateDestination(dst interface{}) error {

	var dstv reflect.Value
	var itemv reflect.Value
	var itemk reflect.Kind

	// Checking input
	dstv = reflect.ValueOf(dst)

	if dstv.Kind() != reflect.Ptr || dstv.IsNil() || dstv.Elem().Kind() != reflect.Slice {
		return errors.New("FetchAll() expects a pointer to slice.")
	}

	itemv = dstv.Elem()
	itemk = itemv.Type().Elem().Kind()

	if itemk != reflect.Struct && itemk != reflect.Map {
		return errors.New("FetchAll() expects a pointer to slice of maps or structs.")
	}

	return nil
}
