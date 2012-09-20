package mysql

import (
	"database/sql"
	"fmt"
	"github.com/gosexy/db"
	"github.com/gosexy/sugar"
	"github.com/kr/pretty"
	"math/rand"
	"testing"
	"time"
)

const myHost = "debian"
const myDatabase = "gotest"
const myUser = "gouser"
const myPassword = "gopass"

func getTestData() db.Item {

	_time, _ := time.ParseDuration("17h20m")

	data := db.Item{
		"_uint":    uint(1),
		"_uintptr": uintptr(1),

		"_uint8":  uint8(1),
		"_uint16": uint16(1),
		"_uint32": uint32(1),
		"_uint64": uint64(1),

		"_int":   int(-1),
		"_int8":  int8(-1),
		"_int16": int16(-1),
		"_int32": int32(-1),
		"_int64": int64(-1),

		"_float32": float32(1.0),
		"_float64": float64(1.0),

		//"_complex64": complex64(1),
		//"_complex128": complex128(1),

		"_byte": byte(1),
		"_rune": rune(1),

		"_bool":   bool(true),
		"_string": string("abc"),
		"_bytea":  []byte{'a', 'b', 'c'},

		//"_list": sugar.List{1, 2, 3},
		//"_map":  sugar.Tuple{"a": 1, "b": 2, "c": 3},

		"_date": time.Date(2012, 7, 28, 1, 2, 3, 0, time.UTC),
		"_time": _time,
	}

	return data
}

func TestMyTruncate(t *testing.T) {

	sess := Session(db.DataSource{Host: myHost, Database: myDatabase, User: myUser, Password: myPassword})

	err := sess.Open()
	defer sess.Close()

	if err != nil {
		panic(err)
	}

	collections := sess.Collections()

	for _, name := range collections {
		col := sess.Collection(name)
		col.Truncate()

		total, _ := col.Count()

		if total != 0 {
			t.Errorf("Could not truncate '%s'.", name)
		}
	}

}

func TestMyAppend(t *testing.T) {

	sess := Session(db.DataSource{Host: myHost, Database: myDatabase, User: myUser, Password: myPassword})

	err := sess.Open()
	defer sess.Close()

	if err != nil {
		panic(err)
	}

	col := sess.Collection("people")

	col.Truncate()

	names := []string{"Juan", "José", "Pedro", "María", "Roberto", "Manuel", "Miguel"}

	for i := 0; i < len(names); i++ {
		col.Append(db.Item{"name": names[i]})
	}

	total, _ := col.Count()

	if total != len(names) {
		t.Error("Could not append all items.")
	}

}

func TestMyFind(t *testing.T) {

	sess := Session(db.DataSource{Host: myHost, Database: myDatabase, User: myUser, Password: myPassword})

	err := sess.Open()
	defer sess.Close()

	if err != nil {
		panic(err)
	}

	col := sess.Collection("people")

	result := col.Find(db.Cond{"name": "José"})

	if result["name"] != "José" {
		t.Error("Could not find a recently appended item.")
	}

}

func TestMyDelete(t *testing.T) {
	sess := Session(db.DataSource{Host: myHost, Database: myDatabase, User: myUser, Password: myPassword})

	err := sess.Open()
	defer sess.Close()

	if err != nil {
		panic(err)
	}

	col := sess.Collection("people")

	col.Remove(db.Cond{"name": "Juan"})

	result := col.Find(db.Cond{"name": "Juan"})

	if len(result) > 0 {
		t.Error("Could not remove a recently appended item.")
	}
}

func TestMyUpdate(t *testing.T) {
	sess := Session(db.DataSource{Host: myHost, Database: myDatabase, User: myUser, Password: myPassword})

	err := sess.Open()
	defer sess.Close()

	if err != nil {
		panic(err)
	}

	sess.Use("test")

	col := sess.Collection("people")

	col.Update(db.Cond{"name": "José"}, db.Set{"name": "Joseph"})

	result := col.Find(db.Cond{"name": "Joseph"})

	if len(result) == 0 {
		t.Error("Could not update a recently appended item.")
	}
}

func TestMyPopulate(t *testing.T) {
	var i int

	sess := Session(db.DataSource{Host: myHost, Database: myDatabase, User: myUser, Password: myPassword})

	err := sess.Open()
	defer sess.Close()

	if err != nil {
		panic(err)
	}

	sess.Use("test")

	places := []string{"Alaska", "Nebraska", "Alaska", "Acapulco", "Rome", "Singapore", "Alabama", "Cancún"}

	for i = 0; i < len(places); i++ {
		sess.Collection("places").Append(db.Item{
			"code_id": i,
			"name":    places[i],
		})
	}

	people := sess.Collection("people").FindAll(
		db.Fields{"id", "name"},
	)

	for i = 0; i < len(people); i++ {
		person := people[i]

		// Has 5 children.
		for j := 0; j < 5; j++ {
			sess.Collection("children").Append(db.Item{
				"name":      fmt.Sprintf("%s's child %d", person["name"], j+1),
				"parent_id": person["id"],
			})
		}

		// Lives in
		sess.Collection("people").Update(
			db.Cond{"id": person["id"]},
			db.Set{"place_code_id": int(rand.Float32() * float32(len(places)))},
		)

		// Has visited
		for k := 0; k < 3; k++ {
			place := sess.Collection("places").Find(db.Cond{
				"code_id": int(rand.Float32() * float32(len(places))),
			})
			sess.Collection("visits").Append(db.Item{
				"place_id":  place["id"],
				"person_id": person["id"],
			})
		}
	}

}

func TestMyRelation(t *testing.T) {
	sess := Session(db.DataSource{Host: myHost, Database: myDatabase, User: myUser, Password: myPassword})

	err := sess.Open()
	defer sess.Close()

	if err != nil {
		panic(err)
	}

	col := sess.Collection("people")

	result := col.FindAll(
		db.Relate{
			"lives_in": db.On{
				sess.Collection("places"),
				db.Cond{"code_id": "{place_code_id}"},
			},
		},
		db.RelateAll{
			"has_children": db.On{
				sess.Collection("children"),
				db.Cond{"parent_id": "{id}"},
			},
			"has_visited": db.On{
				sess.Collection("visits"),
				db.Cond{"person_id": "{id}"},
				db.Relate{
					"place": db.On{
						sess.Collection("places"),
						db.Cond{"id": "{place_id}"},
					},
				},
			},
		},
	)

	fmt.Printf("%# v\n", pretty.Formatter(result))
}

func TestCustom(t *testing.T) {
	sess := Session(db.DataSource{Host: myHost, Database: myDatabase, User: myUser, Password: myPassword})

	err := sess.Open()
	defer sess.Close()

	if err != nil {
		panic(err)
	}

	_, err = sess.Driver().(*sql.DB).Query("SELECT NOW()")

	if err != nil {
		panic(err)
	}

}

func TestDataTypes(t *testing.T) {

	sess := Session(db.DataSource{Host: myHost, Database: myDatabase, User: myUser, Password: myPassword})

	err := sess.Open()

	if err == nil {
		defer sess.Close()
	}

	col := sess.Collection("data_types")

	col.Truncate()

	data := getTestData()

	err = col.Append(data)

	if err != nil {
		t.Errorf("Could not append test data.")
	}

	// Getting and reinserting.
	item := col.Find()

	err = col.Append(item)

	if err == nil {
		t.Errorf("Expecting duplicated-key error.")
	}

	delete(item, "id")

	err = col.Append(item)

	if err != nil {
		t.Errorf("Could not append second element.")
	}

	// Testing rows
	items := col.FindAll()

	for i := 0; i < len(items); i++ {

		item := items[i]

		for key, _ := range item {

			switch key {

			// Signed integers.
			case
				"_int",
				"_int8",
				"_int16",
				"_int32",
				"_int64":
				if item.GetInt(key) != int64(data["_int"].(int)) {
					t.Errorf("Wrong datatype %v.", key)
				}

			// Unsigned integers.
			case
				"_uint",
				"_uintptr",
				"_uint8",
				"_uint16",
				"_uint32",
				"_uint64",
				"_byte",
				"_rune":
				if item.GetInt(key) != int64(data["_uint"].(uint)) {
					t.Errorf("Wrong datatype %v.", key)
				}

			// Floating point.
			case "_float32":
			case "_float64":
				if item.GetFloat(key) != data["_float64"].(float64) {
					t.Errorf("Wrong datatype %v.", key)
				}

			// Boolean
			case "_bool":
				if item.GetBool(key) != data["_bool"].(bool) {
					t.Errorf("Wrong datatype %v.", key)
				}

			// String
			case "_string":
				if item.GetString(key) != data["_string"].(string) {
					t.Errorf("Wrong datatype %v.", key)
				}

			// Map
			case "_map":
				if item.GetTuple(key)["a"] != data["_map"].(sugar.Tuple)["a"] {
					t.Errorf("Wrong datatype %v.", key)
				}

			// Array
			case "_list":
				if item.GetList(key)[0] != data["_list"].(sugar.List)[0] {
					t.Errorf("Wrong datatype %v.", key)
				}

			// Time
			case "_time":
				if item.GetDuration(key).String() != data["_time"].(time.Duration).String() {
					t.Errorf("Wrong datatype %v.", key)
				}

			// Date
			case "_date":
				if item.GetDate(key).Equal(data["_date"].(time.Time)) == false {
					t.Errorf("Wrong datatype %v.", key)
				}
			}
		}
	}

}
