package sqlite

import (
	"fmt"
	"github.com/gosexy/db"
	"github.com/kr/pretty"
	"math/rand"
	"testing"
	"time"
)

const dbpath = "./_dumps/gotest.sqlite3.db"

func testItem() db.Item {

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

func TestEnableDebug(t *testing.T) {
	Debug = true
}

func TestTruncate(t *testing.T) {

	sess, err := db.Open("sqlite", db.DataSource{Database: dbpath})

	if err != nil {
		panic(err)
	}

	defer sess.Close()

	collections := sess.Collections()

	for _, name := range collections {
		col := sess.ExistentCollection(name)
		col.Truncate()

		total, _ := col.Count()

		if total != 0 {
			t.Errorf("Could not truncate %s.", name)
		}
	}

}

func TestAppend(t *testing.T) {

	sess, err := db.Open("sqlite", db.DataSource{Database: dbpath})

	if err != nil {
		panic(err)
	}

	defer sess.Close()

	_, err = sess.Collection("doesnotexists")

	if err == nil {
		t.Errorf("Collection should not exists.")
		//return
	}

	people := sess.ExistentCollection("people")

	people.Truncate()

	names := []string{"Juan", "José", "Pedro", "María", "Roberto", "Manuel", "Miguel"}

	for i := 0; i < len(names); i++ {
		people.Append(db.Item{"name": names[i]})
	}

	total, _ := people.Count()

	if total != len(names) {
		t.Error("Could not append all items.")
	}

}

func TestFind(t *testing.T) {

	sess, err := db.Open("sqlite", db.DataSource{Database: dbpath})

	if err != nil {
		panic(err)
	}

	defer sess.Close()

	people, _ := sess.Collection("people")

	result := people.Find(db.Cond{"name": "José"})

	if result["name"] != "José" {
		t.Error("Could not find a recently appended item.")
	}

}

func TestDelete(t *testing.T) {
	sess, err := db.Open("sqlite", db.DataSource{Database: dbpath})

	if err != nil {
		panic(err)
	}

	defer sess.Close()

	people, _ := sess.Collection("people")

	people.Remove(db.Cond{"name": "Juan"})

	result := people.Find(db.Cond{"name": "Juan"})

	if len(result) > 0 {
		t.Error("Could not remove a recently appended item.")
	}

}

func TestUpdate(t *testing.T) {
	sess, err := db.Open("sqlite", db.DataSource{Database: dbpath})

	if err != nil {
		panic(err)
	}

	defer sess.Close()

	people, _ := sess.Collection("people")

	people.Update(db.Cond{"name": "José"}, db.Set{"name": "Joseph"})

	result := people.Find(db.Cond{"name": "Joseph"})

	if len(result) == 0 {
		t.Error("Could not update a recently appended item.")
	}
}

func TestPopulate(t *testing.T) {

	sess, err := db.Open("sqlite", db.DataSource{Database: dbpath})

	if err != nil {
		panic(err)
	}

	defer sess.Close()

	people, _ := sess.Collection("people")
	places, _ := sess.Collection("places")
	children, _ := sess.Collection("children")
	visits, _ := sess.Collection("visits")

	values := []string{"Alaska", "Nebraska", "Alaska", "Acapulco", "Rome", "Singapore", "Alabama", "Cancún"}

	for i, value := range values {
		places.Append(db.Item{
			"code_id": i,
			"name":    value,
		})
	}

	results := people.FindAll(
		db.Fields{"id", "name"},
		db.Sort{"name": "ASC", "id": -1},
	)

	for _, person := range results {

		// Has 5 children.

		for j := 0; j < 5; j++ {
			children.Append(db.Item{
				"name":      fmt.Sprintf("%s's child %d", person["name"], j+1),
				"parent_id": person["id"],
			})
		}

		// Lives in
		people.Update(
			db.Cond{"id": person["id"]},
			db.Set{"place_code_id": int(rand.Float32() * float32(len(results)))},
		)

		// Has visited
		for k := 0; k < 3; k++ {
			place := places.Find(db.Cond{
				"code_id": int(rand.Float32() * float32(len(results))),
			})
			visits.Append(db.Item{
				"place_id":  place["id"],
				"person_id": person["id"],
			})
		}
	}

}

func TestRelation(t *testing.T) {
	sess, err := db.Open("sqlite", db.DataSource{Database: dbpath})

	if err != nil {
		panic(err)
	}

	defer sess.Close()

	people, _ := sess.Collection("people")

	results := people.FindAll(
		db.Relate{
			"lives_in": db.On{
				sess.ExistentCollection("places"),
				db.Cond{"code_id": "{place_code_id}"},
			},
		},
		db.RelateAll{
			"has_children": db.On{
				sess.ExistentCollection("children"),
				db.Cond{"parent_id": "{id}"},
			},
			"has_visited": db.On{
				sess.ExistentCollection("visits"),
				db.Cond{"person_id": "{id}"},
				db.Relate{
					"place": db.On{
						sess.ExistentCollection("places"),
						db.Cond{"id": "{place_id}"},
					},
				},
			},
		},
	)

	fmt.Printf("%# v\n", pretty.Formatter(results))
}

func TestDataTypes(t *testing.T) {

	sess, err := db.Open("sqlite", db.DataSource{Database: dbpath})

	if err != nil {
		t.Errorf(err.Error())
		return
	}

	defer sess.Close()

	dataTypes, _ := sess.Collection("data_types")

	dataTypes.Truncate()

	testData := testItem()

	ids, err := dataTypes.Append(testData)

	if err != nil {
		t.Errorf("Could not append test data: %s.", err.Error())
	}

	found, _ := dataTypes.Count(db.Cond{"id": db.Id(ids[0])})

	if found == 0 {
		t.Errorf("Cannot find recently inserted item (by ID).")
	}

	// Getting and reinserting.

	item := dataTypes.Find()

	_, err = dataTypes.Append(item)

	if err == nil {
		t.Errorf("Expecting duplicated-key error.")
	}

	delete(item, "id")

	_, err = dataTypes.Append(item)

	if err != nil {
		t.Errorf("Could not append second element: %s.", err.Error())
	}

	// Testing rows

	results := dataTypes.FindAll()

	for _, item := range results {

		for key, _ := range item {

			switch key {

			// Signed integers.
			case
				"_int",
				"_int8",
				"_int16",
				"_int32",
				"_int64":
				if item.GetInt(key) != int64(testData["_int"].(int)) {
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
				if item.GetInt(key) != int64(testData["_uint"].(uint)) {
					t.Errorf("Wrong datatype %v.", key)
				}

			// Floating point.
			case "_float32":
			case "_float64":
				if item.GetFloat(key) != testData["_float64"].(float64) {
					t.Errorf("Wrong datatype %v.", key)
				}

			// Boolean
			case "_bool":
				if item.GetBool(key) != testData["_bool"].(bool) {
					t.Errorf("Wrong datatype %v.", key)
				}

			// String
			case "_string":
				if item.GetString(key) != testData["_string"].(string) {
					t.Errorf("Wrong datatype %v.", key)
				}

			/*
				// Map
				case "_map":
					if item.GetTuple(key)["a"] != testData["_map"].(sugar.Tuple)["a"] {
						t.Errorf("Wrong datatype %v.", key)
					}

				// Array
				case "_list":
					if item.GetList(key)[0] != testData["_list"].(sugar.List)[0] {
						t.Errorf("Wrong datatype %v.", key)
					}
			*/

			// Date
			case "_date":
				if item.GetDate(key).Equal(testData["_date"].(time.Time)) == false {
					t.Errorf("Wrong datatype %v.", key)
				}
			}
		}
	}

}
