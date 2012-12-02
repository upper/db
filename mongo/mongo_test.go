package mongo

import (
	"fmt"
	"github.com/gosexy/db"
	"github.com/gosexy/sugar"
	"github.com/kr/pretty"
	"math/rand"
	"testing"
	"time"
)

const host = "debian"
const dbname = "gotest"

func testItem() db.Item {

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

		"_list": sugar.List{1, 2, 3},
		"_map":  sugar.Tuple{"a": 1, "b": 2, "c": 3},

		"_date": time.Date(2012, 7, 28, 0, 0, 0, 0, time.UTC),
	}

	return data
}

func TestOpen(t *testing.T) {

	sess, err := db.Open("mongo", db.DataSource{Host: "1.1.1.1"})

	if err != nil {
		t.Logf("Got %t, this was intended.", err)
		return
	}

	sess.Close()

	t.Errorf("Reached.")
}

func TestAuthFail(t *testing.T) {

	sess, err := db.Open("mongo", db.DataSource{Host: host, Database: dbname, User: "unknown", Password: "fail"})

	if err != nil {
		t.Logf("Got %t, this was intended.", err)
		return
	}

	sess.Close()

	t.Errorf("Reached.")
}

func TestDrop(t *testing.T) {

	sess, err := db.Open("mongo", db.DataSource{Host: host, Database: dbname})

	if err != nil {
		t.Errorf(err.Error())
		return
	}

	defer sess.Close()

	sess.Drop()
}

func TestAppend(t *testing.T) {

	sess, err := db.Open("mongo", db.DataSource{Host: host, Database: dbname})

	if err != nil {
		t.Errorf(err.Error())
		return
	}

	defer sess.Close()

	col, _ := sess.Collection("people")

	if col.Exists() == true {
		t.Errorf("Collection should not exists, yet.")
		return
	}

	names := []string{"Juan", "José", "Pedro", "María", "Roberto", "Manuel", "Miguel"}

	for i := 0; i < len(names); i++ {
		col.Append(db.Item{"name": names[i]})
	}

	if col.Exists() == false {
		t.Errorf("Collection should exists.")
		return
	}

	count, err := col.Count()

	if err != nil {
		t.Error("Failed to count on collection.")
	}

	if count != len(names) {
		t.Error("Could not append all items.")
	}

}

func TestFind(t *testing.T) {

	sess, err := db.Open("mongo", db.DataSource{Host: host, Database: dbname})

	if err != nil {
		t.Errorf(err.Error())
		return
	}

	defer sess.Close()

	people, _ := sess.Collection("people")

	result := people.Find(db.Cond{"name": "José"})

	if result["name"] != "José" {
		t.Error("Could not find a recently appended item.")
	}

}

func TestDelete(t *testing.T) {

	sess, err := db.Open("mongo", db.DataSource{Host: host, Database: dbname})

	if err != nil {
		t.Errorf(err.Error())
		return
	}

	defer sess.Close()

	people := sess.ExistentCollection("people")

	err = people.Remove(db.Cond{"name": "Juan"})

	if err != nil {
		t.Error("Failed to remove.")
	}

	result := people.Find(db.Cond{"name": "Juan"})

	if len(result) > 0 {
		t.Error("Could not remove a recently appended item.")
	}
}

func TestUpdate(t *testing.T) {
	sess, err := db.Open("mongo", db.DataSource{Host: host, Database: dbname})

	if err != nil {
		t.Errorf(err.Error())
		return
	}

	defer sess.Close()

	people, _ := sess.Collection("people")

	err = people.Update(db.Cond{"name": "José"}, db.Set{"name": "Joseph"})

	if err != nil {
		t.Error("Failed to update collection.")
	}

	result := people.Find(db.Cond{"name": "Joseph"})

	if len(result) == 0 {
		t.Error("Could not update a recently appended item.")
	}
}

func TestPopulate(t *testing.T) {

	sess, err := db.Open("mongo", db.DataSource{Host: host, Database: dbname})

	if err != nil {
		t.Errorf(err.Error())
		return
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
				"parent_id": person["_id"],
			})
		}

		// Lives in
		people.Update(
			db.Cond{"_id": person["_id"]},
			db.Set{"place_code_id": int(rand.Float32() * float32(len(results)))},
		)

		// Has visited
		for k := 0; k < 3; k++ {
			place := places.Find(db.Cond{
				"code_id": int(rand.Float32() * float32(len(results))),
			})
			visits.Append(db.Item{
				"place_id":  place["_id"],
				"person_id": person["_id"],
			})
		}
	}

}

func TestRelation(t *testing.T) {
	sess, err := db.Open("mongo", db.DataSource{Host: host, Database: dbname})

	if err != nil {
		t.Errorf(err.Error())
		return
	}

	defer sess.Close()

	people, _ := sess.Collection("people")
	places, _ := sess.Collection("places")
	children, _ := sess.Collection("children")
	visits, _ := sess.Collection("visits")

	result := people.FindAll(
		db.Relate{
			"lives_in": db.On{
				places,
				db.Cond{"code_id": "{place_code_id}"},
			},
		},
		db.RelateAll{
			"has_children": db.On{
				children,
				db.Cond{"parent_id": "{_id}"},
			},
			"has_visited": db.On{
				visits,
				db.Cond{"person_id": "{_id}"},
				db.Relate{
					"place": db.On{
						places,
						db.Cond{"_id": "{place_id}"},
					},
				},
			},
		},
	)

	fmt.Printf("%# v\n", pretty.Formatter(result))
}

func TestDataTypes(t *testing.T) {

	sess, err := db.Open("mongo", db.DataSource{Host: host, Database: dbname})

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
		t.Errorf("Could not append test data.")
	}

	found, _ := dataTypes.Count(db.Cond{"_id": db.Id(ids[0])})

	if found == 0 {
		t.Errorf("Cannot find recently inserted item (by ID).")
	}

	// Getting and reinserting.

	item := dataTypes.Find()

	_, err = dataTypes.Append(item)

	if err == nil {
		t.Errorf("Expecting duplicated-key error.")
	}

	delete(item, "_id")

	_, err = dataTypes.Append(item)

	if err != nil {
		t.Errorf("Could not append second element.")
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

			// Date
			case "_date":
				if item.GetDate(key).Equal(testData["_date"].(time.Time)) == false {
					t.Errorf("Wrong datatype %v.", key)
				}
			}
		}
	}

}
