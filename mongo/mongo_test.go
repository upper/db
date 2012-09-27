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

const mgHost = "debian"
const mgDatabase = "gotest"

func getTestData() db.Item {
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

func TestMgOpen(t *testing.T) {

	sess := Session(db.DataSource{Host: "1.1.1.1"})

	err := sess.Open()
	defer sess.Close()

	if err != nil {
		t.Logf("Got %t, this was intended.", err)
		return
	}

	t.Error("Are you serious?")
}

func TestMgAuthFail(t *testing.T) {

	sess := Session(db.DataSource{Host: mgHost, Database: mgDatabase, User: "unknown", Password: "fail"})

	err := sess.Open()
	defer sess.Close()

	if err != nil {
		t.Logf("Got %t, this was intended.", err)
		return
	}

	t.Error("Are you serious?")
}

func TestMgDrop(t *testing.T) {

	sess := Session(db.DataSource{Host: mgHost, Database: mgDatabase})

	err := sess.Open()
	defer sess.Close()

	if err != nil {
		panic(err)
	}

	sess.Drop()
}

func TestMgAppend(t *testing.T) {

	sess := Session(db.DataSource{Host: mgHost, Database: mgDatabase})

	err := sess.Open()
	defer sess.Close()

	if err != nil {
		panic(err)
	}

	col := sess.Collection("people")

	names := []string{"Juan", "José", "Pedro", "María", "Roberto", "Manuel", "Miguel"}

	for i := 0; i < len(names); i++ {
		col.Append(db.Item{"name": names[i]})
	}

	count, err := col.Count()

	if err != nil {
		t.Error("Failed to count on collection.")
	}

	if count != len(names) {
		t.Error("Could not append all items.")
	}

}

func TestMgFind(t *testing.T) {

	sess := Session(db.DataSource{Host: mgHost, Database: mgDatabase})

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

func TestMgDelete(t *testing.T) {
	var err error

	sess := Session(db.DataSource{Host: mgHost, Database: mgDatabase})

	err = sess.Open()
	defer sess.Close()

	if err != nil {
		panic(err)
	}

	col := sess.Collection("people")

	err = col.Remove(db.Cond{"name": "Juan"})

	if err != nil {
		t.Error("Failed to remove.")
	}

	result := col.Find(db.Cond{"name": "Juan"})

	if len(result) > 0 {
		t.Error("Could not remove a recently appended item.")
	}
}

func TestMgUpdate(t *testing.T) {
	var err error

	sess := Session(db.DataSource{Host: mgHost, Database: mgDatabase})

	err = sess.Open()
	defer sess.Close()

	if err != nil {
		panic(err)
	}

	col := sess.Collection("people")

	err = col.Update(db.Cond{"name": "José"}, db.Set{"name": "Joseph"})

	if err != nil {
		t.Error("Failed to update collection.")
	}

	result := col.Find(db.Cond{"name": "Joseph"})

	if len(result) == 0 {
		t.Error("Could not update a recently appended item.")
	}
}

func TestMgPopulate(t *testing.T) {
	var i int

	sess := Session(db.DataSource{Host: mgHost, Database: mgDatabase})

	err := sess.Open()
	defer sess.Close()

	if err != nil {
		panic(err)
	}

	places := []string{"Alaska", "Nebraska", "Alaska", "Acapulco", "Rome", "Singapore", "Alabama", "Cancún"}

	for i = 0; i < len(places); i++ {
		sess.Collection("places").Append(db.Item{
			"code_id": i,
			"name":    places[i],
		})
	}

	people := sess.Collection("people").FindAll()

	for i = 0; i < len(people); i++ {
		person := people[i]

		// Has 5 children.
		for j := 0; j < 5; j++ {
			sess.Collection("children").Append(db.Item{
				"name":      fmt.Sprintf("%s's child %d", person["name"], j+1),
				"parent_id": person["_id"],
			})
		}

		// Lives in
		sess.Collection("people").Update(
			db.Cond{"_id": person["_id"]},
			db.Set{"place_code_id": int(rand.Float32() * float32(len(places)))},
		)

		// Has visited
		for k := 0; k < 3; k++ {
			place := sess.Collection("places").Find(db.Cond{
				"code_id": int(rand.Float32() * float32(len(places))),
			})
			sess.Collection("visits").Append(db.Item{
				"place_id":  place["_id"],
				"person_id": person["_id"],
			})
		}
	}

}

func TestMgRelation(t *testing.T) {
	sess := Session(db.DataSource{Host: mgHost, Database: mgDatabase})

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
				db.Cond{"parent_id": "{_id}"},
			},
			"has_visited": db.On{
				sess.Collection("visits"),
				db.Cond{"person_id": "{_id}"},
				db.Relate{
					"place": db.On{
						sess.Collection("places"),
						db.Cond{"_id": "{place_id}"},
					},
				},
			},
		},
	)

	fmt.Printf("%# v\n", pretty.Formatter(result))
}

func TestDataTypes(t *testing.T) {

	sess := Session(db.DataSource{Host: mgHost, Database: mgDatabase})

	err := sess.Open()

	if err == nil {
		defer sess.Close()
	}

	col := sess.Collection("data_types")

	col.Truncate()

	data := getTestData()

	ids, err := col.Append(data)

	if err != nil {
		t.Errorf("Could not append test data.")
	}

	found, _ := col.Count(db.Cond{"_id": db.Id(ids[0])})

	if found == 0 {
		t.Errorf("Cannot find recently inserted item (by ID).")
	}

	// Getting and reinserting.

	item := col.Find()

	_, err = col.Append(item)

	if err == nil {
		t.Errorf("Expecting duplicated-key error.")
	}

	delete(item, "_id")

	_, err = col.Append(item)

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

			// Date
			case "_date":
				if item.GetDate(key).Equal(data["_date"].(time.Time)) == false {
					t.Errorf("Wrong datatype %v.", key)
				}
			}
		}
	}

}
