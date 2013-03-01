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

		"_byte": byte(1),
		"_rune": rune(1),

		"_bool":   bool(true),
		"_string": string("abc"),
		"_bytea":  []byte{'a', 'b', 'c'},

		"_date": time.Date(2012, 7, 28, 1, 2, 3, 0, time.UTC),
		"_time": _time,
	}

	return data
}

func TestEnableDebug(t *testing.T) {
	Debug = true
}

func TestOpenFailed(t *testing.T) {
	_, err := db.Open("sqlite", db.DataSource{})

	if err == nil {
		t.Fatalf("An error was expected.")
	}

}

func TestTruncate(t *testing.T) {

	sess, err := db.Open("sqlite", db.DataSource{Database: dbpath})

	if err != nil {
		t.Fatalf(err.Error())
	}

	defer sess.Close()

	collections := sess.Collections()

	for _, name := range collections {
		col := sess.ExistentCollection(name)
		col.Truncate()

		total, _ := col.Count()

		if total != 0 {
			t.Fatalf("Could not truncate table %s.", name)
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
		t.Fatalf("Collection should not exists.")
	}

	people := sess.ExistentCollection("people")

	// To be inserted
	names := []string{"Juan", "José", "Pedro", "María", "Roberto", "Manuel", "Miguel"}
	var total int

	// Append db.Item
	people.Truncate()

	for _, name := range names {
		people.Append(db.Item{"name": name})
	}

	total, _ = people.Count()

	if total != len(names) {
		t.Fatalf("Could not append all items.")
	}

	// Append map[string]string
	people.Truncate()

	for _, name := range names {
		people.Append(map[string]string{"name": name})
	}

	total, _ = people.Count()

	if total != len(names) {
		t.Fatalf("Could not append all items.")
	}

	// Append map[string]interface{}
	people.Truncate()

	for _, name := range names {
		people.Append(map[string]interface{}{"name": name})
	}

	total, _ = people.Count()

	if total != len(names) {
		t.Fatalf("Could not append all items.")
	}

	// Append struct
	people.Truncate()

	for _, name := range names {
		people.Append(struct{ Name string }{name})
	}

	total, _ = people.Count()

	if total != len(names) {
		t.Fatalf("Could not append all items.")
	}

}

func TestFind(t *testing.T) {

	var err error

	sess, err := db.Open("sqlite", db.DataSource{Database: dbpath})

	if err != nil {
		t.Fatalf(err.Error())
	}

	defer sess.Close()

	people, _ := sess.Collection("people")

	result := people.Find(db.Cond{"name": "José"})

	if result["name"] != "José" {
		t.Fatalf("Could not find a recently appended item.")
	}

	dst := []map[string]string{}

	err = people.FetchAll(&dst, db.Cond{"name": "José"})

	if err != nil {
		t.Fatalf(err.Error())
	}

	if len(dst) != 1 {
		t.Fatalf("Could not find a recently appended item.")
	}

	if dst[0]["name"] != "José" {
		t.Fatalf("Could not find a recently appended item.")
	}

	// Struct and FetchAll()
	dst2 := []struct{ Name string }{}

	err = people.FetchAll(&dst2, db.Cond{"name": "José"})

	if err != nil {
		t.Fatalf(err.Error())
	}

	if len(dst2) != 1 {
		t.Fatalf("Could not find a recently appended item.")
	}

	if dst2[0].Name != "José" {
		t.Fatalf("Could not find a recently appended item.")
	}

	// Map and Fetch()
	dst3 := map[string]interface{}{}

	err = people.Fetch(&dst3, db.Cond{"name": "José"})

	if err != nil {
		t.Fatalf(err.Error())
	}

	if dst3["name"] != "José" {
		t.Fatalf("Could not find a recently appended item.")
	}

	// Struct and Fetch()
	dst4 := struct{ Name string }{}

	err = people.Fetch(&dst4, db.Cond{"name": "José"})

	if err != nil {
		t.Fatalf(err.Error())
	}

	if dst4.Name != "José" {
		t.Fatalf("Could not find a recently appended item.")
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
		t.Fatalf("Could not remove a recently appended item.")
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
		t.Fatalf("Could not update a recently appended item.")
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

func TestRelationStruct(t *testing.T) {
	var err error

	sess, err := db.Open("sqlite", db.DataSource{Database: dbpath})

	if err != nil {
		t.Errorf(err.Error())
	}

	defer sess.Close()

	people := sess.ExistentCollection("people")

	results := []struct {
		Id          int
		Name        string
		PlaceCodeId int
		LivesIn     struct {
			Name string
		}
		HasChildren []struct {
			Name string
		}
		HasVisited []struct {
			PlaceId int
			Place   struct {
				Name string
			}
		}
	}{}

	err = people.FetchAll(&results,
		db.Relate{
			"LivesIn": db.On{
				sess.ExistentCollection("places"),
				db.Cond{"code_id": "{PlaceCodeId}"},
			},
		},
		db.RelateAll{
			"HasChildren": db.On{
				sess.ExistentCollection("children"),
				db.Cond{"parent_id": "{Id}"},
			},
			"HasVisited": db.On{
				sess.ExistentCollection("visits"),
				db.Cond{"person_id": "{Id}"},
				db.Relate{
					"Place": db.On{
						sess.ExistentCollection("places"),
						db.Cond{"id": "{PlaceId}"},
					},
				},
			},
		},
	)

	if err != nil {
		t.Fatalf(err.Error())
	}

	fmt.Printf("STRUCT %# v\n", pretty.Formatter(results))
}

func TestDataTypes(t *testing.T) {

	sess, err := db.Open("sqlite", db.DataSource{Database: dbpath})

	if err != nil {
		t.Fatalf(err.Error())
		return
	}

	defer sess.Close()

	dataTypes, _ := sess.Collection("data_types")

	dataTypes.Truncate()

	testData := testItem()

	ids, err := dataTypes.Append(testData)

	if err != nil {
		t.Fatalf("Could not append test data: %s.", err.Error())
	}

	found, _ := dataTypes.Count(db.Cond{"id": db.Id(ids[0])})

	if found == 0 {
		t.Fatalf("Cannot find recently inserted item (by ID).")
	}

	// Getting and reinserting.

	item := dataTypes.Find()

	_, err = dataTypes.Append(item)

	if err == nil {
		t.Fatalf("Expecting duplicated-key error.")
	}

	delete(item, "id")

	_, err = dataTypes.Append(item)

	if err != nil {
		t.Fatalf("Could not append second element: %s.", err.Error())
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
					t.Fatalf("Wrong datatype %v.", key)
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
					t.Fatalf("Wrong datatype %v.", key)
				}

			// Floating point.
			case "_float32":
			case "_float64":
				if item.GetFloat(key) != testData["_float64"].(float64) {
					t.Fatalf("Wrong datatype %v.", key)
				}

			// Boolean
			case "_bool":
				if item.GetBool(key) != testData["_bool"].(bool) {
					t.Fatalf("Wrong datatype %v.", key)
				}

			// String
			case "_string":
				if item.GetString(key) != testData["_string"].(string) {
					t.Fatalf("Wrong datatype %v.", key)
				}

			// Date
			case "_date":
				if item.GetDate(key).Equal(testData["_date"].(time.Time)) == false {
					t.Fatalf("Wrong datatype %v.", key)
				}
			}
		}
	}

}
