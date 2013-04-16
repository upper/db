package postgresql

import (
	"fmt"
	"github.com/kr/pretty"
	"math/rand"
	"menteslibres.net/gosexy/db"
	"menteslibres.net/gosexy/dig"
	"menteslibres.net/gosexy/to"
	"reflect"
	"testing"
	"time"
)

const wrapperName = "postgresql"

const host = "127.0.0.1"
const socket = "/var/run/postgresql/"
const dbname = "gotest"
const username = "gouser"
const password = "gopass"

var settings = db.DataSource{
	Database: dbname,
	//Host: host,
	Socket:   socket,
	User:     username,
	Password: password,
}

// Structure for testing conversions.
type testValuesStruct struct {
	Uint   uint
	Uint8  uint8
	Uint16 uint16
	Uint32 uint32
	Uint64 uint64

	Int   int
	Int8  int8
	Int16 int16
	Int32 int32
	Int64 int64

	Float32 float32
	Float64 float64

	Bool   bool
	String string

	Date time.Time
	Time time.Duration
}

// Some test values.
var testValues = testValuesStruct{
	1, 1, 1, 1, 1,
	-1, -1, -1, -1, -1,
	1.337, 1.337,
	true,
	"Hello world!",
	time.Date(2012, 7, 28, 1, 2, 3, 0, time.UTC),
	time.Second * time.Duration(7331),
}

// Outputs some information to stdout, useful for development.
func TestEnableDebug(t *testing.T) {
	Debug = true
}

// Trying to open an empty datasource, must fail.
func TestOpenFailed(t *testing.T) {
	_, err := db.Open(wrapperName, db.DataSource{})

	if err == nil {
		t.Errorf("Could not open database.")
	}
}

// Truncates all collections/tables, one by one.
func TestTruncate(t *testing.T) {

	var err error

	sess, err := db.Open(wrapperName, settings)

	if err != nil {
		t.Fatalf(err.Error())
	}

	defer sess.Close()

	collections := sess.Collections()

	for _, name := range collections {
		col := sess.ExistentCollection(name)
		col.Truncate()

		total, err := col.Count()

		if err != nil {
			t.Fatalf(err.Error())
		}

		if total != 0 {
			t.Errorf("Could not truncate.")
		}
	}

}

// Appends maps and structs.
func TestAppend(t *testing.T) {

	sess, err := db.Open(wrapperName, settings)

	if err != nil {
		t.Fatalf(err.Error())
	}

	defer sess.Close()

	_, err = sess.Collection("doesnotexists")

	if err == nil {
		t.Fatalf("Collection should not exists.")
	}

	people := sess.ExistentCollection("people")

	// To be inserted
	names := []string{
		"Juan",
		"José",
		"Pedro",
		"María",
		"Roberto",
		"Manuel",
		"Miguel",
	}

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

// Tries to find and fetch rows.
func TestFind(t *testing.T) {

	var err error
	var res db.Result

	sess, err := db.Open(wrapperName, settings)

	if err != nil {
		t.Fatalf(err.Error())
	}

	defer sess.Close()

	people, _ := sess.Collection("people")

	// Testing Find()
	item, _ := people.Find(db.Cond{"name": "José"})

	if item["name"] != "José" {
		t.Fatalf("Could not find a recently appended item.")
	}

	// Fetch into map slice.
	dst := []map[string]string{}

	res, err = people.Query(db.Cond{"name": "José"})

	if err != nil {
		t.Fatalf(err.Error())
	}

	err = res.All(&dst)

	if err != nil {
		t.Fatalf(err.Error())
	}

	if len(dst) != 1 {
		t.Fatalf("Could not find a recently appended item.")
	}

	if dst[0]["name"] != "José" {
		t.Fatalf("Could not find a recently appended item.")
	}

	// Fetch into struct slice.
	dst2 := []struct{ Name string }{}

	res, err = people.Query(db.Cond{"name": "José"})

	if err != nil {
		t.Fatalf(err.Error())
	}

	err = res.All(&dst2)

	if err != nil {
		t.Fatalf(err.Error())
	}

	if len(dst2) != 1 {
		t.Fatalf("Could not find a recently appended item.")
	}

	if dst2[0].Name != "José" {
		t.Fatalf("Could not find a recently appended item.")
	}

	// Fetch into map.
	dst3 := map[string]interface{}{}

	res, err = people.Query(db.Cond{"name": "José"})

	if err != nil {
		t.Fatalf(err.Error())
	}

	err = res.One(&dst3)

	if err != nil {
		t.Fatalf(err.Error())
	}

	// Fetch into struct.
	dst4 := struct{ Name string }{}

	res, err = people.Query(db.Cond{"name": "José"})

	if err != nil {
		t.Fatalf(err.Error())
	}

	err = res.One(&dst4)

	if err != nil {
		t.Fatalf(err.Error())
	}

	if dst4.Name != "José" {
		t.Fatalf("Could not find a recently appended item.")
	}

	// Makes a query and stores the result
	res, err = people.Query(nil)

	if err != nil {
		t.Fatalf(err.Error())
	}

	dst5 := struct{ Name string }{}
	found := false

	for {
		err = res.Next(&dst5)
		if err != nil {
			break
		}
		if dst5.Name == "José" {
			found = true
		}
	}

	res.Close()

	if found == false {
		t.Fatalf("José was not found.")
	}

}

// Tests limit and offset.
func TestLimitOffset(t *testing.T) {

	var err error

	sess, err := db.Open(wrapperName, settings)

	if err != nil {
		t.Fatalf(err.Error())
	}

	defer sess.Close()

	people, _ := sess.Collection("people")

	items, _ := people.FindAll(db.Limit(2), db.Offset(1))

	if len(items) != 2 {
		t.Fatalf("Test failed")
	}

}

// Tries to delete rows.
func TestDelete(t *testing.T) {
	sess, err := db.Open(wrapperName, settings)

	if err != nil {
		t.Fatalf(err.Error())
	}

	defer sess.Close()

	people := sess.ExistentCollection("people")

	people.Remove(db.Cond{"name": "Juan"})

	result, _ := people.Find(db.Cond{"name": "Juan"})

	if len(result) > 0 {
		t.Fatalf("Could not remove a recently appended item.")
	}

}

// Tries to update rows.
func TestUpdate(t *testing.T) {
	var found int

	sess, err := db.Open(wrapperName, settings)

	if err != nil {
		t.Fatalf(err.Error())
	}

	defer sess.Close()

	people := sess.ExistentCollection("people")

	// Update with map.
	people.Update(db.Cond{"name": "José"}, db.Set{"name": "Joseph"})

	found, _ = people.Count(db.Cond{"name": "Joseph"})

	if found != 1 {
		t.Fatalf("Could not update a recently appended item.")
	}

	// Update with struct.
	people.Update(db.Cond{"name": "Joseph"}, struct{ Name string }{"José"})

	found, _ = people.Count(db.Cond{"name": "José"})

	if found != 1 {
		t.Fatalf("Could not update a recently appended item.")
	}
}

// Tries to add test data and relations.
func TestPopulate(t *testing.T) {

	sess, err := db.Open(wrapperName, settings)

	if err != nil {
		t.Errorf(err.Error())
	}

	defer sess.Close()

	people := sess.ExistentCollection("people")
	places := sess.ExistentCollection("places")
	children := sess.ExistentCollection("children")
	visits := sess.ExistentCollection("visits")

	values := []string{"Alaska", "Nebraska", "Alaska", "Acapulco", "Rome", "Singapore", "Alabama", "Cancún"}

	for i, value := range values {
		places.Append(db.Item{
			"code_id": i,
			"name":    value,
		})
	}

	results, _ := people.FindAll(
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
		for j := 0; j < 3; j++ {
			place, _ := places.Find(db.Cond{
				"code_id": int(rand.Float32() * float32(len(results))),
			})
			visits.Append(db.Item{
				"place_id":  place["id"],
				"person_id": person["id"],
			})
		}
	}

}

// Tests relations between collections.
func TestRelation(t *testing.T) {
	sess, err := db.Open(wrapperName, settings)

	if err != nil {
		t.Errorf(err.Error())
	}

	defer sess.Close()

	people, _ := sess.Collection("people")

	results, _ := people.FindAll(
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

	fmt.Printf("relations (1) %# v\n", pretty.Formatter(results))

	var testv string

	testv = dig.String(&results, 0, "lives_in", "name")

	if testv == "" {
		t.Fatalf("Test failed, expected some value.")
	}

	testv = dig.String(&results, 1, "has_children", 2, "name")

	if testv == "" {
		t.Fatalf("Test failed, expected some value.")
	}
}

// Tests relations between collections using structs.
func TestRelationStruct(t *testing.T) {
	var err error
	var res db.Result

	sess, err := db.Open(wrapperName, settings)

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

	res, err = people.Query(
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

	err = res.All(&results)

	if err != nil {
		t.Fatalf(err.Error())
	}

	fmt.Printf("relations (2) %# v\n", pretty.Formatter(results))
}

// Tests datatype conversions.
func TestDataTypes(t *testing.T) {
	var res db.Result
	var items []db.Item

	sess, err := db.Open(wrapperName, settings)

	if err != nil {
		t.Fatalf(err.Error())
	}

	defer sess.Close()

	dataTypes := sess.ExistentCollection("data_types")

	dataTypes.Truncate()

	ids, err := dataTypes.Append(testValues)

	if err != nil {
		t.Fatalf(err.Error())
	}

	found, err := dataTypes.Count(db.Cond{"id": db.Id(ids[0])})

	if err != nil {
		t.Fatalf(err.Error())
	}

	if found == 0 {
		t.Errorf("Expecting an item.")
	}

	// Getting and reinserting (a db.Item).
	item, _ := dataTypes.Find()

	_, err = dataTypes.Append(item)

	if err == nil {
		t.Fatalf("Expecting duplicated-key error.")
	}

	delete(item, "id")

	_, err = dataTypes.Append(item)

	if err != nil {
		t.Fatalf(err.Error())
	}

	// Testing date ranges
	items, err = dataTypes.FindAll(db.Cond{
		"_date": time.Now(),
	})

	if err != nil {
		t.Fatalf(err.Error())
	}

	if len(items) > 0 {
		t.Fatalf("Expecting no results.")
	}

	items, err = dataTypes.FindAll(db.Cond{
		"_date <=": time.Now(),
	})

	if err != nil {
		t.Fatalf(err.Error())
	}

	if len(items) != 2 {
		t.Fatalf("Expecting some results.")
	}

	// Testing struct
	sresults := []testValuesStruct{}

	res, err = dataTypes.Query()

	if err != nil {
		t.Fatalf(err.Error())
	}

	err = res.All(&sresults)

	if err != nil {
		t.Fatalf(err.Error())
	}

	// Testing struct equality
	for _, item := range sresults {
		if reflect.DeepEqual(item, testValues) == false {
			t.Errorf("Struct is different.")
		}
	}

	// Testing maps
	results, _ := dataTypes.FindAll()

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
				if to.Int64(item[key]) != testValues.Int64 {
					t.Fatalf("Wrong datatype %v.", key)
				}

			// Unsigned integers.
			case
				"_uint",
				"_uint8",
				"_uint16",
				"_uint32",
				"_uint64":
				if to.Uint64(item[key]) != testValues.Uint64 {
					t.Fatalf("Wrong datatype %v.", key)
				}

			// Floating point.
			case "_float32":
			case "_float64":
				if to.Float64(item[key]) != testValues.Float64 {
					t.Fatalf("Wrong datatype %v.", key)
				}

			// Boolean
			case "_bool":
				if to.Bool(item[key]) != testValues.Bool {
					t.Fatalf("Wrong datatype %v.", key)
				}

			// String
			case "_string":
				if to.String(item[key]) != testValues.String {
					t.Fatalf("Wrong datatype %v.", key)
				}

			// Date
			case "_date":
				if to.Time(item[key]).Equal(testValues.Date) == false {
					t.Fatalf("Wrong datatype %v.", key)
				}
			}
		}
	}

}
