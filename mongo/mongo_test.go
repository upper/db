package mongo

import (
	"fmt"
	"github.com/gosexy/db"
	"github.com/gosexy/to"
	"github.com/kr/pretty"
	"labix.org/v2/mgo/bson"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

const wrapperName = "mongo"

const host = "127.0.0.1"
const socket = "/tmp/mongodb-27017.sock"
const dbname = "gotest"
const username = "gouser"
const password = "gopass"

var settings = db.DataSource{
	Database: dbname,
	Host:     host,
	// https://bugs.launchpad.net/mgo/+bug/954436
	//Socket:   socket,

	//User:     username,
	//Password: password,
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
	time.Date(2012, 7, 28, 1, 2, 3, 0, time.Local),
	time.Second * time.Duration(7331),
}

// Outputs some information to stdout, useful for development.
func TestEnableDebug(t *testing.T) {
	Debug = true
}

/*
// Trying to open an empty datasource, must fail.
func TestOpenFailed(t *testing.T) {
	_, err := db.Open(wrapperName, db.DataSource{})

	if err == nil {
		t.Errorf("Could not open database.")
	}
}
*/

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
		col, _ := sess.Collection(name)
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

	people, _ := sess.Collection("people")

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

	sess, err := db.Open(wrapperName, settings)

	if err != nil {
		t.Fatalf(err.Error())
	}

	defer sess.Close()

	people := sess.ExistentCollection("people")

	// Testing Find()
	result := people.Find(db.Cond{"name": "José"})

	if result["name"] != "José" {
		t.Fatalf("Could not find a recently appended item.")
	}

	// Fetch into map slice.
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

	// Fetch into struct slice.
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

	// Fetch into map.
	dst3 := map[string]interface{}{}

	err = people.Fetch(&dst3, db.Cond{"name": "José"})

	if err != nil {
		t.Fatalf(err.Error())
	}

	if dst3["name"] != "José" {
		t.Fatalf("Could not find a recently appended item.")
	}

	// Fetch into struct.
	dst4 := struct{ Name string }{}

	err = people.Fetch(&dst4, db.Cond{"name": "José"})

	if err != nil {
		t.Fatalf(err.Error())
	}

	if dst4.Name != "José" {
		t.Fatalf("Could not find a recently appended item.")
	}

}

// Tries to delete rows.
func TestDelete(t *testing.T) {
	sess, err := db.Open(wrapperName, settings)

	if err != nil {
		t.Fatalf(err.Error())
	}

	defer sess.Close()

	people, _ := sess.Collection("people")

	people.Remove(db.Cond{"name": "Juan"})

	result := people.Find(db.Cond{"name": "Juan"})

	if len(result) > 0 {
		t.Fatalf("Could not remove a recently appended item.")
	}

}

// Tries to update rows.
func TestUpdate(t *testing.T) {
	sess, err := db.Open(wrapperName, settings)

	if err != nil {
		t.Fatalf(err.Error())
	}

	defer sess.Close()

	people, _ := sess.Collection("people")

	people.Update(db.Cond{"name": "José"}, db.Set{"name": "Joseph"})

	result := people.Find(db.Cond{"name": "Joseph"})

	if len(result) == 0 {
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
		db.Fields{"_id", "name"},
		db.Sort{"name": "ASC", "_id": -1},
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
		for j := 0; j < 3; j++ {
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

// Tests relations between collections.
func TestRelation(t *testing.T) {
	sess, err := db.Open(wrapperName, settings)

	if err != nil {
		t.Errorf(err.Error())
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
				db.Cond{"parent_id": "{_id}"},
			},
			"has_visited": db.On{
				sess.ExistentCollection("visits"),
				db.Cond{"person_id": "{_id}"},
				db.Relate{
					"place": db.On{
						sess.ExistentCollection("places"),
						db.Cond{"_id": "{place_id}"},
					},
				},
			},
		},
	)

	fmt.Printf("relations (1) %# v\n", pretty.Formatter(results))
}

// Tests relations between collections using structs.
func TestRelationStruct(t *testing.T) {
	var err error

	sess, err := db.Open(wrapperName, settings)

	if err != nil {
		t.Errorf(err.Error())
	}

	defer sess.Close()

	people := sess.ExistentCollection("people")

	results := []struct {
		Id          bson.ObjectId `_id`
		Name        string
		PlaceCodeId int `place_code_id`
		LivesIn     struct {
			Name string
		}
		HasChildren []struct {
			Name string
		}
		HasVisited []struct {
			PlaceId bson.ObjectId `place_id`
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
						db.Cond{"_id": "{PlaceId}"},
					},
				},
			},
		},
	)

	if err != nil {
		t.Fatalf(err.Error())
	}

	fmt.Printf("relations (2) %# v\n", pretty.Formatter(results))
}

// Tests datatype conversions.
func TestDataTypes(t *testing.T) {

	sess, err := db.Open(wrapperName, settings)

	if err != nil {
		t.Fatalf(err.Error())
	}

	defer sess.Close()

	dataTypes, _ := sess.Collection("data_types")

	dataTypes.Truncate()

	ids, err := dataTypes.Append(testValues)

	if err != nil {
		t.Fatalf(err.Error())
	}

	found, err := dataTypes.Count(db.Cond{"_id": db.Id(ids[0])})

	if err != nil {
		t.Fatalf(err.Error())
	}

	if found == 0 {
		t.Errorf("Expecting an item.")
	}

	// Getting and reinserting (a db.Item).
	item := dataTypes.Find()

	_, err = dataTypes.Append(item)

	if err == nil {
		t.Fatalf("Expecting duplicated-key error.")
	}

	delete(item, "_id")

	_, err = dataTypes.Append(item)

	if err != nil {
		t.Fatalf(err.Error())
	}

	// Testing struct
	sresults := []testValuesStruct{}
	err = dataTypes.FetchAll(&sresults)

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
