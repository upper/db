package db

import (
	"fmt"
	"github.com/kr/pretty"
	"math/rand"
	"testing"
)

const mgHost = "10.0.0.11"
const mgDatabase = "gotest"

func TestMgOpen(t *testing.T) {

	db := MongoSession(DataSource{Host: "0.0.0.0"})

	err := db.Open()
	defer db.Close()

	if err != nil {
		t.Logf("Got %t, this was intended.", err)
		return
	}

	t.Error("Are you serious?")
}

func TestMgAuthFail(t *testing.T) {

	db := MongoSession(DataSource{Host: mgHost, Database: mgDatabase, User: "unknown", Password: "fail"})

	err := db.Open()
	defer db.Close()

	if err != nil {
		t.Logf("Got %t, this was intended.", err)
		return
	}

	t.Error("Are you serious?")
}

func TestMgDrop(t *testing.T) {

	db := MongoSession(DataSource{Host: mgHost, Database: mgDatabase})

	err := db.Open()
	defer db.Close()

	if err != nil {
		panic(err)
	}

	db.Drop()
}

func TestMgAppend(t *testing.T) {

	db := MongoSession(DataSource{Host: mgHost, Database: mgDatabase})

	err := db.Open()
	defer db.Close()

	if err != nil {
		panic(err)
	}

	col := db.Collection("people")

	names := []string{"Juan", "José", "Pedro", "María", "Roberto", "Manuel", "Miguel"}

	for i := 0; i < len(names); i++ {
		col.Append(Item{"name": names[i]})
	}

	if col.Count() != len(names) {
		t.Error("Could not append all items.")
	}

}

func TestMgFind(t *testing.T) {

	db := MongoSession(DataSource{Host: mgHost, Database: mgDatabase})

	err := db.Open()
	defer db.Close()

	if err != nil {
		panic(err)
	}

	col := db.Collection("people")

	result := col.Find(Where{"name": "José"})

	if result["name"] != "José" {
		t.Error("Could not find a recently appended item.")
	}

}

func TestMgDelete(t *testing.T) {
	db := MongoSession(DataSource{Host: mgHost, Database: mgDatabase})

	err := db.Open()
	defer db.Close()

	if err != nil {
		panic(err)
	}

	col := db.Collection("people")

	col.Remove(Where{"name": "Juan"})

	result := col.Find(Where{"name": "Juan"})

	if len(result) > 0 {
		t.Error("Could not remove a recently appended item.")
	}
}

func TestMgUpdate(t *testing.T) {
	db := MongoSession(DataSource{Host: mgHost, Database: mgDatabase})

	err := db.Open()
	defer db.Close()

	if err != nil {
		panic(err)
	}

	col := db.Collection("people")

	col.Update(Where{"name": "José"}, Set{"name": "Joseph"})

	result := col.Find(Where{"name": "Joseph"})

	if len(result) == 0 {
		t.Error("Could not update a recently appended item.")
	}
}

func TestMgPopulate(t *testing.T) {
	var i int

	db := MongoSession(DataSource{Host: mgHost, Database: mgDatabase})

	err := db.Open()
	defer db.Close()

	if err != nil {
		panic(err)
	}

	places := []string{"Alaska", "Nebraska", "Alaska", "Acapulco", "Rome", "Singapore", "Alabama", "Cancún"}

	for i = 0; i < len(places); i++ {
		db.Collection("places").Append(Item{
			"code_id": i,
			"name":    places[i],
		})
	}

	people := db.Collection("people").FindAll()

	for i = 0; i < len(people); i++ {
		person := people[i]

		// Has 5 children.
		for j := 0; j < 5; j++ {
			db.Collection("children").Append(Item{
				"name":      fmt.Sprintf("%s's child %d", person["name"], j+1),
				"parent_id": person["_id"],
			})
		}

		// Lives in
		db.Collection("people").Update(
			Where{"_id": person["_id"]},
			Set{"place_code_id": int(rand.Float32() * float32(len(places)))},
		)

		// Has visited
		for k := 0; k < 3; k++ {
			place := db.Collection("places").Find(Where{
				"code_id": int(rand.Float32() * float32(len(places))),
			})
			db.Collection("visits").Append(Item{
				"place_id":  place["_id"],
				"person_id": person["_id"],
			})
		}
	}

}

func TestMgRelation(t *testing.T) {
	db := MongoSession(DataSource{Host: mgHost, Database: mgDatabase})

	err := db.Open()
	defer db.Close()

	if err != nil {
		panic(err)
	}

	col := db.Collection("people")

	result := col.FindAll(
		Relate{
			"lives_in": On{
				db.Collection("places"),
				Where{"code_id": "{place_code_id}"},
			},
		},
		RelateAll{
			"has_children": On{
				db.Collection("children"),
				Where{"parent_id": "{_id}"},
			},
			"has_visited": On{
				db.Collection("visits"),
				Where{"person_id": "{_id}"},
				Relate{
					"place": On{
						db.Collection("places"),
						Where{"_id": "{place_id}"},
					},
				},
			},
		},
	)

	fmt.Printf("%# v\n", pretty.Formatter(result))
}
