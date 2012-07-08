package db

import (
	"fmt"
	"github.com/kr/pretty"
	"math/rand"
	"testing"
)

const sqDatabase = "./dumps/gotest.sqlite3.db"

func TestSqTruncate(t *testing.T) {

	db := NewSqliteDB(&DataSource{Database: sqDatabase})

	err := db.Connect()

	if err != nil {
		panic(err)
	}

	collections := db.Collections()

	for _, name := range collections {
		col := db.Collection(name)
		col.Truncate()
		if col.Count() != 0 {
			t.Errorf("Could not truncate '%s'.", name)
		}
	}

}

func TestSqAppend(t *testing.T) {

	db := NewSqliteDB(&DataSource{Database: sqDatabase})

	err := db.Connect()

	if err != nil {
		panic(err)
	}

	col := db.Collection("people")

	col.Truncate()

	names := []string{"Juan", "José", "Pedro", "María", "Roberto", "Manuel", "Miguel"}

	for i := 0; i < len(names); i++ {
		col.Append(Item{"name": names[i]})
	}

	if col.Count() != len(names) {
		panic(fmt.Errorf("Could not append all items"))
	}

}

func TestSqFind(t *testing.T) {

	db := NewSqliteDB(&DataSource{Database: sqDatabase})

	err := db.Connect()

	if err != nil {
		panic(err)
	}

	col := db.Collection("people")

	result := col.Find(Where{"name": "José"})

	if result["name"] != "José" {
		t.Error("Could not find a recently appended item.")
	}

}

func TestSqDelete(t *testing.T) {
	db := NewSqliteDB(&DataSource{Database: sqDatabase})

	err := db.Connect()

	if err != nil {
		panic(err)
	}

	col := db.Collection("people")

	// Remove() may not always work http://www.sqlite.org/compile.html#enable_update_delete_limit
	col.RemoveAll(Where{"name": "Juan"})

	result := col.Find(Where{"name": "Juan"})

	if len(result) > 0 {
		t.Error("Could not remove a recently appended item.")
	}
}

func TestSqUpdate(t *testing.T) {
	db := NewSqliteDB(&DataSource{Database: sqDatabase})

	err := db.Connect()

	if err != nil {
		panic(err)
	}

	col := db.Collection("people")

	// Update() may not always work http://www.sqlite.org/compile.html#enable_update_delete_limit
	col.UpdateAll(Where{"name": "José"}, Set{"name": "Joseph"})

	result := col.Find(Where{"name": "Joseph"})

	if len(result) == 0 {
		t.Error("Could not update a recently appended item.")
	}
}

func TestSqPopulate(t *testing.T) {
	var i int

	db := NewSqliteDB(&DataSource{Database: sqDatabase})

	err := db.Connect()

	if err != nil {
		panic(err)
	}

	db.Use("test")

	places := []string{"Alaska", "Nebraska", "Alaska", "Acapulco", "Rome", "Singapore", "Alabama", "Cancún"}

	for i = 0; i < len(places); i++ {
		db.Collection("places").Append(Item{
			"code_id": i,
			"name":    places[i],
		})
	}

	people := db.Collection("people").FindAll(
		Fields{"id", "name"},
	)

	for i = 0; i < len(people); i++ {
		person := people[i]

		// Has 5 children.
		for j := 0; j < 5; j++ {
			db.Collection("children").Append(Item{
				"name":      fmt.Sprintf("%s's child %d", person["name"], j+1),
				"parent_id": person["id"],
			})
		}

		// Lives in
		db.Collection("people").UpdateAll(
			Where{"id": person["id"]},
			Set{"place_code_id": int(rand.Float32() * float32(len(places)))},
		)

		// Has visited
		for k := 0; k < 3; k++ {
			place := db.Collection("places").Find(Where{
				"code_id": int(rand.Float32() * float32(len(places))),
			})
			db.Collection("visits").Append(Item{
				"place_id":  place["id"],
				"person_id": person["id"],
			})
		}
	}

}

func TestSqRelation(t *testing.T) {
	db := NewSqliteDB(&DataSource{Database: sqDatabase})

	err := db.Connect()

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
				Where{"parent_id": "{id}"},
			},
			"has_visited": On{
				db.Collection("visits"),
				Where{"person_id": "{id}"},
				Relate{
					"place": On{
						db.Collection("places"),
						Where{"id": "{place_id}"},
					},
				},
			},
		},
	)

	fmt.Printf("%# v\n", pretty.Formatter(result))
}
