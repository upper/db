package sqlite

import (
	"fmt"
	"github.com/kr/pretty"
	"github.com/xiam/gosexy/db"
	"math/rand"
	"testing"
)

const sqDatabase = "./dumps/gotest.sqlite3.db"

func TestSqTruncate(t *testing.T) {

	sess := SqliteSession(db.DataSource{Database: sqDatabase})

	err := sess.Open()
	defer sess.Close()

	if err != nil {
		panic(err)
	}

	collections := sess.Collections()

	for _, name := range collections {
		col := sess.Collection(name)
		col.Truncate()
		if col.Count() != 0 {
			t.Errorf("Could not truncate '%s'.", name)
		}
	}

}

func TestSqAppend(t *testing.T) {

	sess := SqliteSession(db.DataSource{Database: sqDatabase})

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

	if col.Count() != len(names) {
		panic(fmt.Errorf("Could not append all items"))
	}

}

func TestSqFind(t *testing.T) {

	sess := SqliteSession(db.DataSource{Database: sqDatabase})

	err := sess.Open()
	defer sess.Close()

	if err != nil {
		panic(err)
	}

	col := sess.Collection("people")

	result := col.Find(db.Where{"name": "José"})

	if result["name"] != "José" {
		t.Error("Could not find a recently appended item.")
	}

}

func TestSqDelete(t *testing.T) {
	sess := SqliteSession(db.DataSource{Database: sqDatabase})

	err := sess.Open()
	defer sess.Close()

	if err != nil {
		panic(err)
	}

	col := sess.Collection("people")

	col.Remove(db.Where{"name": "Juan"})

	result := col.Find(db.Where{"name": "Juan"})

	if len(result) > 0 {
		t.Error("Could not remove a recently appended item.")
	}
}

func TestSqUpdate(t *testing.T) {
	sess := SqliteSession(db.DataSource{Database: sqDatabase})

	err := sess.Open()
	defer sess.Close()

	if err != nil {
		panic(err)
	}

	col := sess.Collection("people")

	col.Update(db.Where{"name": "José"}, db.Set{"name": "Joseph"})

	result := col.Find(db.Where{"name": "Joseph"})

	if len(result) == 0 {
		t.Error("Could not update a recently appended item.")
	}
}

func TestSqPopulate(t *testing.T) {
	var i int

	sess := SqliteSession(db.DataSource{Database: sqDatabase})

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
			db.Where{"id": person["id"]},
			db.Set{"place_code_id": int(rand.Float32() * float32(len(places)))},
		)

		// Has visited
		for k := 0; k < 3; k++ {
			place := sess.Collection("places").Find(db.Where{
				"code_id": int(rand.Float32() * float32(len(places))),
			})
			sess.Collection("visits").Append(db.Item{
				"place_id":  place["id"],
				"person_id": person["id"],
			})
		}
	}

}

func TestSqRelation(t *testing.T) {
	sess := SqliteSession(db.DataSource{Database: sqDatabase})

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
				db.Where{"code_id": "{place_code_id}"},
			},
		},
		db.RelateAll{
			"has_children": db.On{
				sess.Collection("children"),
				db.Where{"parent_id": "{id}"},
			},
			"has_visited": db.On{
				sess.Collection("visits"),
				db.Where{"person_id": "{id}"},
				db.Relate{
					"place": db.On{
						sess.Collection("places"),
						db.Where{"id": "{place_id}"},
					},
				},
			},
		},
	)

	fmt.Printf("%# v\n", pretty.Formatter(result))
}
