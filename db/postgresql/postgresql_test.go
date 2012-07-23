package postgresql

import (
	"fmt"
	"github.com/kr/pretty"
	"github.com/xiam/gosexy/db"
	"math/rand"
	"testing"
)

const pgHost = "10.0.0.11"
const pgDatabase = "gotest"
const pgUser = "gouser"
const pgPassword = "gopass"

func TestPgTruncate(t *testing.T) {

	sess := Session(db.DataSource{Host: pgHost, Database: pgDatabase, User: pgUser, Password: pgPassword})

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

func TestPgAppend(t *testing.T) {

	sess := Session(db.DataSource{Host: pgHost, Database: pgDatabase, User: pgUser, Password: pgPassword})

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
		t.Error("Could not append all items.")
	}

}

func TestPgFind(t *testing.T) {

	sess := Session(db.DataSource{Host: pgHost, Database: pgDatabase, User: pgUser, Password: pgPassword})

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

func TestPgDelete(t *testing.T) {
	sess := Session(db.DataSource{Host: pgHost, Database: pgDatabase, User: pgUser, Password: pgPassword})

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

func TestPgUpdate(t *testing.T) {
	sess := Session(db.DataSource{Host: pgHost, Database: pgDatabase, User: pgUser, Password: pgPassword})

	err := sess.Open()
	defer sess.Close()

	if err != nil {
		panic(err)
	}

	sess.Use("gotest")

	col := sess.Collection("people")

	col.Update(db.Where{"name": "José"}, db.Set{"name": "Joseph"})

	result := col.Find(db.Where{"name": "Joseph"})

	if len(result) == 0 {
		t.Error("Could not update a recently appended item.")
	}
}

func TestPgPopulate(t *testing.T) {
	var i int

	sess := Session(db.DataSource{Host: pgHost, Database: pgDatabase, User: pgUser, Password: pgPassword})

	err := sess.Open()
	defer sess.Close()

	if err != nil {
		panic(err)
	}

	sess.Use("gotest")

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

func TestPgRelation(t *testing.T) {
	sess := Session(db.DataSource{Host: pgHost, Database: pgDatabase, User: pgUser, Password: pgPassword})

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
