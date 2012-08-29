package mongo

import (
	"fmt"
	"github.com/gosexy/db"
	"github.com/kr/pretty"
	"math/rand"
	"testing"
)

const mgHost = "10.0.0.11"
const mgDatabase = "gotest"

func TestMgOpen(t *testing.T) {

	sess := Session(db.DataSource{Host: "0.0.0.0"})

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

	if col.Count() != len(names) {
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
	sess := Session(db.DataSource{Host: mgHost, Database: mgDatabase})

	err := sess.Open()
	defer sess.Close()

	if err != nil {
		panic(err)
	}

	col := sess.Collection("people")

	col.Remove(db.Cond{"name": "Juan"})

	result := col.Find(db.Cond{"name": "Juan"})

	if len(result) > 0 {
		t.Error("Could not remove a recently appended item.")
	}
}

func TestMgUpdate(t *testing.T) {
	sess := Session(db.DataSource{Host: mgHost, Database: mgDatabase})

	err := sess.Open()
	defer sess.Close()

	if err != nil {
		panic(err)
	}

	col := sess.Collection("people")

	col.Update(db.Cond{"name": "José"}, db.Set{"name": "Joseph"})

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
