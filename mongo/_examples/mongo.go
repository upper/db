package main

import (
	"fmt"
	"menteslibres.net/gosexy/db"
	"menteslibres.net/gosexy/db/mongo"
	"github.com/kr/pretty"
)

func main() {
	sess := mongo.Session(db.DataSource{Host: "10.0.0.11", Database: "gotest"})

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
