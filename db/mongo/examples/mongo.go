package main

import (
	"fmt"
	"github.com/kr/pretty"
	"github.com/xiam/gosexy/db"
	"github.com/xiam/gosexy/db/mongo"
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
				db.Where{"code_id": "{place_code_id}"},
			},
		},
		db.RelateAll{
			"has_children": db.On{
				sess.Collection("children"),
				db.Where{"parent_id": "{_id}"},
			},
			"has_visited": db.On{
				sess.Collection("visits"),
				db.Where{"person_id": "{_id}"},
				db.Relate{
					"place": db.On{
						sess.Collection("places"),
						db.Where{"_id": "{place_id}"},
					},
				},
			},
		},
	)

	fmt.Printf("%# v\n", pretty.Formatter(result))
}
