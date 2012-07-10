package main

import (
	"fmt"
	"github.com/kr/pretty"
	. "github.com/xiam/gosexy/db"
)

func main() {
	db := MongoSession(DataSource{Host: "10.0.0.11", Database: "gotest"})

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
