package main

import (
	"fmt"
	"github.com/gosexy/db"
	_ "github.com/gosexy/db/mongo"
	"github.com/gosexy/sugar"
)

func main() {

	sess := db.Open("mongo", db.DataSource{Host: "127.0.0.1", Database: "gosexy-dev"})

	if sess == nil {
		panic("Could not open connection to MongoDB.")
	}

	defer sess.Close()

	sess.Drop()

	animals := sess.Collection("animals")

	animals.Append(db.Item{
		"animal": "Bird",
		"young":  "Chick",
		"female": "Hen",
		"male":   "Cock",
		"group":  "flock",
	})

	animals.Append(db.Item{
		"animal": "Bovidae",
		"young":  "Calf",
		"female": "Cow",
		"male":   "Bull",
		"group":  "Herd",
	})

	animals.Append(db.Item{
		"animal": "Canidae",
		"young":  sugar.List{"Puppy", "Pup"},
		"female": "Bitch",
		"male":   "Dog",
		"group":  "Pack",
	})

	items := animals.FindAll()

	for _, item := range items {
		fmt.Printf("animal: %s, young: %s\n", item["animal"], item["young"])
	}

}
