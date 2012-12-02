package main

import (
	"fmt"
	"github.com/gosexy/db"
	_ "github.com/gosexy/db/mongo"
	"github.com/gosexy/sugar"
)

const host = "debian"
const dbname = "dev"

func main() {

	sess, err := db.Open("mongo", db.DataSource{Host: host, Database: dbname})

	if err != nil {
		panic(err)
	}

	defer sess.Close()

	sess.Drop()

	animals, _ := sess.Collection("animals")

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
