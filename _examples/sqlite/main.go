package main

import (
	"fmt"
	"github.com/gosexy/db"
	_ "github.com/gosexy/db/sqlite"
)

var settings = db.DataSource{
	Database: "animals.db",
}

func main() {

	sess, err := db.Open("sqlite", settings)

	if err != nil {
		fmt.Println("Please create the `animals.db` sqlite3 database.")
		return
	}

	defer sess.Close()

	animals, err := sess.Collection("animals")

	if err != nil {
		fmt.Println("Please create the `animals` table and make sure the `animals.db` sqlite3 database exists.")
		return
	}

	animals.Truncate()

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
		"young":  "Puppy, Pup",
		"female": "Bitch",
		"male":   "Dog",
		"group":  "Pack",
	})

	items := animals.FindAll()

	for _, item := range items {
		fmt.Printf("animal: %s, young: %s\n", item["animal"], item["young"])
	}

}
