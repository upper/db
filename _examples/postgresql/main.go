package main

import (
	"fmt"
	"menteslibres.net/gosexy/db"
	_ "menteslibres.net/gosexy/db/postgresql"
)

var settings = db.DataSource{
	//Host:     "localhost",
	Socket:   "/var/run/postgresql/",
	Database: "gotest",
	User:     "gouser",
	Password: "gopass",
}

func main() {

	sess, err := db.Open("postgresql", settings)

	if err != nil {
		panic(err)
	}

	defer sess.Close()

	animals, err := sess.Collection("animals")

	if err != nil {
		fmt.Printf("Please create the `animals` table: %s", err.Error())
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

	items, err := animals.FindAll()

	if err != nil {
		panic(err.Error())
	}

	for _, item := range items {
		fmt.Printf("animal: %s, young: %s\n", item["animal"], item["young"])
	}

}
