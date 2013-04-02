package main

import (
	"fmt"
	"menteslibres.net/gosexy/db"
	"menteslibres.net/gosexy/db/sqlite"
	"time"
)

var settings = db.DataSource{
	Database: "animals.db",
}

func main() {

	sqlite.DateFormat = "2006-01-02"

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

	items, err := animals.FindAll()

	if err != nil {
		panic(err.Error())
	}

	for _, item := range items {
		fmt.Printf("animal: %s, young: %s\n", item["animal"], item["young"])
	}

	birthdays, err := sess.Collection("birthdays")

	if err != nil {
		fmt.Println("Please create the `birthdays` table and make sure the `animals.db` sqlite3 database exists.")
		return
	}

	birthdays.Append(db.Item{
		"name": "Joseph",
		"born": time.Date(1987, time.July, 28, 0, 0, 0, 0, time.UTC),
		"age":  26,
	})

	items, err = birthdays.FindAll()

	if err != nil {
		panic(err.Error())
	}

	for _, item := range items {
		fmt.Printf("name: %s, born: %s, age: %d\n", item["name"], item["born"], item["age"])
	}

}
