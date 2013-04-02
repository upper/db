package main

import (
	"database/sql"
	"fmt"
	"menteslibres.net/gosexy/db"
	_ "menteslibres.net/gosexy/db/mysql"
	"menteslibres.net/gosexy/db/util/sqlutil"
)

var settings = db.DataSource{
	//Host:     "localhost",
	Socket:   "/var/run/mysqld/mysqld.sock",
	Database: "gotest",
	User:     "gouser",
	Password: "gopass",
}

func main() {

	sess, err := db.Open("mysql", settings)

	if err != nil {
		panic(err)
	}

	defer sess.Close()

	animals, err := sess.Collection("animals")

	if err != nil {
		fmt.Printf("Please create the `animals` table.: %s", err.Error())
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

	// Custom SQL
	drv := sess.Driver().(*sql.DB)

	rows, err := drv.Query("SELECT * from animals")

	items = []db.Item{}

	// Empty virtual table
	vtable := &sqlutil.T{}
	vtable.FetchRows(&items, rows)

	for _, item := range items {
		fmt.Printf("animal: %s, young: %s\n", item["animal"], item["young"])
	}

}
