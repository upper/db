package main

import (
	"fmt"
	"time"
	"upper.io/db"
	_ "upper.io/db/sqlite"
)

var settings = db.Settings{
	Database: `example.db`,
}

type Birthday struct {
	Name string    `field:"name"`
	Born time.Time `field:"born"`
}

func main() {

	sess, err := db.Open("sqlite", settings)

	if err != nil {
		fmt.Println("Please create the `example.db` sqlite3 database.")
		return
	}

	defer sess.Close()

	birthdayCollection, err := sess.Collection("birthdays")

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	err = birthdayCollection.Truncate()

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	birthdayCollection.Append(Birthday{
		Name: "Hayao Miyazaki",
		Born: time.Date(1941, time.January, 5, 0, 0, 0, 0, time.UTC),
	})

	birthdayCollection.Append(Birthday{
		Name: "Nobuo Uematsu",
		Born: time.Date(1959, time.March, 21, 0, 0, 0, 0, time.UTC),
	})

	birthdayCollection.Append(Birthday{
		Name: "Hironobu Sakaguchi",
		Born: time.Date(1962, time.November, 25, 0, 0, 0, 0, time.UTC),
	})

	var res db.Result

	res = birthdayCollection.Find()

	var birthdays []Birthday
	var birthday Birthday

	// Pulling all at once.
	err = res.All(&birthdays)

	if err != nil {
		panic(err.Error())
		return
	}

	for _, birthday = range birthdays {
		fmt.Printf("%s was born in %s.\n", birthday.Name, birthday.Born.Format("January 2, 2006"))
	}

	// Pulling one by one
	for {
		err = res.Next(&birthday)
		if err == nil {
			fmt.Printf("%s was born in %s.\n", birthday.Name, birthday.Born.Format("January 2, 2006"))
		} else if err == db.ErrNoMoreRows {
			break
		} else {
			panic(err.Error())
		}
	}

}
