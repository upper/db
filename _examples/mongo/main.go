package main

import (
	"fmt"
	"time"
	"upper.io/db"
	_ "upper.io/db/mongo"
)

var settings = db.Settings{
	Database: `upperio_tests`,
	Host:     `127.0.0.1`,
}

type Birthday struct {
	Name string    `bson:"name"`
	Born time.Time `bson:"born"`
}

func main() {

	sess, err := db.Open("mongo", settings)

	if err != nil {
		fmt.Println("Unable to connect:", err.Error())
		return
	}

	defer sess.Close()

	birthdayCollection, err := sess.Collection("birthdays")

	if err != nil {
		if err != db.ErrCollectionDoesNotExists {
			fmt.Println("Could not use collection:", err.Error())
			return
		}
	} else {
		err = birthdayCollection.Truncate()

		if err != nil {
			fmt.Println(err.Error())
			return
		}
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

	res, err = birthdayCollection.Find()

	if err != nil {
		panic(err.Error())
	}

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
