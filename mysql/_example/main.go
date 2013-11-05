package main

import (
	"fmt"
	"time"
	"upper.io/db"
	_ "upper.io/db/mysql"
)

var settings = db.Settings{
	Database: `upperio_tests`,
	Socket:   `/var/run/mysqld/mysqld.sock`,
	User:     `upperio`,
	Password: `upperio`,
}

type Birthday struct {
	Name string    `field:"name"`
	Born time.Time `field:"born"`
}

func main() {

	sess, err := db.Open("mysql", settings)

	if err != nil {
		fmt.Println("Unable to connect:", err.Error())
		return
	}

	defer sess.Close()

	birthdayCollection, err := sess.Collection("birthdays")

	if err != nil {
		fmt.Println("Could not use collection:", err.Error())
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

	err = res.All(&birthdays)

	if err != nil {
		panic(err.Error())
		return
	}

	for _, birthday := range birthdays {
		fmt.Printf("%s was born in %s.\n", birthday.Name, birthday.Born.Format("January 2, 2006"))
	}

}
