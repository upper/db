package main

import (
	"fmt"
	"log"
	"time"

	"upper.io/db"         // Imports the main db package.
	_ "upper.io/db/mysql" // Improts the mysql adapter.
)

var settings = db.Settings{
	Database: `upperio_tests`, // Database name
	Host:     `testserver.local`,
	User:     `upperio`, // Database username.
	Password: `upperio`, // Database password.
}

// Birthday struct example.
type Birthday struct {
	// Maps the "Name" property to the "name" column of the "birthdays" table.
	Name string `db:"name"`
	// Maps the "Born" property to the "born" column of the "birthdays" table.
	Born time.Time `db:"born"`
}

func main() {

	// Attemping to establish a connection to the database.
	sess, err := db.Open("mysql", settings)

	if err != nil {
		log.Fatalf("db.Open(): %q\n", err)
	}

	// Remember to close the database session.
	defer sess.Close()

	// Pointing to the "birthdays" table.
	birthdayCollection, err := sess.Collection("birthdays")

	if err != nil {
		log.Fatalf("sess.Collection(): %q\n", err)
	}

	// Attempt to remove existing rows (if any).
	err = birthdayCollection.Truncate()

	if err != nil {
		log.Fatalf("Truncate(): %q\n", err)
	}

	// Inserting some rows into the "birthdays" table.

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

	// Let's query for the results we've just inserted.
	var res db.Result

	res = birthdayCollection.Find()

	var birthdays []Birthday

	// Query all results and fill the birthdays variable with them.
	err = res.All(&birthdays)

	if err != nil {
		log.Fatalf("res.All(): %q\n", err)
	}

	// Printing to stdout.
	for _, birthday := range birthdays {
		fmt.Printf("%s was born in %s.\n", birthday.Name, birthday.Born.Format("January 2, 2006"))
	}

}
