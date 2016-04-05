package main

import (
	"log"

	"upper.io/db.v2"
	"upper.io/db.v2/postgresql"
)

var settings = postgresql.ConnectionURL{
	Host:     "demo.upper.io",
	Database: "booktown",
	User:     "demouser",
	Password: "demop4ss",
}

type Book struct {
	ID        int    `db:"id"`
	Title     string `db:"title"`
	AuthorID  int    `db:"author_id"`
	SubjectID int    `db:"subject_id"`
}

func main() {
	sess, err := db.Open("postgresql", settings)
	if err != nil {
		log.Fatalf("db.Open(): %q\n", err)
	}

	defer sess.Close()

	booksCol := sess.Collection("books")

	var books []Book
	err = booksCol.Find().All(&books)
	if err != nil {
		log.Fatalf("Find(): %q\n", err)
	}

	for i, book := range books {
		log.Printf("Book %d: %#v\n", i, book)
	}
}
