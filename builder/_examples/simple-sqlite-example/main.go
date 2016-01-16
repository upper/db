package main

import (
	"database/sql"
	"log"
	"reflect"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"upper.io/db.v2/builder"
	"upper.io/db.v2/builder/sqlbuilder"
	"upper.io/db.v2/builder/template/sqlite"
)

// Book is a struct that uses the `db` tag to map each one of its fields to
// table columns.
type Book struct {
	// The ID fields uses "omitempty" to skip it when inserting it to database
	// whenever it has the zero value.
	ID         int       `db:"id,omitempty"`
	Title      string    `db:"title"`
	Author     string    `db:"author"`
	CatalogID  int       `db:"catalog_id"`
	CategoryID int       `db:"category_id"`
	DateAdded  time.Time `db:"date_added"`
}

func main() {
	var db *sql.DB
	var err error

	// Let's open a sqlite3 database.
	db, err = sql.Open("sqlite3", "./test.db")
	if err != nil {
		log.Fatal("sql.Open: %q", err)
	}

	// Then create a SQL builder with it. You'll need a template as well.
	bob, err := sqlbuilder.New(db, sqlite.Template)
	if err != nil {
		log.Fatal("sqlbuilder.New: %q", err)
	}

	// You can use the budilder to execute raw queries.
	_, err = bob.Exec(`
		DROP TABLE IF EXISTS books
	`)
	if err != nil {
		log.Fatal("db.Exec: %q", err)
	}

	_, err = bob.Exec(`
		CREATE TABLE books (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			title VARCHAR(255) NULL,
			author VARCHAR(255) NULL,
			catalog_id INT,
			category_id INT,
			date_added DATE NULL
		)
	`)
	if err != nil {
		log.Fatal("Exec: %q", err)
	}

	// Up to this point we only have an empty table, let's insert something into
	// it.  What about a book?
	book := Book{
		Title:      "To Kill a Mockingbird",
		Author:     "Harper Lee",
		CatalogID:  1235,
		CategoryID: 88,
		DateAdded:  time.Date(2011, time.January, 14, 11, 23, 55, 0, time.UTC),
	}

	// The Values() method takes the book struct and creates a full INSERT
	// statement with it.
	_, err = bob.InsertInto("books").Values(book).Exec()
	if err != nil {
		log.Fatal("Exec: %q", err)
	}

	log.Printf("book: %#v", book)

	// This Select() method specified only a few columns. The Where methods sets
	// the conditions for this selection.
	q := bob.Select("id").From("books").Where("author = ?", "Harper Lee")

	var partialBook Book

	// The Select() method has an Iterator(), this iterator let's you map
	// database rows to Go structs by using the All() or One() methods. In this
	// case we only need one row, so we use One().
	err = q.Iterator().One(&partialBook)
	if err != nil {
		log.Fatal("One: %q", err)
	}

	log.Printf("partialBook: %#v", partialBook)

	// Remember that we didn't set an ID for book, let's take it from the value
	// we've just got.
	book.ID = partialBook.ID

	if reflect.DeepEqual(partialBook, book) {
		log.Fatal("Expecting both books to be different but got %#v and %#v", partialBook, book)
	}

	// The partialBook struct we pulled in during the last query is empty,
	// except for the ID field. Let's select again except this time we want all
	// the columns.
	q = bob.SelectAllFrom("books").Where("id = ?", partialBook.ID)

	var completeBook Book

	// Use One() to execute the query and map the result to completeBook. Note
	// that One() takes a pointer.
	err = q.Iterator().One(&completeBook)
	if err != nil {
		log.Fatal("One: %q", err)
	}

	log.Printf("completeBook: %#v", completeBook)

	if !reflect.DeepEqual(completeBook, book) {
		log.Fatal("Expecting both books to have identical values but got %#v and %#v", completeBook, book)
	}

	// Now let's try to change the book's category_id to 42.
	//
	// Note that Set("category_id", 42) is just a shortcut for
	// Set("category_id = ?", 42)
	q2 := bob.Update("books").Set("category_id", 42)

	// Up to this point no query has been commited, we can continue chaining
	// methods to q2. How about constraining the update to the only book we have?
	//
	// Note again that Where("id", partialBook.ID) is a shortcut for
	// Where("id = ?", partialBook.ID)
	q2.Where("id", partialBook.ID)

	// Use the Exec() method in order to actually build and submit the query.
	_, err = q2.Exec()
	if err != nil {
		log.Fatal("Exec: %q", err)
	}

	// Remember "q"? "q" still represents a query, we can execute it again to
	// refresh our completeBook.
	err = q.Iterator().One(&completeBook)
	if err != nil {
		log.Fatal("Exec: %q", err)
	}

	log.Printf("completeBook: %v", completeBook)

	// Update is smart enough to accept maps as well.
	e1 := bob.Update("books").Set(map[string]int{
		"catalog_id":  41,
		"category_id": 889,
	}).Where("id", partialBook.ID)

	log.Printf("e1's actuals SQL: %s", e1)

	// Use Exec() to actually execute the update.
	_, err = e1.Exec()
	if err != nil {
		log.Fatal("Exec: %q", err)
	}

	// Use q again to update completeBook.
	err = q.Iterator().One(&completeBook)
	if err != nil {
		log.Fatal("Exec: %q", err)
	}

	log.Printf("completeBook: %#v", completeBook)

	// Enough of this, let's clean after ourselves.
	_, err = bob.DeleteFrom("books").Where("catalog_id = ? AND category_id = ?", 41, 889).Exec()
	if err != nil {
		log.Fatal("Exec: %q", err)
	}

	// We only played with one row so we should not have any other row on the
	// table.
	q = bob.Select(builder.Raw("COUNT(1)")).From("books")
	row, err := q.QueryRow()
	if err != nil {
		log.Fatal("QueryRow: %q", err)
	}

	// QueryRow returns a *sql.Row that you can use like you normally would.
	var n int
	row.Scan(&n)
	log.Printf("How many rows do we have? %d\n", n)
}
