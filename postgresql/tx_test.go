// Copyright (c) 2012-2015 The upper.io/db authors. All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package postgresql

import (
	"database/sql"
	"testing"

	"upper.io/db"
)

// TestInjectExternalTx tests injecting a transaction into upper.io/db.
func TestInjectExternalTx(t *testing.T) {
	sqlDB, err := sql.Open("postgres", settings.String())
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()

	// Begin transaction.
	sqlTx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlTx.Rollback()

	// Insert one row using the sqlTx transaction.
	res, err := sqlTx.Exec(`INSERT INTO artist (id,name) VALUES (1977,'Elvis Pressly');`)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatal("expected to affect 1 row")
	}

	sess, err := db.Open(Adapter, settings)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	// upper/db session without the injected transaction shouldn't see Elvis.
	{
		artist, err := sess.Collection("artist")
		if err != nil {
			t.Fatal(err)
		}
		count, err := artist.Find(db.Cond{"name": "Elvis Pressly"}).Count()
		if err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("expected 0 row, got %v", count)
		}
	}

	// upper/db session with injected transaction should see Elvis.
	{
		sessTx, err := sess.WithSession(sqlTx)
		if err != nil {
			t.Fatal(err)
		}
		artist, err := sessTx.Collection("artist")
		if err != nil {
			t.Fatal(err)
		}
		count, err := artist.Find(db.Cond{"name": "Elvis Pressly"}).Count()
		if err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("expected 1 row, got %v", count)
		}
	}
}
