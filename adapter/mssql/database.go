// Copyright (c) 2012-present The upper.io/db authors. All rights reserved.
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

// Package mssql wraps the github.com/go-sql-driver/mssql MySQL driver. See
// https://github.com/upper/db/adapter/mssql for documentation, particularities and usage
// examples.
package mssql

import (
	"strings"

	"database/sql"

	_ "github.com/denisenkom/go-mssqldb" // MSSQL driver
	db "github.com/upper/db/v4"
	"github.com/upper/db/v4/internal/sqladapter"
	"github.com/upper/db/v4/internal/sqladapter/exql"
)

type database struct {
}

func (*database) Template() *exql.Template {
	return template
}

func (*database) OpenDSN(sess sqladapter.Session, dsn string) (*sql.DB, error) {
	return sql.Open("mssql", dsn)
}

func (*database) Collections(sess sqladapter.Session) (collections []string, err error) {
	q := sess.SQL().
		Select(`table_name`).
		From(`information_schema.tables`).
		Where(`table_type`, `BASE TABLE`).
		And(`table_catalog`, sess.Name())

	iter := q.Iterator()
	defer iter.Close()

	for iter.Next() {
		var tableName string
		if err := iter.Scan(&tableName); err != nil {
			return nil, err
		}
		collections = append(collections, tableName)
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}

	return collections, nil
}

func (*database) Err(err error) error {
	if err != nil {
		// This error is not exported so we have to check it by its string value.
		s := err.Error()
		if strings.Contains(s, `many connections`) {
			return db.ErrTooManyClients
		}
	}
	return err
}

func (*database) NewCollection() sqladapter.CollectionAdapter {
	return &collectionAdapter{}
}

func (*database) LookupName(sess sqladapter.Session) (string, error) {
	q := sess.SQL().
		Select(db.Raw(`DB_NAME() AS name`))

	iter := q.Iterator()
	defer iter.Close()

	if iter.Next() {
		var name string
		err := iter.Scan(&name)
		return name, err
	}

	return "", iter.Err()
}

func (*database) TableExists(sess sqladapter.Session, name string) error {
	q := sess.SQL().
		Select(`table_name`).
		From(`information_schema.tables`).
		Where(`table_schema`, sess.Name()).
		And(`table_name`, name)

	iter := q.Iterator()
	defer iter.Close()

	if iter.Next() {
		var name string
		if err := iter.Scan(&name); err != nil {
			return err
		}
		return nil
	}
	if err := iter.Err(); err != nil {
		return err
	}

	return db.ErrCollectionDoesNotExist
}

func (*database) PrimaryKeys(sess sqladapter.Session, tableName string) ([]string, error) {
	q := sess.SQL().
		Select(`k.column_name`).
		From(
			`information_schema.table_constraints AS t`,
			`information_schema.key_column_usage AS k`,
		).
		Where(`k.constraint_name = t.constraint_name`).
		And(`k.table_name = t.table_name`).
		And(`t.constraint_type = ?`, `PRIMARY KEY`).
		And(`t.table_name = ?`, tableName).
		OrderBy(`k.ordinal_position`)

	iter := q.Iterator()
	defer iter.Close()

	pk := []string{}

	for iter.Next() {
		var k string
		if err := iter.Scan(&k); err != nil {
			return nil, err
		}
		pk = append(pk, k)
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}

	return pk, nil
}
