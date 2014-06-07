package sqlgen

import (
	"regexp"
	"strings"
	"testing"
)

var reInvisible = regexp.MustCompile(`[\t\n\r]`)
var reSpace = regexp.MustCompile(`\s+`)

func trim(a string) string {
	a = reInvisible.ReplaceAllString(strings.TrimSpace(a), " ")
	a = reSpace.ReplaceAllString(strings.TrimSpace(a), " ")
	return a
}

func TestTruncateTable(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type:   SqlTruncate,
		Source: Source{"source name"},
	}

	s = strings.TrimSpace(stmt.Compile())
	e = `TRUNCATE TABLE "source name"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestDropTable(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type:   SqlDropTable,
		Source: Source{"source name"},
	}

	s = strings.TrimSpace(stmt.Compile())
	e = `DROP TABLE "source name"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestDropDatabase(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type:     SqlDropDatabase,
		Database: Database{"source name"},
	}

	s = trim(stmt.Compile())
	e = `DROP DATABASE "source name"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestSelectCount(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type:   SqlSelectCount,
		Source: Source{"source name"},
	}

	s = trim(stmt.Compile())
	e = `SELECT COUNT(1) AS _t FROM "source name"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestSelectCountWhere(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type:   SqlSelectCount,
		Source: Source{"source name"},
		Where: Where{
			ColumnValue{Column{"a"}, "=", Value{Raw{"7"}}},
		},
	}

	s = trim(stmt.Compile())
	e = `SELECT COUNT(1) AS _t FROM "source name" WHERE ("a" = 7)`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestSelectFieldsFrom(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type: SqlSelect,
		Columns: Columns{
			[]Column{
				{"foo"},
				{"bar"},
				{"baz"},
			},
		},
		Source: Source{"source name"},
	}

	s = trim(stmt.Compile())
	e = `SELECT "foo", "bar", "baz" FROM "source name"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestSelectFieldsFromWithLimitOffset(t *testing.T) {
	var s, e string
	var stmt Statement

	// LIMIT only.
	stmt = Statement{
		Type: SqlSelect,
		Columns: Columns{
			[]Column{
				{"foo"},
				{"bar"},
				{"baz"},
			},
		},
		Limit:  42,
		Source: Source{"source name"},
	}

	s = trim(stmt.Compile())
	e = `SELECT "foo", "bar", "baz" FROM "source name" LIMIT 42`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

	// OFFSET only.
	stmt = Statement{
		Type: SqlSelect,
		Columns: Columns{
			[]Column{
				{"foo"},
				{"bar"},
				{"baz"},
			},
		},
		Offset: 17,
		Source: Source{"source name"},
	}

	s = trim(stmt.Compile())
	e = `SELECT "foo", "bar", "baz" FROM "source name" OFFSET 17`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

	// LIMIT AND OFFSET.
	stmt = Statement{
		Type: SqlSelect,
		Columns: Columns{
			[]Column{
				{"foo"},
				{"bar"},
				{"baz"},
			},
		},
		Limit:  42,
		Offset: 17,
		Source: Source{"source name"},
	}

	s = trim(stmt.Compile())
	e = `SELECT "foo", "bar", "baz" FROM "source name" LIMIT 42 OFFSET 17`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestSelectFieldsFromWithOrderBy(t *testing.T) {
	var s, e string
	var stmt Statement

	// Simple ORDER BY
	stmt = Statement{
		Type: SqlSelect,
		Columns: Columns{
			[]Column{
				{"foo"},
				{"bar"},
				{"baz"},
			},
		},
		OrderBy: OrderBy{
			Columns: Columns{
				[]Column{
					{"foo"},
				},
			},
		},
		Source: Source{"source name"},
	}

	s = trim(stmt.Compile())
	e = `SELECT "foo", "bar", "baz" FROM "source name" ORDER BY "foo"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

	// ORDER BY field ASC
	stmt = Statement{
		Type: SqlSelect,
		Columns: Columns{
			[]Column{
				{"foo"},
				{"bar"},
				{"baz"},
			},
		},
		OrderBy: OrderBy{
			Columns: Columns{
				[]Column{
					{"foo"},
				},
			},
			Sort: Sort{SqlSortAsc},
		},
		Source: Source{"source name"},
	}

	s = trim(stmt.Compile())
	e = `SELECT "foo", "bar", "baz" FROM "source name" ORDER BY "foo" ASC`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

	// ORDER BY field DESC
	stmt = Statement{
		Type: SqlSelect,
		Columns: Columns{
			[]Column{
				{"foo"},
				{"bar"},
				{"baz"},
			},
		},
		OrderBy: OrderBy{
			Columns: Columns{
				[]Column{
					{"foo"},
				},
			},
			Sort: Sort{SqlSortDesc},
		},
		Source: Source{"source name"},
	}

	s = trim(stmt.Compile())
	e = `SELECT "foo", "bar", "baz" FROM "source name" ORDER BY "foo" DESC`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestSelectFieldsFromWhere(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type: SqlSelect,
		Columns: Columns{
			[]Column{
				{"foo"},
				{"bar"},
				{"baz"},
			},
		},
		Source: Source{"source name"},
		Where: Where{
			ColumnValue{Column{"baz"}, "=", Value{99}},
		},
	}

	s = trim(stmt.Compile())
	e = `SELECT "foo", "bar", "baz" FROM "source name" WHERE ("baz" = "99")`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestSelectFieldsFromWhereLimitOffset(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type: SqlSelect,
		Columns: Columns{
			[]Column{
				{"foo"},
				{"bar"},
				{"baz"},
			},
		},
		Source: Source{"source name"},
		Where: Where{
			ColumnValue{Column{"baz"}, "=", Value{99}},
		},
		Limit:  10,
		Offset: 23,
	}

	s = trim(stmt.Compile())
	e = `SELECT "foo", "bar", "baz" FROM "source name" WHERE ("baz" = "99") LIMIT 10 OFFSET 23`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestDelete(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type:   SqlDelete,
		Source: Source{"source name"},
		Where: Where{
			ColumnValue{Column{"baz"}, "=", Value{99}},
		},
	}

	s = trim(stmt.Compile())
	e = `DELETE FROM "source name" WHERE ("baz" = "99")`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestUpdate(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type:   SqlUpdate,
		Source: Source{"source name"},
		ColumnValues: ColumnValues{
			[]ColumnValue{
				{Column{"foo"}, "=", Value{76}},
			},
		},
		Where: Where{
			ColumnValue{Column{"baz"}, "=", Value{99}},
		},
	}

	s = trim(stmt.Compile())
	e = `UPDATE "source name" SET "foo" = "76" WHERE ("baz" = "99")`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

	stmt = Statement{
		Type:   SqlUpdate,
		Source: Source{"source name"},
		ColumnValues: ColumnValues{
			[]ColumnValue{
				{Column{"foo"}, "=", Value{76}},
				{Column{"bar"}, "=", Value{Raw{"88"}}},
			},
		},
		Where: Where{
			ColumnValue{Column{"baz"}, "=", Value{99}},
		},
	}

	s = trim(stmt.Compile())
	e = `UPDATE "source name" SET "foo" = "76", "bar" = 88 WHERE ("baz" = "99")`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestInsert(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type:   SqlInsert,
		Source: Source{"source name"},
		Columns: Columns{
			[]Column{
				Column{"foo"},
				Column{"bar"},
				Column{"baz"},
			},
		},
		Values: Values{
			Value{"1"},
			Value{2},
			Value{Raw{"3"}},
		},
	}

	s = trim(stmt.Compile())
	e = `INSERT INTO "source name" ("foo", "bar", "baz") VALUES ("1", "2", 3)`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}
