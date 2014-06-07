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

func TestColumnString(t *testing.T) {
	var s, e string

	column := Column{"role.name"}

	s = column.String()
	e = `"role"."name"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestColumns(t *testing.T) {
	var s, e string

	columns := Columns{
		[]Column{
			{"id"},
			{"customer"},
			{"service_id"},
			{"role.name"},
			{"role.id"},
		},
	}

	s = columns.String()
	e = `"id", "customer", "service_id", "role"."name", "role"."id"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

}

func TestValue(t *testing.T) {
	var s, e string
	var val Value

	val = Value{1}

	s = val.String()
	e = `"1"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

	val = Value{Raw{"NOW()"}}

	s = val.String()
	e = `NOW()`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

}

func TestColumnValue(t *testing.T) {
	var s, e string
	var cv ColumnValue

	cv = ColumnValue{Column{"id"}, "=", Value{1}}

	s = cv.String()
	e = `"id" = "1"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

	cv = ColumnValue{Column{"date"}, "=", Value{Raw{"NOW()"}}}

	s = cv.String()
	e = `"date" = NOW()`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestColumnValues(t *testing.T) {
	var s, e string
	var cvs ColumnValues

	cvs = ColumnValues{
		[]ColumnValue{
			{Column{"id"}, ">", Value{8}},
			{Column{"other.id"}, "<", Value{Raw{"100"}}},
			{Column{"name"}, "=", Value{"Haruki Murakami"}},
			{Column{"created"}, ">=", Value{Raw{"NOW()"}}},
			{Column{"modified"}, "<=", Value{Raw{"NOW()"}}},
		},
	}

	s = cvs.String()
	e = `"id" > "8", "other"."id" < 100, "name" = "Haruki Murakami", "created" >= NOW(), "modified" <= NOW()`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

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
