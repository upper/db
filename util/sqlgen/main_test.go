package sqlgen

import (
	"strings"
	"testing"
)

func TestTruncateTable(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type:  SqlTruncate,
		Table: Table{"table name"},
	}

	s = strings.TrimSpace(stmt.Compile())
	e = `TRUNCATE TABLE "table name"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestDropTable(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type:  SqlDropTable,
		Table: Table{"table name"},
	}

	s = strings.TrimSpace(stmt.Compile())
	e = `DROP TABLE "table name"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestDropDatabase(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type:     SqlDropDatabase,
		Database: Database{"table name"},
	}

	s = trim(stmt.Compile())
	e = `DROP DATABASE "table name"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestSelectCount(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type:  SqlSelectCount,
		Table: Table{"table name"},
	}

	s = trim(stmt.Compile())
	e = `SELECT COUNT(1) AS _t FROM "table name"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestSelectCountWhere(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type:  SqlSelectCount,
		Table: Table{"table name"},
		Where: Where{
			ColumnValue{Column{"a"}, "=", Value{Raw{"7"}}},
		},
	}

	s = trim(stmt.Compile())
	e = `SELECT COUNT(1) AS _t FROM "table name" WHERE ("a" = 7)`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestSelectStarFrom(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type:  SqlSelect,
		Table: Table{"table name"},
	}

	s = trim(stmt.Compile())
	e = `SELECT * FROM "table name"`

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
			{"foo"},
			{"bar"},
			{"baz"},
		},
		Table: Table{"table name"},
	}

	s = trim(stmt.Compile())
	e = `SELECT "foo", "bar", "baz" FROM "table name"`

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
			{"foo"},
			{"bar"},
			{"baz"},
		},
		Limit: 42,
		Table: Table{"table name"},
	}

	s = trim(stmt.Compile())
	e = `SELECT "foo", "bar", "baz" FROM "table name" LIMIT 42`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

	// OFFSET only.
	stmt = Statement{
		Type: SqlSelect,
		Columns: Columns{
			{"foo"},
			{"bar"},
			{"baz"},
		},
		Offset: 17,
		Table:  Table{"table name"},
	}

	s = trim(stmt.Compile())
	e = `SELECT "foo", "bar", "baz" FROM "table name" OFFSET 17`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

	// LIMIT AND OFFSET.
	stmt = Statement{
		Type: SqlSelect,
		Columns: Columns{
			{"foo"},
			{"bar"},
			{"baz"},
		},
		Limit:  42,
		Offset: 17,
		Table:  Table{"table name"},
	}

	s = trim(stmt.Compile())
	e = `SELECT "foo", "bar", "baz" FROM "table name" LIMIT 42 OFFSET 17`

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
			{"foo"},
			{"bar"},
			{"baz"},
		},
		OrderBy: OrderBy{
			Columns: Columns{
				{"foo"},
			},
		},
		Table: Table{"table name"},
	}

	s = trim(stmt.Compile())
	e = `SELECT "foo", "bar", "baz" FROM "table name" ORDER BY "foo"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

	// ORDER BY field ASC
	stmt = Statement{
		Type: SqlSelect,
		Columns: Columns{
			{"foo"},
			{"bar"},
			{"baz"},
		},
		OrderBy: OrderBy{
			Columns: Columns{
				{"foo"},
			},
			Sort: Sort{SqlSortAsc},
		},
		Table: Table{"table name"},
	}

	s = trim(stmt.Compile())
	e = `SELECT "foo", "bar", "baz" FROM "table name" ORDER BY "foo" ASC`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

	// ORDER BY field DESC
	stmt = Statement{
		Type: SqlSelect,
		Columns: Columns{
			{"foo"},
			{"bar"},
			{"baz"},
		},
		OrderBy: OrderBy{
			Columns: Columns{
				{"foo"},
			},
			Sort: Sort{SqlSortDesc},
		},
		Table: Table{"table name"},
	}

	s = trim(stmt.Compile())
	e = `SELECT "foo", "bar", "baz" FROM "table name" ORDER BY "foo" DESC`

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
			{"foo"},
			{"bar"},
			{"baz"},
		},
		Table: Table{"table name"},
		Where: Where{
			ColumnValue{Column{"baz"}, "=", Value{99}},
		},
	}

	s = trim(stmt.Compile())
	e = `SELECT "foo", "bar", "baz" FROM "table name" WHERE ("baz" = "99")`

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
			{"foo"},
			{"bar"},
			{"baz"},
		},
		Table: Table{"table name"},
		Where: Where{
			ColumnValue{Column{"baz"}, "=", Value{99}},
		},
		Limit:  10,
		Offset: 23,
	}

	s = trim(stmt.Compile())
	e = `SELECT "foo", "bar", "baz" FROM "table name" WHERE ("baz" = "99") LIMIT 10 OFFSET 23`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestDelete(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type:  SqlDelete,
		Table: Table{"table name"},
		Where: Where{
			ColumnValue{Column{"baz"}, "=", Value{99}},
		},
	}

	s = trim(stmt.Compile())
	e = `DELETE FROM "table name" WHERE ("baz" = "99")`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestUpdate(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type:  SqlUpdate,
		Table: Table{"table name"},
		ColumnValues: ColumnValues{
			{Column{"foo"}, "=", Value{76}},
		},
		Where: Where{
			ColumnValue{Column{"baz"}, "=", Value{99}},
		},
	}

	s = trim(stmt.Compile())
	e = `UPDATE "table name" SET "foo" = "76" WHERE ("baz" = "99")`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

	stmt = Statement{
		Type:  SqlUpdate,
		Table: Table{"table name"},
		ColumnValues: ColumnValues{
			{Column{"foo"}, "=", Value{76}},
			{Column{"bar"}, "=", Value{Raw{"88"}}},
		},
		Where: Where{
			ColumnValue{Column{"baz"}, "=", Value{99}},
		},
	}

	s = trim(stmt.Compile())
	e = `UPDATE "table name" SET "foo" = "76", "bar" = 88 WHERE ("baz" = "99")`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestInsert(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type:  SqlInsert,
		Table: Table{"table name"},
		Columns: Columns{
			Column{"foo"},
			Column{"bar"},
			Column{"baz"},
		},
		Values: Values{
			Value{"1"},
			Value{2},
			Value{Raw{"3"}},
		},
	}

	s = trim(stmt.Compile())
	e = `INSERT INTO "table name" ("foo", "bar", "baz") VALUES ("1", "2", 3)`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}
