package sqlgen

import (
	"testing"
)

func TestTruncateTable(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type:  SqlTruncate,
		Table: Table{"table_name"},
	}

	s = trim(stmt.Compile(defaultTemplate))
	e = `TRUNCATE TABLE "table_name"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestDropTable(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type:  SqlDropTable,
		Table: Table{"table_name"},
	}

	s = trim(stmt.Compile(defaultTemplate))
	e = `DROP TABLE "table_name"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestDropDatabase(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type:     SqlDropDatabase,
		Database: Database{Name: "table_name"},
	}

	s = trim(stmt.Compile(defaultTemplate))
	e = `DROP DATABASE "table_name"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestSelectCount(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type:  SqlSelectCount,
		Table: Table{"table_name"},
	}

	s = trim(stmt.Compile(defaultTemplate))
	e = `SELECT COUNT(1) AS _t FROM "table_name"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestSelectCountRelation(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type:  SqlSelectCount,
		Table: Table{"information_schema.tables"},
	}

	s = trim(stmt.Compile(defaultTemplate))
	e = `SELECT COUNT(1) AS _t FROM "information_schema"."tables"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestSelectCountWhere(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type:  SqlSelectCount,
		Table: Table{"table_name"},
		Where: Where{
			&ColumnValue{Column: Column{Name: "a"}, Operator: "=", Value: Value{Raw{Value: "7"}}},
		},
	}

	s = trim(stmt.Compile(defaultTemplate))
	e = `SELECT COUNT(1) AS _t FROM "table_name" WHERE ("a" = 7)`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestSelectStarFrom(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type:  SqlSelect,
		Table: Table{"table_name"},
	}

	s = trim(stmt.Compile(defaultTemplate))
	e = `SELECT * FROM "table_name"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestSelectStarFromAlias(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type:  SqlSelect,
		Table: Table{"table.name AS foo"},
	}

	s = trim(stmt.Compile(defaultTemplate))
	e = `SELECT * FROM "table"."name" AS "foo"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestSelectStarFromRawWhere(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type:  SqlSelect,
		Table: Table{"table.name AS foo"},
		Where: Where{
			&Raw{Value: "foo.id = bar.foo_id"},
		},
	}

	s = trim(stmt.Compile(defaultTemplate))
	e = `SELECT * FROM "table"."name" AS "foo" WHERE (foo.id = bar.foo_id)`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

	stmt = Statement{
		Type:  SqlSelect,
		Table: Table{"table.name AS foo"},
		Where: Where{
			&Raw{Value: "foo.id = bar.foo_id"},
			&Raw{Value: "baz.id = exp.baz_id"},
		},
	}

	s = trim(stmt.Compile(defaultTemplate))
	e = `SELECT * FROM "table"."name" AS "foo" WHERE (foo.id = bar.foo_id AND baz.id = exp.baz_id)`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestSelectStarFromMany(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type:  SqlSelect,
		Table: Table{"first.table AS foo, second.table as BAR, third.table aS baz"},
	}

	s = trim(stmt.Compile(defaultTemplate))
	e = `SELECT * FROM "first"."table" AS "foo", "second"."table" AS "BAR", "third"."table" AS "baz"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestSelectArtistNameFrom(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type:  SqlSelect,
		Table: Table{"artist"},
		Columns: NewColumns(
			Column{Name: "artist.name"},
		),
	}

	s = trim(stmt.Compile(defaultTemplate))
	e = `SELECT "artist"."name" FROM "artist"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestSelectRawFrom(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type:  SqlSelect,
		Table: Table{`artist`},
		Columns: NewColumns(
			Column{Name: `artist.name`},
			Column{Name: Raw{Value: `CONCAT(artist.name, " ", artist.last_name)`}},
		),
	}

	s = trim(stmt.Compile(defaultTemplate))
	e = `SELECT "artist"."name", CONCAT(artist.name, " ", artist.last_name) FROM "artist"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestSelectFieldsFrom(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type: SqlSelect,
		Columns: NewColumns(
			Column{Name: "foo"},
			Column{Name: "bar"},
			Column{Name: "baz"},
		),
		Table: Table{"table_name"},
	}

	s = trim(stmt.Compile(defaultTemplate))
	e = `SELECT "foo", "bar", "baz" FROM "table_name"`

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
		Columns: NewColumns(
			Column{Name: "foo"},
			Column{Name: "bar"},
			Column{Name: "baz"},
		),
		Limit: 42,
		Table: Table{"table_name"},
	}

	s = trim(stmt.Compile(defaultTemplate))
	e = `SELECT "foo", "bar", "baz" FROM "table_name" LIMIT 42`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

	// OFFSET only.
	stmt = Statement{
		Type: SqlSelect,
		Columns: NewColumns(
			Column{Name: "foo"},
			Column{Name: "bar"},
			Column{Name: "baz"},
		),
		Offset: 17,
		Table:  Table{"table_name"},
	}

	s = trim(stmt.Compile(defaultTemplate))
	e = `SELECT "foo", "bar", "baz" FROM "table_name" OFFSET 17`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

	// LIMIT AND OFFSET.
	stmt = Statement{
		Type: SqlSelect,
		Columns: NewColumns(
			Column{Name: "foo"},
			Column{Name: "bar"},
			Column{Name: "baz"},
		),
		Limit:  42,
		Offset: 17,
		Table:  Table{"table_name"},
	}

	s = trim(stmt.Compile(defaultTemplate))
	e = `SELECT "foo", "bar", "baz" FROM "table_name" LIMIT 42 OFFSET 17`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestGroupBy(t *testing.T) {
	var s, e string
	var stmt Statement

	// Simple GROUP BY
	stmt = Statement{
		Type: SqlSelect,
		Columns: NewColumns(
			Column{Name: "foo"},
			Column{Name: "bar"},
			Column{Name: "baz"},
		),
		GroupBy: GroupBy{
			Columns: []Column{
				Column{Name: "foo"},
			},
		},
		Table: Table{"table_name"},
	}

	s = trim(stmt.Compile(defaultTemplate))
	e = `SELECT "foo", "bar", "baz" FROM "table_name" GROUP BY "foo"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

	stmt = Statement{
		Type: SqlSelect,
		Columns: NewColumns(
			Column{Name: "foo"},
			Column{Name: "bar"},
			Column{Name: "baz"},
		),
		GroupBy: GroupBy{
			Columns: []Column{
				Column{Name: "foo"},
				Column{Name: "bar"},
			},
		},
		Table: Table{"table_name"},
	}

	s = trim(stmt.Compile(defaultTemplate))
	e = `SELECT "foo", "bar", "baz" FROM "table_name" GROUP BY "foo", "bar"`

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
		Columns: NewColumns(
			Column{Name: "foo"},
			Column{Name: "bar"},
			Column{Name: "baz"},
		),
		OrderBy: OrderBy{
			SortColumns: SortColumns{
				SortColumn{Column: Column{Name: "foo"}},
			},
		},
		Table: Table{"table_name"},
	}

	s = trim(stmt.Compile(defaultTemplate))
	e = `SELECT "foo", "bar", "baz" FROM "table_name" ORDER BY "foo"`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

	// ORDER BY field ASC
	stmt = Statement{
		Type: SqlSelect,
		Columns: NewColumns(
			Column{Name: "foo"},
			Column{Name: "bar"},
			Column{Name: "baz"},
		),
		OrderBy: OrderBy{
			SortColumns{
				SortColumn{Column{Name: "foo"}, SqlSortAsc},
			},
		},
		Table: Table{"table_name"},
	}

	s = trim(stmt.Compile(defaultTemplate))
	e = `SELECT "foo", "bar", "baz" FROM "table_name" ORDER BY "foo" ASC`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

	// ORDER BY field DESC
	stmt = Statement{
		Type: SqlSelect,
		Columns: NewColumns(
			Column{Name: "foo"},
			Column{Name: "bar"},
			Column{Name: "baz"},
		),
		OrderBy: OrderBy{
			SortColumns{
				{Column{Name: "foo"}, SqlSortDesc},
			},
		},
		Table: Table{"table_name"},
	}

	s = trim(stmt.Compile(defaultTemplate))
	e = `SELECT "foo", "bar", "baz" FROM "table_name" ORDER BY "foo" DESC`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

	// ORDER BY many fields
	stmt = Statement{
		Type: SqlSelect,
		Columns: NewColumns(
			Column{Name: "foo"},
			Column{Name: "bar"},
			Column{Name: "baz"},
		),
		OrderBy: OrderBy{
			SortColumns{
				{Column{Name: "foo"}, SqlSortDesc},
				{Column{Name: "bar"}, SqlSortAsc},
				{Column{Name: "baz"}, SqlSortDesc},
			},
		},
		Table: Table{"table_name"},
	}

	s = trim(stmt.Compile(defaultTemplate))
	e = `SELECT "foo", "bar", "baz" FROM "table_name" ORDER BY "foo" DESC, "bar" ASC, "baz" DESC`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

	// ORDER BY function
	stmt = Statement{
		Type: SqlSelect,
		Columns: NewColumns(
			Column{Name: "foo"},
			Column{Name: "bar"},
			Column{Name: "baz"},
		),
		OrderBy: OrderBy{
			SortColumns{
				{Column{Name: Raw{Value: "FOO()"}}, SqlSortDesc},
				{Column{Name: Raw{Value: "BAR()"}}, SqlSortAsc},
			},
		},
		Table: Table{"table_name"},
	}

	s = trim(stmt.Compile(defaultTemplate))
	e = `SELECT "foo", "bar", "baz" FROM "table_name" ORDER BY FOO() DESC, BAR() ASC`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestSelectFieldsFromWhere(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type: SqlSelect,
		Columns: NewColumns(
			Column{Name: "foo"},
			Column{Name: "bar"},
			Column{Name: "baz"},
		),
		Table: Table{"table_name"},
		Where: Where{
			&ColumnValue{Column: Column{Name: "baz"}, Operator: "=", Value: Value{99}},
		},
	}

	s = trim(stmt.Compile(defaultTemplate))
	e = `SELECT "foo", "bar", "baz" FROM "table_name" WHERE ("baz" = '99')`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestSelectFieldsFromWhereLimitOffset(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type: SqlSelect,
		Columns: NewColumns(
			Column{Name: "foo"},
			Column{Name: "bar"},
			Column{Name: "baz"},
		),
		Table: Table{"table_name"},
		Where: Where{
			&ColumnValue{Column: Column{Name: "baz"}, Operator: "=", Value: Value{99}},
		},
		Limit:  10,
		Offset: 23,
	}

	s = trim(stmt.Compile(defaultTemplate))
	e = `SELECT "foo", "bar", "baz" FROM "table_name" WHERE ("baz" = '99') LIMIT 10 OFFSET 23`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestDelete(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type:  SqlDelete,
		Table: Table{"table_name"},
		Where: Where{
			&ColumnValue{Column: Column{Name: "baz"}, Operator: "=", Value: Value{99}},
		},
	}

	s = trim(stmt.Compile(defaultTemplate))
	e = `DELETE FROM "table_name" WHERE ("baz" = '99')`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestUpdate(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type:  SqlUpdate,
		Table: Table{"table_name"},
		ColumnValues: *NewColumnValues(
			ColumnValue{Column: Column{Name: "foo"}, Operator: "=", Value: Value{76}},
		),
		Where: Where{
			&ColumnValue{Column: Column{Name: "baz"}, Operator: "=", Value: Value{99}},
		},
	}

	s = trim(stmt.Compile(defaultTemplate))
	e = `UPDATE "table_name" SET "foo" = '76' WHERE ("baz" = '99')`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}

	stmt = Statement{
		Type:  SqlUpdate,
		Table: Table{"table_name"},
		ColumnValues: *NewColumnValues(
			ColumnValue{Column: Column{Name: "foo"}, Operator: "=", Value: Value{76}},
			ColumnValue{Column: Column{Name: "bar"}, Operator: "=", Value: Value{Raw{Value: "88"}}},
		),
		Where: Where{
			&ColumnValue{Column: Column{Name: "baz"}, Operator: "=", Value: Value{99}},
		},
	}

	s = trim(stmt.Compile(defaultTemplate))
	e = `UPDATE "table_name" SET "foo" = '76', "bar" = 88 WHERE ("baz" = '99')`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestInsert(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type:  SqlInsert,
		Table: Table{"table_name"},
		Columns: NewColumns(
			Column{Name: "foo"},
			Column{Name: "bar"},
			Column{Name: "baz"},
		),
		Values: Values{
			Value{"1"},
			Value{2},
			Value{Raw{Value: "3"}},
		},
	}

	s = trim(stmt.Compile(defaultTemplate))
	e = `INSERT INTO "table_name" ("foo", "bar", "baz") VALUES ('1', '2', 3)`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}

func TestInsertExtra(t *testing.T) {
	var s, e string
	var stmt Statement

	stmt = Statement{
		Type:  SqlInsert,
		Table: Table{"table_name"},
		Extra: "RETURNING id",
		Columns: NewColumns(
			Column{Name: "foo"},
			Column{Name: "bar"},
			Column{Name: "baz"},
		),
		Values: Values{
			Value{"1"},
			Value{2},
			Value{Raw{Value: "3"}},
		},
	}

	s = trim(stmt.Compile(defaultTemplate))
	e = `INSERT INTO "table_name" ("foo", "bar", "baz") VALUES ('1', '2', 3) RETURNING id`

	if s != e {
		t.Fatalf("Got: %s, Expecting: %s", s, e)
	}
}
