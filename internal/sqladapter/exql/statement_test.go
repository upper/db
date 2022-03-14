package exql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTruncateTable(t *testing.T) {
	stmt := Statement{
		Type:  Truncate,
		Table: TableWithName("table_name"),
	}

	s := mustTrim(stmt.Compile(defaultTemplate))
	assert.Equal(t, `TRUNCATE TABLE "table_name"`, s)
}

func TestDropTable(t *testing.T) {
	stmt := Statement{
		Type:  DropTable,
		Table: TableWithName("table_name"),
	}

	s := mustTrim(stmt.Compile(defaultTemplate))
	assert.Equal(t, `DROP TABLE "table_name"`, s)
}

func TestDropDatabase(t *testing.T) {
	stmt := Statement{
		Type:     DropDatabase,
		Database: &Database{Name: "table_name"},
	}

	s := mustTrim(stmt.Compile(defaultTemplate))
	assert.Equal(t, `DROP DATABASE "table_name"`, s)
}

func TestCount(t *testing.T) {
	stmt := Statement{
		Type:  Count,
		Table: TableWithName("table_name"),
	}

	s := mustTrim(stmt.Compile(defaultTemplate))
	assert.Equal(t, `SELECT COUNT(1) AS _t FROM "table_name"`, s)
}

func TestCountRelation(t *testing.T) {
	stmt := Statement{
		Type:  Count,
		Table: TableWithName("information_schema.tables"),
	}

	s := mustTrim(stmt.Compile(defaultTemplate))
	assert.Equal(t, `SELECT COUNT(1) AS _t FROM "information_schema"."tables"`, s)
}

func TestCountWhere(t *testing.T) {
	stmt := Statement{
		Type:  Count,
		Table: TableWithName("table_name"),
		Where: WhereConditions(
			&ColumnValue{Column: &Column{Name: "a"}, Operator: "=", Value: &Raw{Value: "7"}},
		),
	}

	s := mustTrim(stmt.Compile(defaultTemplate))
	assert.Equal(t, `SELECT COUNT(1) AS _t FROM "table_name" WHERE ("a" = 7)`, s)
}

func TestSelectStarFrom(t *testing.T) {
	stmt := Statement{
		Type:  Select,
		Table: TableWithName("table_name"),
	}

	s := mustTrim(stmt.Compile(defaultTemplate))
	assert.Equal(t, `SELECT * FROM "table_name"`, s)
}

func TestSelectStarFromAlias(t *testing.T) {
	stmt := Statement{
		Type:  Select,
		Table: TableWithName("table.name AS foo"),
	}

	s := mustTrim(stmt.Compile(defaultTemplate))
	assert.Equal(t, `SELECT * FROM "table"."name" AS "foo"`, s)
}

func TestSelectStarFromRawWhere(t *testing.T) {
	{
		stmt := Statement{
			Type:  Select,
			Table: TableWithName("table.name AS foo"),
			Where: WhereConditions(
				&Raw{Value: "foo.id = bar.foo_id"},
			),
		}

		s := mustTrim(stmt.Compile(defaultTemplate))
		assert.Equal(t, `SELECT * FROM "table"."name" AS "foo" WHERE (foo.id = bar.foo_id)`, s)
	}

	{
		stmt := Statement{
			Type:  Select,
			Table: TableWithName("table.name AS foo"),
			Where: WhereConditions(
				&Raw{Value: "foo.id = bar.foo_id"},
				&Raw{Value: "baz.id = exp.baz_id"},
			),
		}

		s := mustTrim(stmt.Compile(defaultTemplate))
		assert.Equal(t, `SELECT * FROM "table"."name" AS "foo" WHERE (foo.id = bar.foo_id AND baz.id = exp.baz_id)`, s)
	}
}

func TestSelectStarFromMany(t *testing.T) {
	stmt := Statement{
		Type:  Select,
		Table: TableWithName("first.table AS foo, second.table as BAR, third.table aS baz"),
	}

	s := mustTrim(stmt.Compile(defaultTemplate))
	assert.Equal(t, `SELECT * FROM "first"."table" AS "foo", "second"."table" AS "BAR", "third"."table" AS "baz"`, s)
}

func TestSelectTableStarFromMany(t *testing.T) {
	stmt := Statement{
		Type: Select,
		Columns: JoinColumns(
			&Column{Name: "foo.name"},
			&Column{Name: "BAR.*"},
			&Column{Name: "baz.last_name"},
		),
		Table: TableWithName("first.table AS foo, second.table as BAR, third.table aS baz"),
	}

	s := mustTrim(stmt.Compile(defaultTemplate))
	assert.Equal(t, `SELECT "foo"."name", "BAR".*, "baz"."last_name" FROM "first"."table" AS "foo", "second"."table" AS "BAR", "third"."table" AS "baz"`, s)
}

func TestSelectArtistNameFrom(t *testing.T) {
	stmt := Statement{
		Type:  Select,
		Table: TableWithName("artist"),
		Columns: JoinColumns(
			&Column{Name: "artist.name"},
		),
	}

	s := mustTrim(stmt.Compile(defaultTemplate))
	assert.Equal(t, `SELECT "artist"."name" FROM "artist"`, s)
}

func TestSelectJoin(t *testing.T) {
	stmt := Statement{
		Type:  Select,
		Table: TableWithName("artist a"),
		Columns: JoinColumns(
			&Column{Name: "a.name"},
		),
		Joins: JoinConditions(&Join{
			Table: TableWithName("books b"),
			On: OnConditions(
				&ColumnValue{
					Column:   ColumnWithName("b.author_id"),
					Operator: `=`,
					Value:    NewValue(ColumnWithName("a.id")),
				},
			),
		}),
	}

	s := mustTrim(stmt.Compile(defaultTemplate))
	assert.Equal(t, `SELECT "a"."name" FROM "artist" AS "a" JOIN "books" AS "b" ON ("b"."author_id" = "a"."id")`, s)
}

func TestSelectJoinUsing(t *testing.T) {
	stmt := Statement{
		Type:  Select,
		Table: TableWithName("artist a"),
		Columns: JoinColumns(
			&Column{Name: "a.name"},
		),
		Joins: JoinConditions(&Join{
			Table: TableWithName("books b"),
			Using: UsingColumns(
				ColumnWithName("artist_id"),
				ColumnWithName("country"),
			),
		}),
	}

	s := mustTrim(stmt.Compile(defaultTemplate))
	assert.Equal(t, `SELECT "a"."name" FROM "artist" AS "a" JOIN "books" AS "b" USING ("artist_id", "country")`, s)
}

func TestSelectUnfinishedJoin(t *testing.T) {
	stmt := Statement{
		Type:  Select,
		Table: TableWithName("artist a"),
		Columns: JoinColumns(
			&Column{Name: "a.name"},
		),
		Joins: JoinConditions(&Join{}),
	}

	s := mustTrim(stmt.Compile(defaultTemplate))
	assert.Equal(t, `SELECT "a"."name" FROM "artist" AS "a"`, s)
}

func TestSelectNaturalJoin(t *testing.T) {
	stmt := Statement{
		Type:  Select,
		Table: TableWithName("artist"),
		Joins: JoinConditions(&Join{
			Table: TableWithName("books"),
		}),
	}

	s := mustTrim(stmt.Compile(defaultTemplate))
	assert.Equal(t, `SELECT * FROM "artist" NATURAL JOIN "books"`, s)
}

func TestSelectRawFrom(t *testing.T) {
	stmt := Statement{
		Type:  Select,
		Table: TableWithName(`artist`),
		Columns: JoinColumns(
			&Column{Name: `artist.name`},
			&Column{Name: Raw{Value: `CONCAT(artist.name, " ", artist.last_name)`}},
		),
	}

	s := mustTrim(stmt.Compile(defaultTemplate))
	assert.Equal(t, `SELECT "artist"."name", CONCAT(artist.name, " ", artist.last_name) FROM "artist"`, s)
}

func TestSelectFieldsFrom(t *testing.T) {
	stmt := Statement{
		Type: Select,
		Columns: JoinColumns(
			&Column{Name: "foo"},
			&Column{Name: "bar"},
			&Column{Name: "baz"},
		),
		Table: TableWithName("table_name"),
	}

	s := mustTrim(stmt.Compile(defaultTemplate))
	assert.Equal(t, `SELECT "foo", "bar", "baz" FROM "table_name"`, s)
}

func TestSelectFieldsFromWithLimitOffset(t *testing.T) {
	{
		stmt := Statement{
			Type: Select,
			Columns: JoinColumns(
				&Column{Name: "foo"},
				&Column{Name: "bar"},
				&Column{Name: "baz"},
			),
			Limit: 42,
			Table: TableWithName("table_name"),
		}

		s := mustTrim(stmt.Compile(defaultTemplate))
		assert.Equal(t, `SELECT "foo", "bar", "baz" FROM "table_name" LIMIT 42`, s)
	}

	{
		stmt := Statement{
			Type: Select,
			Columns: JoinColumns(
				&Column{Name: "foo"},
				&Column{Name: "bar"},
				&Column{Name: "baz"},
			),
			Offset: 17,
			Table:  TableWithName("table_name"),
		}

		s := mustTrim(stmt.Compile(defaultTemplate))
		assert.Equal(t, `SELECT "foo", "bar", "baz" FROM "table_name" OFFSET 17`, s)
	}

	{
		stmt := Statement{
			Type: Select,
			Columns: JoinColumns(
				&Column{Name: "foo"},
				&Column{Name: "bar"},
				&Column{Name: "baz"},
			),
			Limit:  42,
			Offset: 17,
			Table:  TableWithName("table_name"),
		}

		s := mustTrim(stmt.Compile(defaultTemplate))
		assert.Equal(t, `SELECT "foo", "bar", "baz" FROM "table_name" LIMIT 42 OFFSET 17`, s)
	}
}

func TestStatementGroupBy(t *testing.T) {
	{
		stmt := Statement{
			Type: Select,
			Columns: JoinColumns(
				&Column{Name: "foo"},
				&Column{Name: "bar"},
				&Column{Name: "baz"},
			),
			GroupBy: GroupByColumns(
				&Column{Name: "foo"},
			),
			Table: TableWithName("table_name"),
		}

		s := mustTrim(stmt.Compile(defaultTemplate))
		assert.Equal(t, `SELECT "foo", "bar", "baz" FROM "table_name" GROUP BY "foo"`, s)
	}

	{
		stmt := Statement{
			Type: Select,
			Columns: JoinColumns(
				&Column{Name: "foo"},
				&Column{Name: "bar"},
				&Column{Name: "baz"},
			),
			GroupBy: GroupByColumns(
				&Column{Name: "foo"},
				&Column{Name: "bar"},
			),
			Table: TableWithName("table_name"),
		}

		s := mustTrim(stmt.Compile(defaultTemplate))
		assert.Equal(t, `SELECT "foo", "bar", "baz" FROM "table_name" GROUP BY "foo", "bar"`, s)
	}
}

func TestSelectFieldsFromWithOrderBy(t *testing.T) {
	{
		stmt := Statement{
			Type: Select,
			Columns: JoinColumns(
				&Column{Name: "foo"},
				&Column{Name: "bar"},
				&Column{Name: "baz"},
			),
			OrderBy: JoinWithOrderBy(
				JoinSortColumns(
					&SortColumn{Column: &Column{Name: "foo"}},
				),
			),
			Table: TableWithName("table_name"),
		}

		s := mustTrim(stmt.Compile(defaultTemplate))
		assert.Equal(t, `SELECT "foo", "bar", "baz" FROM "table_name" ORDER BY "foo"`, s)
	}

	{
		stmt := Statement{
			Type: Select,
			Columns: JoinColumns(
				&Column{Name: "foo"},
				&Column{Name: "bar"},
				&Column{Name: "baz"},
			),
			OrderBy: JoinWithOrderBy(
				JoinSortColumns(
					&SortColumn{Column: &Column{Name: "foo"}, Order: Order_Ascendent},
				),
			),
			Table: TableWithName("table_name"),
		}

		s := mustTrim(stmt.Compile(defaultTemplate))
		assert.Equal(t, `SELECT "foo", "bar", "baz" FROM "table_name" ORDER BY "foo" ASC`, s)
	}

	{
		stmt := Statement{
			Type: Select,
			Columns: JoinColumns(
				&Column{Name: "foo"},
				&Column{Name: "bar"},
				&Column{Name: "baz"},
			),
			OrderBy: JoinWithOrderBy(
				JoinSortColumns(
					&SortColumn{Column: &Column{Name: "foo"}, Order: Order_Descendent},
				),
			),
			Table: TableWithName("table_name"),
		}

		s := mustTrim(stmt.Compile(defaultTemplate))
		assert.Equal(t, `SELECT "foo", "bar", "baz" FROM "table_name" ORDER BY "foo" DESC`, s)
	}

	{
		stmt := Statement{
			Type: Select,
			Columns: JoinColumns(
				&Column{Name: "foo"},
				&Column{Name: "bar"},
				&Column{Name: "baz"},
			),
			OrderBy: JoinWithOrderBy(
				JoinSortColumns(
					&SortColumn{Column: &Column{Name: "foo"}, Order: Order_Descendent},
					&SortColumn{Column: &Column{Name: "bar"}, Order: Order_Ascendent},
					&SortColumn{Column: &Column{Name: "baz"}, Order: Order_Descendent},
				),
			),
			Table: TableWithName("table_name"),
		}

		s := mustTrim(stmt.Compile(defaultTemplate))
		assert.Equal(t, `SELECT "foo", "bar", "baz" FROM "table_name" ORDER BY "foo" DESC, "bar" ASC, "baz" DESC`, s)
	}

	{
		stmt := Statement{
			Type: Select,
			Columns: JoinColumns(
				&Column{Name: "foo"},
				&Column{Name: "bar"},
				&Column{Name: "baz"},
			),
			OrderBy: JoinWithOrderBy(
				JoinSortColumns(
					&SortColumn{Column: &Column{Name: Raw{Value: "FOO()"}}, Order: Order_Descendent},
					&SortColumn{Column: &Column{Name: Raw{Value: "BAR()"}}, Order: Order_Ascendent},
				),
			),
			Table: TableWithName("table_name"),
		}

		s := mustTrim(stmt.Compile(defaultTemplate))
		assert.Equal(t, `SELECT "foo", "bar", "baz" FROM "table_name" ORDER BY FOO() DESC, BAR() ASC`, s)
	}
}

func TestSelectFieldsFromWhere(t *testing.T) {
	{
		stmt := Statement{
			Type: Select,
			Columns: JoinColumns(
				&Column{Name: "foo"},
				&Column{Name: "bar"},
				&Column{Name: "baz"},
			),
			Table: TableWithName("table_name"),
			Where: WhereConditions(
				&ColumnValue{Column: &Column{Name: "baz"}, Operator: "=", Value: NewValue(99)},
			),
		}

		s := mustTrim(stmt.Compile(defaultTemplate))
		assert.Equal(t, `SELECT "foo", "bar", "baz" FROM "table_name" WHERE ("baz" = '99')`, s)
	}
}

func TestSelectFieldsFromWhereLimitOffset(t *testing.T) {
	{
		stmt := Statement{
			Type: Select,
			Columns: JoinColumns(
				&Column{Name: "foo"},
				&Column{Name: "bar"},
				&Column{Name: "baz"},
			),
			Table: TableWithName("table_name"),
			Where: WhereConditions(
				&ColumnValue{Column: &Column{Name: "baz"}, Operator: "=", Value: NewValue(99)},
			),
			Limit:  10,
			Offset: 23,
		}

		s := mustTrim(stmt.Compile(defaultTemplate))
		assert.Equal(t, `SELECT "foo", "bar", "baz" FROM "table_name" WHERE ("baz" = '99') LIMIT 10 OFFSET 23`, s)
	}
}

func TestDelete(t *testing.T) {
	stmt := Statement{
		Type:  Delete,
		Table: TableWithName("table_name"),
		Where: WhereConditions(
			&ColumnValue{Column: &Column{Name: "baz"}, Operator: "=", Value: NewValue(99)},
		),
	}

	s := mustTrim(stmt.Compile(defaultTemplate))
	assert.Equal(t, `DELETE FROM "table_name" WHERE ("baz" = '99')`, s)
}

func TestUpdate(t *testing.T) {
	{
		stmt := Statement{
			Type:  Update,
			Table: TableWithName("table_name"),
			ColumnValues: JoinColumnValues(
				&ColumnValue{Column: &Column{Name: "foo"}, Operator: "=", Value: NewValue(76)},
			),
			Where: WhereConditions(
				&ColumnValue{Column: &Column{Name: "baz"}, Operator: "=", Value: NewValue(99)},
			),
		}

		s := mustTrim(stmt.Compile(defaultTemplate))
		assert.Equal(t, `UPDATE "table_name" SET "foo" = '76' WHERE ("baz" = '99')`, s)
	}

	{
		stmt := Statement{
			Type:  Update,
			Table: TableWithName("table_name"),
			ColumnValues: JoinColumnValues(
				&ColumnValue{Column: &Column{Name: "foo"}, Operator: "=", Value: NewValue(76)},
				&ColumnValue{Column: &Column{Name: "bar"}, Operator: "=", Value: NewValue(Raw{Value: "88"})},
			),
			Where: WhereConditions(
				&ColumnValue{Column: &Column{Name: "baz"}, Operator: "=", Value: NewValue(99)},
			),
		}

		s := mustTrim(stmt.Compile(defaultTemplate))
		assert.Equal(t, `UPDATE "table_name" SET "foo" = '76', "bar" = 88 WHERE ("baz" = '99')`, s)
	}
}

func TestInsert(t *testing.T) {
	stmt := Statement{
		Type:  Insert,
		Table: TableWithName("table_name"),
		Columns: JoinColumns(
			&Column{Name: "foo"},
			&Column{Name: "bar"},
			&Column{Name: "baz"},
		),
		Values: NewValueGroup(
			&Value{V: "1"},
			&Value{V: 2},
			&Value{V: Raw{Value: "3"}},
		),
	}

	s := mustTrim(stmt.Compile(defaultTemplate))
	assert.Equal(t, `INSERT INTO "table_name" ("foo", "bar", "baz") VALUES ('1', '2', 3)`, s)
}

func TestInsertMultiple(t *testing.T) {
	stmt := Statement{
		Type:  Insert,
		Table: TableWithName("table_name"),
		Columns: JoinColumns(
			&Column{Name: "foo"},
			&Column{Name: "bar"},
			&Column{Name: "baz"},
		),
		Values: JoinValueGroups(
			NewValueGroup(
				NewValue("1"),
				NewValue("2"),
				NewValue(&Raw{Value: "3"}),
			),
			NewValueGroup(
				NewValue(&Raw{Value: "4"}),
				NewValue(&Raw{Value: "5"}),
				NewValue(&Raw{Value: "6"}),
			),
		),
	}

	s := mustTrim(stmt.Compile(defaultTemplate))
	assert.Equal(t, `INSERT INTO "table_name" ("foo", "bar", "baz") VALUES ('1', '2', 3), (4, 5, 6)`, s)
}

func TestInsertReturning(t *testing.T) {
	stmt := Statement{
		Type:  Insert,
		Table: TableWithName("table_name"),
		Returning: ReturningColumns(
			ColumnWithName("id"),
		),
		Columns: JoinColumns(
			&Column{Name: "foo"},
			&Column{Name: "bar"},
			&Column{Name: "baz"},
		),
		Values: NewValueGroup(
			&Value{V: "1"},
			&Value{V: 2},
			&Value{V: Raw{Value: "3"}},
		),
	}

	s := mustTrim(stmt.Compile(defaultTemplate))
	assert.Equal(t, `INSERT INTO "table_name" ("foo", "bar", "baz") VALUES ('1', '2', 3) RETURNING "id"`, s)
}

func TestRawSQLStatement(t *testing.T) {
	stmt := RawSQL(`SELECT * FROM "foo" ORDER BY "bar"`)

	s := mustTrim(stmt.Compile(defaultTemplate))
	assert.Equal(t, `SELECT * FROM "foo" ORDER BY "bar"`, s)
}

func BenchmarkStatementSimpleQuery(b *testing.B) {
	stmt := Statement{
		Type:  Count,
		Table: TableWithName("table_name"),
		Where: WhereConditions(
			&ColumnValue{Column: &Column{Name: "a"}, Operator: "=", Value: NewValue(Raw{Value: "7"})},
		),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = stmt.Compile(defaultTemplate)
	}
}

func BenchmarkStatementSimpleQueryHash(b *testing.B) {
	stmt := Statement{
		Type:  Count,
		Table: TableWithName("table_name"),
		Where: WhereConditions(
			&ColumnValue{Column: &Column{Name: "a"}, Operator: "=", Value: NewValue(Raw{Value: "7"})},
		),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = stmt.Hash()
	}
}

func BenchmarkStatementSimpleQueryNoCache(b *testing.B) {
	for i := 0; i < b.N; i++ {
		stmt := Statement{
			Type:  Count,
			Table: TableWithName("table_name"),
			Where: WhereConditions(
				&ColumnValue{Column: &Column{Name: "a"}, Operator: "=", Value: NewValue(Raw{Value: "7"})},
			),
		}
		_, _ = stmt.Compile(defaultTemplate)
	}
}

func BenchmarkStatementComplexQuery(b *testing.B) {
	stmt := Statement{
		Type:  Insert,
		Table: TableWithName("table_name"),
		Columns: JoinColumns(
			&Column{Name: "foo"},
			&Column{Name: "bar"},
			&Column{Name: "baz"},
		),
		Values: NewValueGroup(
			&Value{V: "1"},
			&Value{V: 2},
			&Value{V: Raw{Value: "3"}},
		),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = stmt.Compile(defaultTemplate)
	}
}

func BenchmarkStatementComplexQueryNoCache(b *testing.B) {
	for i := 0; i < b.N; i++ {
		stmt := Statement{
			Type:  Insert,
			Table: TableWithName("table_name"),
			Columns: JoinColumns(
				&Column{Name: "foo"},
				&Column{Name: "bar"},
				&Column{Name: "baz"},
			),
			Values: NewValueGroup(
				&Value{V: "1"},
				&Value{V: 2},
				&Value{V: Raw{Value: "3"}},
			),
		}
		_, _ = stmt.Compile(defaultTemplate)
	}
}
