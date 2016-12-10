package postgresql

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"upper.io/db.v2"
)

func TestStringAndInt64Array(t *testing.T) {
	sess := mustOpen()
	driver := sess.Driver().(*sql.DB)

	defer func() {
		driver.Exec(`DROP TABLE IF EXISTS array_types`)
		sess.Close()
	}()

	if _, err := driver.Exec(`
		CREATE TABLE array_types (
			id serial primary key,
			integers bigint[] DEFAULT NULL,
			strings varchar(64)[]
		)`); err != nil {
		assert.NoError(t, err)
	}

	arrayTypes := sess.Collection("array_types")
	err := arrayTypes.Truncate()
	assert.NoError(t, err)

	type arrayType struct {
		ID       int64    `db:"id,pk"`
		Integers []int64  `db:"integers,int64array"`
		Strings  []string `db:"strings,stringarray"`
	}

	tt := []arrayType{
		// Test nil arrays.
		arrayType{
			ID:       1,
			Integers: nil,
			Strings:  nil,
		},

		// Test empty arrays.
		arrayType{
			ID:       2,
			Integers: []int64{},
			Strings:  []string{},
		},

		// Test non-empty arrays.
		arrayType{
			ID:       3,
			Integers: []int64{1, 2, 3},
			Strings:  []string{"1", "2", "3"},
		},
	}

	for _, item := range tt {
		id, err := arrayTypes.Insert(item)
		assert.NoError(t, err)

		if pk, ok := id.(int64); !ok || pk == 0 {
			t.Fatalf("Expecting an ID.")
		}

		var itemCheck arrayType
		err = arrayTypes.Find(db.Cond{"id": id}).One(&itemCheck)
		assert.NoError(t, err)
		assert.Len(t, itemCheck.Integers, len(item.Integers))
		assert.Len(t, itemCheck.Strings, len(item.Strings))

		// Check nil/zero values just to make sure that the arrays won't
		// be JSON-marshalled into `null` instead of empty array `[]`.
		assert.NotNil(t, itemCheck.Integers)
		assert.NotNil(t, itemCheck.Strings)
		assert.NotZero(t, itemCheck.Integers)
		assert.NotZero(t, itemCheck.Strings)
	}
}

func TestIssue210(t *testing.T) {
	list := []string{
		`DROP TABLE IF EXISTS testing123`,
		`DROP TABLE IF EXISTS hello`,
		`CREATE TABLE IF NOT EXISTS testing123 (
			ID INT PRIMARY KEY     NOT NULL,
			NAME           TEXT    NOT NULL
		)
		`,
		`CREATE TABLE IF NOT EXISTS hello (
			ID INT PRIMARY KEY     NOT NULL,
			NAME           TEXT    NOT NULL
		)`,
	}

	sess := mustOpen()
	defer sess.Close()

	tx, err := sess.NewTx()
	assert.NoError(t, err)

	for i := range list {
		_, err = tx.Exec(list[i])
		assert.NoError(t, err)
	}

	err = tx.Commit()
	assert.NoError(t, err)

	_, err = sess.Collection("testing123").Find().Count()
	assert.NoError(t, err)

	_, err = sess.Collection("hello").Find().Count()
	assert.NoError(t, err)
}

func TestNonTrivialSubqueries(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	{
		q, err := sess.Query(`WITH test AS (?) ?`,
			sess.Select("id AS foo").From("artist"),
			sess.Select("foo").From("test").Where("foo > ?", 0),
		)

		assert.NoError(t, err)
		assert.NotNil(t, q)

		assert.True(t, q.Next())

		var number int
		assert.NoError(t, q.Scan(&number))

		assert.Equal(t, 1, number)
		assert.NoError(t, q.Close())
	}

	{
		row, err := sess.QueryRow(`WITH test AS (?) ?`,
			sess.Select("id AS foo").From("artist"),
			sess.Select("foo").From("test").Where("foo > ?", 0),
		)

		assert.NoError(t, err)
		assert.NotNil(t, row)

		var number int
		assert.NoError(t, row.Scan(&number))

		assert.Equal(t, 1, number)
	}

	{
		res, err := sess.Exec(`UPDATE artist a1 SET id = ?`,
			sess.Select(db.Raw("id + 1")).From("artist a2").Where("a2.id = a1.id"),
		)

		assert.NoError(t, err)
		assert.NotNil(t, res)
	}
}
