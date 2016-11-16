package postgresql

import (
	"database/sql"
	"os"
	"sync"
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

// The "driver: bad connection" problem (driver.ErrBadConn) happens when the
// database process is abnormally interrupted, this can happen for a variety of
// reasons, for instance when the database process is OOM killed by the OS.
func TestDriverBadConnection(t *testing.T) {
	sess := mustOpen()
	defer sess.Close()

	db.Conf.SetRetryQueryOnError(true)
	defer db.Conf.SetRetryQueryOnError(false)

	var tMu sync.Mutex
	tFatal := func(err error) {
		tMu.Lock()
		defer tMu.Unlock()
		t.Fatal(err)
	}

	const concurrentOpts = 20
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := sess.Collection("artist").Find().Count()
			if err != nil {
				tFatal(err)
			}
		}(i)
		if i%concurrentOpts == (concurrentOpts - 1) {
			// This triggers the "bad connection" problem, if you want to see that
			// instead of retrying the query set maxQueryRetryAttempts to 0.
			wg.Add(1)
			go func() {
				defer wg.Done()
				sess.Query("SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND datname = ?", os.Getenv("DB_NAME"))
			}()
			wg.Wait()
		}
	}

	wg.Wait()
}
