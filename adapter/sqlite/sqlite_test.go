package sqlite

import (
	"path/filepath"
	"testing"

	"database/sql"

	"github.com/stretchr/testify/suite"
	"github.com/upper/db/v4/internal/testsuite"
)

type AdapterTests struct {
	testsuite.Suite
}

func (s *AdapterTests) SetupSuite() {
	s.Helper = &Helper{}
}

func (s *AdapterTests) Test_Issue633_OpenSession() {
	sess, err := Open(settings)
	s.NoError(err)
	defer sess.Close()

	absoluteName, _ := filepath.Abs(settings.Database)
	s.Equal(absoluteName, sess.Name())
}

func (s *AdapterTests) Test_Issue633_NewAdapterWithFile() {
	sqldb, err := sql.Open("sqlite3", settings.Database)
	s.NoError(err)

	sess, err := New(sqldb)
	s.NoError(err)
	defer sess.Close()

	absoluteName, _ := filepath.Abs(settings.Database)
	s.Equal(absoluteName, sess.Name())
}

func (s *AdapterTests) Test_Issue633_NewAdapterWithMemory() {
	sqldb, err := sql.Open("sqlite3", ":memory:")
	s.NoError(err)

	sess, err := New(sqldb)
	s.NoError(err)
	defer sess.Close()

	s.Equal("main", sess.Name())
}

func TestAdapter(t *testing.T) {
	suite.Run(t, &AdapterTests{})
}
