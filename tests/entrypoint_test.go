package db_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/upper/db/v4"
	"github.com/upper/db/v4/adapter/postgresql"

	postgresqltest "github.com/upper/db/v4/tests/postgresql"
)

const (
	defaultUsername = "upper_user"
	defaultPassword = "upp3r//S3cr37"
	defaultDatabase = "upper"
	defaultHost     = "127.0.0.1"
	defaultTimeZone = "Canada/Eastern"
)

const TimeZone = "Canada/Eastern"

var defaultTimeLocation, _ = time.LoadLocation(TimeZone)

type Helper interface {
	Session() db.Session

	Adapter() string

	SetUp() error
	TearDown() error
}

type Suite struct {
	suite.Suite

	Helper
}

func (s *Suite) AfterTest(suiteName, testName string) {
	err := s.TearDown()
	s.NoError(err)
}

func (s *Suite) BeforeTest(suiteName, testName string) {
	err := s.SetUp()
	s.NoError(err)
}

func init() {
	defaultMap := map[string]string{
		"DB_USERNAME": defaultUsername,
		"DB_PASSWORD": defaultPassword,
		"DB_NAME":     defaultDatabase,
		"DB_HOST":     defaultHost,
		"DB_TIMEZONE": defaultTimeZone,
	}

	for k, v := range defaultMap {
		if os.Getenv(k) == "" {
			os.Setenv(k, v)
		}
	}
}

func postgresqlCfg(port int) postgresql.ConnectionURL {
	return postgresql.ConnectionURL{
		Database: os.Getenv("DB_NAME"),
		User:     os.Getenv("DB_USERNAME"),
		Password: os.Getenv("DB_PASSWORD"),
		Host:     os.Getenv("DB_HOST") + ":" + fmt.Sprintf("%d", port),
		Options: map[string]string{
			"timezone": os.Getenv("DB_TIMEZONE"),
		},
	}
}

func TestMain(t *testing.T) {
	testCfgs := map[string]db.ConnectionURL{
		"postgresql-17": postgresqlCfg(5432),
		"postgresql-16": postgresqlCfg(5433),
		"postgresql-15": postgresqlCfg(5434),
	}

	for name, cfg := range testCfgs {
		helper := postgresqltest.NewHelper(cfg.(postgresql.ConnectionURL))

		t.Run(name+" SQL", func(t *testing.T) {
			suite.Run(t, &SQLTestSuite{Helper: helper})
		})

		t.Run(name+" Record", func(t *testing.T) {
			suite.Run(t, &RecordTestSuite{Helper: helper})
		})

		t.Run(name+" Generic", func(t *testing.T) {
			suite.Run(t, &GenericTestSuite{Helper: helper})
		})
	}
}
