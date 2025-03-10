package db_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/upper/db/v4"
	"github.com/upper/db/v4/adapter/cockroachdb"
	"github.com/upper/db/v4/adapter/mongo"
	"github.com/upper/db/v4/adapter/mssql"
	"github.com/upper/db/v4/adapter/mysql"
	"github.com/upper/db/v4/adapter/postgresql"

	cockroachdbtest "github.com/upper/db/v4/tests/cockroachdb"
	mongotest "github.com/upper/db/v4/tests/mongo"
	mssqltest "github.com/upper/db/v4/tests/mssql"
	mysqltest "github.com/upper/db/v4/tests/mysql"
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

func mysqlCfg(port int) mysql.ConnectionURL {
	return mysql.ConnectionURL{
		Database: os.Getenv("DB_NAME"),
		User:     os.Getenv("DB_USERNAME"),
		Password: os.Getenv("DB_PASSWORD"),
		Host:     os.Getenv("DB_HOST") + ":" + fmt.Sprintf("%d", port),
		Options: map[string]string{
			"parseTime": "true",
			"time_zone": fmt.Sprintf(`'%s'`, TimeZone),
			"loc":       TimeZone,
		},
	}
}

func cockroachdbCfg(port int) cockroachdb.ConnectionURL {
	return cockroachdb.ConnectionURL{
		Database: os.Getenv("DB_NAME"),
		User:     os.Getenv("DB_USERNAME"),
		Password: os.Getenv("DB_PASSWORD"),
		Host:     os.Getenv("DB_HOST") + ":" + fmt.Sprintf("%d", port),
		Options: map[string]string{
			"timezone": os.Getenv("DB_TIMEZONE"),
		},
	}
}

func mssqlCfg(port int) mssql.ConnectionURL {
	return mssql.ConnectionURL{
		Database: os.Getenv("DB_NAME"),
		User:     os.Getenv("DB_USERNAME"),
		Password: os.Getenv("DB_PASSWORD"),
		Host:     os.Getenv("DB_HOST") + ":" + fmt.Sprintf("%d", port),
		Options:  map[string]string{},
	}
}

func mongoCfg(port int) mongo.ConnectionURL {
	return mongo.ConnectionURL{
		Database: os.Getenv("DB_NAME"),
		User:     os.Getenv("DB_USERNAME"),
		Password: os.Getenv("DB_PASSWORD"),
		Host:     os.Getenv("DB_HOST") + ":" + fmt.Sprintf("%d", port),
		Options:  map[string]string{},
	}
}

func TestMain(t *testing.T) {
	testCfgs := map[string]Helper{
		"postgresql-17": postgresqltest.NewHelper(postgresqlCfg(5432)),
		"postgresql-16": postgresqltest.NewHelper(postgresqlCfg(5433)),
		"postgresql-15": postgresqltest.NewHelper(postgresqlCfg(5434)),
		"mysql-tls":     mysqltest.NewHelper(mysqlCfg(3306)),
		"mysql-5":       mysqltest.NewHelper(mysqlCfg(3307)),
		"cockroach-v23": cockroachdbtest.NewHelper(cockroachdbCfg(26257)),
		"cockroach-v22": cockroachdbtest.NewHelper(cockroachdbCfg(26258)),
		"cockroach-v21": cockroachdbtest.NewHelper(cockroachdbCfg(26259)),
		"mssql-2022":    mssqltest.NewHelper(mssqlCfg(1433)),
		"mssql-2019":    mssqltest.NewHelper(mssqlCfg(1434)),
		"mongo-8":       mongotest.NewHelper(mongoCfg(27017)),
		"mongo-7":       mongotest.NewHelper(mongoCfg(27018)),
	}

	for name, helper := range testCfgs {
		t.Run("Generic "+name, func(t *testing.T) {
			suite.Run(t, &GenericTestSuite{Helper: helper})
		})

		t.Run("Record "+name, func(t *testing.T) {
			suite.Run(t, &RecordTestSuite{Helper: helper})
		})

		t.Run("SQL "+name, func(t *testing.T) {
			suite.Run(t, &SQLTestSuite{Helper: helper})
		})
	}
}
