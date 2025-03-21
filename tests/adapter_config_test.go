package db_test

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
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
	defaultUsername = "upper_db_user"
	defaultPassword = "upp3r//S3cr37"
	defaultDatabase = "upper_db"
	defaultHost     = "127.0.0.1"
	defaultTimeZone = "Canada/Eastern"
)

const (
	minPort = 40000
	maxPort = 60000
)

const TimeZone = defaultTimeZone

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
	s.Require().NoError(err)
}

func (s *Suite) BeforeTest(suiteName, testName string) {
	err := s.SetUp()
	s.Require().NoError(err)
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

func getRandomPort() int {
	for i := 0; i < 5; i++ {
		port := minPort + rand.Intn(maxPort-minPort)

		li, err := net.Listen("tcp", defaultHost+":"+strconv.Itoa(port))
		if err == nil {
			li.Close()
			return port
		}
	}

	log.Fatalf("could not find a free port")

	return 0
}

func newPostgreSQLTestHelper() func() (Helper, string, int) {
	return func() (Helper, string, int) {
		host, port := os.Getenv("DB_HOST"), getRandomPort()

		connURL := postgresql.ConnectionURL{
			Database: os.Getenv("DB_NAME"),
			User:     os.Getenv("DB_USERNAME"),
			Password: os.Getenv("DB_PASSWORD"),
			Host:     host + ":" + fmt.Sprintf("%d", port),
			Options: map[string]string{
				"timezone": os.Getenv("DB_TIMEZONE"),
			},
		}

		helper := postgresqltest.NewHelper(connURL)
		return helper, host, port
	}
}

func newMySQLTestHelper() func() (Helper, string, int) {
	return func() (Helper, string, int) {
		host, port := os.Getenv("DB_HOST"), getRandomPort()

		connURL := mysql.ConnectionURL{
			Database: os.Getenv("DB_NAME"),
			User:     os.Getenv("DB_USERNAME"),
			Password: os.Getenv("DB_PASSWORD"),
			Host:     host + ":" + fmt.Sprintf("%d", port),
			Options: map[string]string{
				"parseTime": "true",
				"time_zone": fmt.Sprintf(`'%s'`, TimeZone),
				"loc":       TimeZone,
			},
		}

		helper := mysqltest.NewHelper(connURL)
		return helper, host, port
	}
}

func newCockroachDBTestHelper() func() (Helper, string, int) {
	return func() (Helper, string, int) {
		host, port := os.Getenv("DB_HOST"), getRandomPort()

		connURL := cockroachdb.ConnectionURL{
			Database: os.Getenv("DB_NAME"),
			User:     os.Getenv("DB_USERNAME"),
			Password: os.Getenv("DB_PASSWORD"),
			Host:     host + ":" + fmt.Sprintf("%d", port),
			Options: map[string]string{
				"timezone": os.Getenv("DB_TIMEZONE"),
			},
		}

		helper := cockroachdbtest.NewHelper(connURL)
		return helper, host, port
	}
}

func newMSSQLTestHelper() func() (Helper, string, int) {
	return func() (Helper, string, int) {
		host, port := os.Getenv("DB_HOST"), getRandomPort()

		connURL := mssql.ConnectionURL{
			Database: "master",
			User:     "sa",
			Password: os.Getenv("DB_PASSWORD"),
			Host:     host + ":" + fmt.Sprintf("%d", port),
			Options:  map[string]string{},
		}

		helper := mssqltest.NewHelper(connURL)
		return helper, host, port
	}
}

func newMongoDBTestHelper() func() (Helper, string, int) {
	return func() (Helper, string, int) {
		host, port := os.Getenv("DB_HOST"), getRandomPort()

		connURL := mongo.ConnectionURL{
			Database: "admin",
			User:     os.Getenv("DB_USERNAME"),
			Password: os.Getenv("DB_PASSWORD"),
			Host:     host + ":" + fmt.Sprintf("%d", port),
			Options:  map[string]string{},
		}

		helper := mongotest.NewHelper(connURL)
		return helper, host, port
	}
}
