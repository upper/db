package db_test

import (
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/suite"
)

type testHelperConfig struct {
	initFn func() (Helper, string, int)

	withoutGeneric bool
	withoutRecord  bool
	withoutSQL     bool
}

func serverUp(t *testing.T, name string, host string, port int) {
	cmd := exec.Command("make", "-C", "ansible", "server-up")

	cmd.Env = append(
		os.Environ(),
		"TARGET="+name,
		"CONTAINER_BIND_HOST="+host,
		"CONTAINER_BIND_PORT="+fmt.Sprintf("%d", port),
	)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Run()
	if err != nil {
		t.Fatalf("could not start server: %v", err)
	}
}

func serverDown(t *testing.T, name string) {
	cmd := exec.Command("make", "-C", "ansible", "server-down")

	cmd.Env = append(
		os.Environ(),
		"TARGET="+name,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Run()
	if err != nil {
		t.Fatalf("could not stop server: %v", err)
	}
}

func Test(t *testing.T) {
	testCfgs := map[string]testHelperConfig{
		"postgresql-17": {
			initFn: newPostgreSQLTestHelper(),
		},
		"postgresql-16": {
			initFn: newPostgreSQLTestHelper(),
		},
		"postgresql-15": {
			initFn: newPostgreSQLTestHelper(),
		},
		"mysql-latest": {
			initFn: newMySQLTestHelper(),
		},
		"mysql-lts": {
			initFn: newMySQLTestHelper(),
		},
		"mysql-5": {
			initFn: newMySQLTestHelper(),
		},

		"cockroachdb-v23": {
			initFn: newCockroachDBTestHelper(),
		},
		"cockroachdb-v22": {
			initFn: newCockroachDBTestHelper(),
		},

		"mssql-2022": {
			initFn:        newMSSQLTestHelper(),
			withoutRecord: true,
		},
		"mssql-2019": {
			initFn:        newMSSQLTestHelper(),
			withoutRecord: true, // TODO: fix MSSQL record tests
		},

		"mongodb-8": {
			initFn:        newMongoDBTestHelper(),
			withoutSQL:    true,
			withoutRecord: true,
		},
		"mongodb-7": {
			initFn:        newMongoDBTestHelper(),
			withoutSQL:    true,
			withoutRecord: true,
		},
	}

	for name, cfg := range testCfgs {
		t.Run(name, func(t *testing.T) {
			helper, bindAddr, bindPort := cfg.initFn()

			serverUp(t, name, bindAddr, bindPort)
			defer serverDown(t, name)

			t.Run("Generic", func(t *testing.T) {
				if cfg.withoutGeneric {
					t.Skip("Generic tests are disabled for this adapter")
				}

				suite.Run(t, &GenericTestSuite{Helper: helper})
			})

			t.Run("Record", func(t *testing.T) {
				if cfg.withoutRecord {
					t.Skip("Record tests are disabled for this adapter")
				}

				suite.Run(t, &RecordTestSuite{Helper: helper})
			})

			t.Run("SQL", func(t *testing.T) {
				if cfg.withoutSQL {
					t.Skip("SQL tests are disabled for this adapter")
				}

				suite.Run(t, &SQLTestSuite{Helper: helper})
			})
		})
	}
}
