// +build !generated

package mysql

import (
	"log"
	"testing"

	"github.com/upper/db/lib/sqlbuilder"
)

func mustOpen() sqlbuilder.Database {
	return nil
}

func TestMain(*testing.M) {
	log.Fatal(`Tests use generated code and a custom database, please use "make test".`)
}
