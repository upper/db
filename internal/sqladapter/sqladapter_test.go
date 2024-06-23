package sqladapter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/upper/db/v4"
)

var (
	_ db.Collection = &collectionWithSession{}
	_ Collection    = &collectionWithSession{}
)

func TestReplaceWithDollarSign(t *testing.T) {
	tests := []struct {
		in  string
		out string
	}{
		{
			`SELECT ?`,
			`SELECT $1`,
		},
		{
			`SELECT ? FROM ? WHERE ?`,
			`SELECT $1 FROM $2 WHERE $3`,
		},
		{
			`SELECT ?? FROM ? WHERE ??`,
			`SELECT ? FROM $1 WHERE ?`,
		},
		{
			`SELECT ??? FROM ? WHERE ??`,
			`SELECT ?$1 FROM $2 WHERE ?`,
		},
		{
			`SELECT ??? FROM ? WHERE ????`,
			`SELECT ?$1 FROM $2 WHERE ??`,
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("Case_%03d", i), func(t *testing.T) {
			assert.Equal(t, []byte(test.out), ReplaceWithDollarSign([]byte(test.in)))
		})
	}
}
