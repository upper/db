package sqladapter

import (
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

	for _, test := range tests {
		assert.Equal(t, test.out, ReplaceWithDollarSign(test.in))
	}
}
