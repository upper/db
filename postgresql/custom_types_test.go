package postgresql

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type testStruct struct {
	X int    `json:"x"`
	Z string `json:"z"`
	V JSONB  `json:"v"`
}

func TestDecodeJSONB(t *testing.T) {
	a := testStruct{}
	err := DecodeJSONB(&a, []byte(`{"x": 5, "z": "Hello", "v": 1}`))
	assert.NoError(t, err)
	assert.Equal(t, "Hello", a.Z)
	assert.Equal(t, float64(1), a.V.V)
	assert.Equal(t, 5, a.X)
}
