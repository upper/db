package postgresql

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testStruct struct {
	X int         `json:"x"`
	Z string      `json:"z"`
	V interface{} `json:"v"`
}

func TestScanJSONB(t *testing.T) {
	{
		a := testStruct{}
		err := ScanJSONB(&a, []byte(`{"x": 5, "z": "Hello", "v": 1}`))
		assert.NoError(t, err)
		assert.Equal(t, "Hello", a.Z)
		assert.Equal(t, float64(1), a.V)
		assert.Equal(t, 5, a.X)
	}
	{
		a := testStruct{}
		err := ScanJSONB(&a, []byte(`{"x": 5, "z": "Hello", "v": null}`))
		assert.NoError(t, err)
		assert.Equal(t, "Hello", a.Z)
		assert.Equal(t, nil, a.V)
		assert.Equal(t, 5, a.X)
	}
	{
		a := testStruct{}
		err := ScanJSONB(&a, []byte(`{"x": 5, "z": "Hello"}`))
		assert.NoError(t, err)
		assert.Equal(t, "Hello", a.Z)
		assert.Equal(t, nil, a.V)
		assert.Equal(t, 5, a.X)
	}
	{
		a := testStruct{}
		err := ScanJSONB(&a, []byte(`{"v": "Hello"}`))
		assert.NoError(t, err)
		assert.Equal(t, "Hello", a.V)
	}
	{
		a := testStruct{}
		err := ScanJSONB(&a, []byte(`{"v": true}`))
		assert.NoError(t, err)
		assert.Equal(t, true, a.V)
	}
	{
		a := testStruct{}
		err := ScanJSONB(&a, []byte(`{}`))
		assert.NoError(t, err)
		assert.Equal(t, nil, a.V)
	}
	{
		a := []*testStruct{}
		err := json.Unmarshal([]byte(`[{}]`), &a)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(a))
		assert.Nil(t, a[0].V)
	}
	{
		a := []*testStruct{}
		err := json.Unmarshal([]byte(`[{"v": true}]`), &a)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(a))
		assert.Equal(t, true, a[0].V)
	}
	{
		a := []*testStruct{}
		err := json.Unmarshal([]byte(`[{"v": null}]`), &a)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(a))
		assert.Nil(t, a[0].V)
	}
	{
		a := []*testStruct{}
		err := json.Unmarshal([]byte(`[{"v": 12.34}]`), &a)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(a))
		assert.Equal(t, 12.34, a[0].V)
	}
}
