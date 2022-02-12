package sqlbuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	db "github.com/upper/db/v4"
)

func TestPrepareForDisplay(t *testing.T) {
	samples := []struct {
		In  string
		Out string
	}{
		{
			In:  "12345",
			Out: "12345",
		},
		{
			In:  "\r\n\t12345",
			Out: "12345",
		},
		{
			In:  "12345\r\n\t",
			Out: "12345",
		},
		{
			In:  "\r\n\t1\r2\n3\t4\r5\r\n\t",
			Out: "1 2 3 4 5",
		},
		{
			In:  "\r\n    \t  1\r 2\n 3\t 4\r    5\r    \n\t",
			Out: "1 2 3 4 5",
		},
		{
			In:  "\r\n    \t  11\r 22\n 33\t    44 \r      55",
			Out: "11 22 33 44 55",
		},
		{
			In:  "11\r    22\n 33\t       44 \r      55",
			Out: "11 22 33 44 55",
		},
		{
			In:  "1  2  3 4 5",
			Out: "1 2 3 4 5",
		},
		{
			In:  "?",
			Out: "$1",
		},
		{
			In:  "? ?",
			Out: "$1 $2",
		},
		{
			In:  "?  ?    ?",
			Out: "$1 $2 $3",
		},
		{
			In:  " ?  ?    ?        ",
			Out: "$1 $2 $3",
		},
		{
			In:  "???",
			Out: "$1$2$3",
		},
	}
	for _, sample := range samples {
		assert.Equal(t, sample.Out, prepareQueryForDisplay(sample.In))
	}
}

func TestPlaceholderSimple(t *testing.T) {
	{
		ret, _ := Preprocess("?", []interface{}{1})
		assert.Equal(t, "?", ret)
	}
	{
		ret, _ := Preprocess("?", nil)
		assert.Equal(t, "?", ret)
	}
}

func TestPlaceholderMany(t *testing.T) {
	{
		ret, _ := Preprocess("?, ?, ?", []interface{}{1, 2, 3})
		assert.Equal(t, "?, ?, ?", ret)
	}
}

func TestPlaceholderArray(t *testing.T) {
	{
		ret, _ := Preprocess("?, ?, ?", []interface{}{1, 2, []interface{}{3, 4, 5}})
		assert.Equal(t, "?, ?, (?, ?, ?)", ret)
	}

	{
		ret, _ := Preprocess("?, ?, ?", []interface{}{[]interface{}{1, 2, 3}, 4, 5})
		assert.Equal(t, "(?, ?, ?), ?, ?", ret)
	}

	{
		ret, _ := Preprocess("?, ?, ?", []interface{}{1, []interface{}{2, 3, 4}, 5})
		assert.Equal(t, "?, (?, ?, ?), ?", ret)
	}

	{
		ret, _ := Preprocess("???", []interface{}{1, []interface{}{2, 3, 4}, 5})
		assert.Equal(t, "?(?, ?, ?)?", ret)
	}

	{
		ret, _ := Preprocess("??", []interface{}{[]interface{}{1, 2, 3}, []interface{}{}, []interface{}{4, 5}, []interface{}{}})
		assert.Equal(t, "(?, ?, ?)(NULL)", ret)
	}
}

func TestPlaceholderArguments(t *testing.T) {
	{
		_, args := Preprocess("?, ?, ?", []interface{}{1, 2, []interface{}{3, 4, 5}})
		assert.Equal(t, []interface{}{1, 2, 3, 4, 5}, args)
	}

	{
		_, args := Preprocess("?, ?, ?", []interface{}{1, []interface{}{2, 3, 4}, 5})
		assert.Equal(t, []interface{}{1, 2, 3, 4, 5}, args)
	}

	{
		_, args := Preprocess("?, ?, ?", []interface{}{[]interface{}{1, 2, 3}, 4, 5})
		assert.Equal(t, []interface{}{1, 2, 3, 4, 5}, args)
	}

	{
		_, args := Preprocess("?, ?", []interface{}{[]interface{}{1, 2, 3}, []interface{}{4, 5}})
		assert.Equal(t, []interface{}{1, 2, 3, 4, 5}, args)
	}
}

func TestPlaceholderReplace(t *testing.T) {
	{
		ret, args := Preprocess("?, ?, ?", []interface{}{1, db.Raw("foo"), 3})
		assert.Equal(t, "?, foo, ?", ret)
		assert.Equal(t, []interface{}{1, 3}, args)
	}
}
