package exql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestColumnValue(t *testing.T) {
	cv := &ColumnValue{Column: ColumnWithName("id"), Operator: "=", Value: NewValue(1)}
	s, err := cv.Compile(defaultTemplate)
	assert.NoError(t, err)
	assert.Equal(t, `"id" = '1'`, s)

	cv = &ColumnValue{Column: ColumnWithName("date"), Operator: "=", Value: &Raw{Value: "NOW()"}}
	s, err = cv.Compile(defaultTemplate)
	assert.NoError(t, err)
	assert.Equal(t, `"date" = NOW()`, s)
}

func TestColumnValues(t *testing.T) {
	cvs := JoinColumnValues(
		&ColumnValue{Column: ColumnWithName("id"), Operator: ">", Value: NewValue(8)},
		&ColumnValue{Column: ColumnWithName("other.id"), Operator: "<", Value: NewValue(&Raw{Value: "100"})},
		&ColumnValue{Column: ColumnWithName("name"), Operator: "=", Value: NewValue("Haruki Murakami")},
		&ColumnValue{Column: ColumnWithName("created"), Operator: ">=", Value: NewValue(&Raw{Value: "NOW()"})},
		&ColumnValue{Column: ColumnWithName("modified"), Operator: "<=", Value: NewValue(&Raw{Value: "NOW()"})},
	)

	s, err := cvs.Compile(defaultTemplate)
	assert.NoError(t, err)
	assert.Equal(t, `"id" > '8', "other"."id" < 100, "name" = 'Haruki Murakami', "created" >= NOW(), "modified" <= NOW()`, s)
}

func BenchmarkNewColumnValue(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = &ColumnValue{Column: ColumnWithName("a"), Operator: "=", Value: NewValue(Raw{Value: "7"})}
	}
}

func BenchmarkColumnValueHash(b *testing.B) {
	cv := &ColumnValue{Column: ColumnWithName("id"), Operator: "=", Value: NewValue(1)}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cv.Hash()
	}
}

func BenchmarkColumnValueCompile(b *testing.B) {
	cv := &ColumnValue{Column: ColumnWithName("id"), Operator: "=", Value: NewValue(1)}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cv.Compile(defaultTemplate)
	}
}

func BenchmarkColumnValueCompileNoCache(b *testing.B) {
	for i := 0; i < b.N; i++ {
		cv := &ColumnValue{Column: ColumnWithName("id"), Operator: "=", Value: NewValue(1)}
		_, _ = cv.Compile(defaultTemplate)
	}
}

func BenchmarkJoinColumnValues(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = JoinColumnValues(
			&ColumnValue{Column: ColumnWithName("id"), Operator: ">", Value: NewValue(8)},
			&ColumnValue{Column: ColumnWithName("other.id"), Operator: "<", Value: NewValue(Raw{Value: "100"})},
			&ColumnValue{Column: ColumnWithName("name"), Operator: "=", Value: NewValue("Haruki Murakami")},
			&ColumnValue{Column: ColumnWithName("created"), Operator: ">=", Value: NewValue(Raw{Value: "NOW()"})},
			&ColumnValue{Column: ColumnWithName("modified"), Operator: "<=", Value: NewValue(Raw{Value: "NOW()"})},
		)
	}
}

func BenchmarkColumnValuesHash(b *testing.B) {
	cvs := JoinColumnValues(
		&ColumnValue{Column: ColumnWithName("id"), Operator: ">", Value: NewValue(8)},
		&ColumnValue{Column: ColumnWithName("other.id"), Operator: "<", Value: NewValue(Raw{Value: "100"})},
		&ColumnValue{Column: ColumnWithName("name"), Operator: "=", Value: NewValue("Haruki Murakami")},
		&ColumnValue{Column: ColumnWithName("created"), Operator: ">=", Value: NewValue(Raw{Value: "NOW()"})},
		&ColumnValue{Column: ColumnWithName("modified"), Operator: "<=", Value: NewValue(Raw{Value: "NOW()"})},
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cvs.Hash()
	}
}

func BenchmarkColumnValuesCompile(b *testing.B) {
	cvs := JoinColumnValues(
		&ColumnValue{Column: ColumnWithName("id"), Operator: ">", Value: NewValue(8)},
		&ColumnValue{Column: ColumnWithName("other.id"), Operator: "<", Value: NewValue(Raw{Value: "100"})},
		&ColumnValue{Column: ColumnWithName("name"), Operator: "=", Value: NewValue("Haruki Murakami")},
		&ColumnValue{Column: ColumnWithName("created"), Operator: ">=", Value: NewValue(Raw{Value: "NOW()"})},
		&ColumnValue{Column: ColumnWithName("modified"), Operator: "<=", Value: NewValue(Raw{Value: "NOW()"})},
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cvs.Compile(defaultTemplate)
	}
}

func BenchmarkColumnValuesCompileNoCache(b *testing.B) {
	for i := 0; i < b.N; i++ {
		cvs := JoinColumnValues(
			&ColumnValue{Column: ColumnWithName("id"), Operator: ">", Value: NewValue(8)},
			&ColumnValue{Column: ColumnWithName("other.id"), Operator: "<", Value: NewValue(Raw{Value: "100"})},
			&ColumnValue{Column: ColumnWithName("name"), Operator: "=", Value: NewValue("Haruki Murakami")},
			&ColumnValue{Column: ColumnWithName("created"), Operator: ">=", Value: NewValue(Raw{Value: "NOW()"})},
			&ColumnValue{Column: ColumnWithName("modified"), Operator: "<=", Value: NewValue(Raw{Value: "NOW()"})},
		)
		_, _ = cvs.Compile(defaultTemplate)
	}
}
