package sqlbuilder

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAsWithoutFromDoesNotNilPanic(t *testing.T) {
	b := &sqlBuilder{t: newTemplateWithUtils(&testTemplate)}

	// Valid path still works.
	assert.Contains(t, b.SelectFrom("artist").As("a").String(), "artist")

	// As without From used to nil-deref inside statement(); now surface a real error.
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic with error message, got none")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("expected string panic, got %T %v", r, r)
		}
		if strings.Contains(msg, "nil pointer") || strings.Contains(msg, "invalid memory") {
			t.Fatalf("still nil-pointer style panic: %s", msg)
		}
		if !strings.Contains(msg, "As()") {
			t.Fatalf("unexpected error message: %s", msg)
		}
	}()
	_ = b.Select("id").As("alias").String()
}

func TestSelectorStatementReturnsBuildError(t *testing.T) {
	b := &sqlBuilder{t: newTemplateWithUtils(&testTemplate)}
	sel := b.Select("id").As("alias").(*selector)
	st, err := sel.statement()
	assert.Nil(t, st)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "As()")
}
