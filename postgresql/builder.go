package postgresql

import (
	"upper.io/db"
)

type Builder struct {
	sess *database
}

func (b *Builder) Select(fields ...interface{}) db.QuerySelector {
	return &QuerySelector{builder: b, fields: fields}
}

type QuerySelector struct {
	builder *Builder
	fields  []interface{}
}

func (qs *QuerySelector) From(table ...string) db.Result {
	return qs.builder.sess.C(table...).Find().Select(qs.fields...)
}
