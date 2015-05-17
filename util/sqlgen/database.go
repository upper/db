package sqlgen

import (
	"fmt"
)

type Database struct {
	Name string
	hash string
}

func (d *Database) Hash() string {
	if d.hash == "" {
		d.hash = fmt.Sprintf(`sqlgen.Database{Name:%q}`, d.Name)
	}
	return d.hash
}

func (d *Database) Compile(layout *Template) (compiled string) {
	if c, ok := layout.Read(d); ok {
		return c
	}

	compiled = mustParse(layout.IdentifierQuote, Raw{Value: d.Name})

	layout.Write(d, compiled)

	return
}
