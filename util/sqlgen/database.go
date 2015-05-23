package sqlgen

import (
	"fmt"
)

// Database represents a SQL database.
type Database struct {
	Name string
	hash string
}

// DatabaseWithName returns a Database with the given name.
func DatabaseWithName(name string) *Database {
	return &Database{Name: name}
}

// Hash returns a unique identifier.
func (d *Database) Hash() string {
	if d.hash == "" {
		d.hash = fmt.Sprintf(`Database{Name:%q}`, d.Name)
	}
	return d.hash
}

// Compile transforms the Database into an equivalent SQL representation.
func (d *Database) Compile(layout *Template) (compiled string) {
	if c, ok := layout.Read(d); ok {
		return c
	}

	compiled = mustParse(layout.IdentifierQuote, Raw{Value: d.Name})

	layout.Write(d, compiled)

	return
}
