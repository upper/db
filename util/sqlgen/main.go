package sqlgen

import (
	"bytes"
	"text/template"
)

type Type uint

const (
	SqlTruncate = iota
	SqlDropTable
	SqlDropDatabase
	SqlSelectCount
	SqlInsert
	SqlSelect
	SqlUpdate
	SqlDelete
)

type (
	Limit  int
	Offset int
	Extra  string
)

var (
	parsedTemplates = make(map[string]*template.Template)
)

func mustParse(text string, data interface{}) string {
	var b bytes.Buffer
	var ok bool

	if _, ok = parsedTemplates[text]; !ok {
		parsedTemplates[text] = template.Must(template.New("").Parse(text))
	}

	if err := parsedTemplates[text].Execute(&b, data); err != nil {
		panic("There was an error compiling the following template:\n" + text + "\nError was: " + err.Error())
	}

	return b.String()
}
