package schema

type DatabaseSchema struct {
	Name      string
	Alias     string
	Tables    []string
	TableInfo map[string]*TableSchema
}

type TableSchema struct {
	PrimaryKey string
	Alias      string
	Columns    []string
}

func NewDatabaseSchema() *DatabaseSchema {
	schema := new(DatabaseSchema)
	schema.Tables = []string{}
	schema.TableInfo = make(map[string]*TableSchema)
	return schema
}

func (d *DatabaseSchema) AddTable(name string) {
	if _, ok := d.TableInfo[name]; !ok {
		table := new(TableSchema)
		table.Columns = []string{}
		d.TableInfo[name] = table
		d.Tables = append(d.Tables, name)
	}
}

func (d *DatabaseSchema) Table(name string) *TableSchema {
	d.AddTable(name)
	return d.TableInfo[name]
}
