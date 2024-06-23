package reflectx

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type E1 struct {
	A int
}
type E2 struct {
	E1
	B int
}
type E3 struct {
	E2
	C int
}
type E4 struct {
	E3
	D int
}

func TestReflectMapper(t *testing.T) {
	t.Run("TopLevelField", func(t *testing.T) {
		type A struct {
			F0 int
			F1 int
			F2 int
		}

		f := A{1, 2, 3}
		fv := reflect.ValueOf(f)

		m := NewMapperFunc("", func(s string) string { return s })

		{
			v := m.FieldByName(fv, "F0")
			assert.Equal(t, f.F0, v.Interface().(int))
		}

		{
			v := m.FieldByName(fv, "F2")
			assert.Equal(t, f.F2, v.Interface().(int))
		}
	})

	t.Run("NestedFields", func(t *testing.T) {
		type A struct {
			F0 int
			F1 int
			F2 int
		}

		type B struct {
			A // nested

			F3 int
			F4 int
		}

		type C struct {
			F5 int

			B `db:"B"`
		}

		c := C{1, B{A{2, 3, 4}, 5, 6}}
		cv := reflect.ValueOf(c)

		m := NewMapperFunc("db", func(s string) string { return s })

		assert.Equal(t, 1, m.FieldByName(cv, "F5").Interface().(int))
		assert.Equal(t, 2, m.FieldByName(cv, "B.F0").Interface().(int))
		assert.Equal(t, 3, m.FieldByName(cv, "B.F1").Interface().(int))
		assert.Equal(t, 4, m.FieldByName(cv, "B.F2").Interface().(int))
		assert.Equal(t, 5, m.FieldByName(cv, "B.F3").Interface().(int))
		assert.Equal(t, 6, m.FieldByName(cv, "B.F4").Interface().(int))

		assert.False(t, m.FieldByName(cv, "D").IsValid())

		t.Run("TypeMap", func(t *testing.T) {
			fields := m.TypeMap(reflect.TypeOf(c))

			assert.Equal(t, 8, len(fields.Index))
			assert.Equal(t, 1, len(fields.GetByPath("F5").Index))
			assert.Equal(t, "F5", fields.GetByPath("F5").Name)
			assert.Zero(t, fields.GetByPath("F6"))
		})
	})

	t.Run("NestedFieldsWithTags", func(t *testing.T) {
		m := NewMapper("db")

		type Details struct {
			Active bool `db:"active"`
		}

		type Asset struct {
			Title   string  `db:"title"`
			Details Details `db:"details"`
		}

		type Post struct {
			Author string `db:"author,required"`
			Asset  `db:"asset"`
		}

		post := Post{
			Author: "Joe",
			Asset: Asset{
				Title: "Hello",
				Details: Details{
					Active: true,
				},
			},
		}

		pv := reflect.ValueOf(post)

		assert.Equal(t, "Joe", m.FieldByName(pv, "author").Interface().(string))
		assert.Equal(t, "Hello", m.FieldByName(pv, "asset.title").Interface().(string))
		assert.Zero(t, m.FieldByName(pv, "title"))
		assert.Equal(t, true, m.FieldByName(pv, "asset.details.active").Interface().(bool))
	})

	t.Run("NestedFieldsWithAmbiguousTags", func(t *testing.T) {
		type Foo struct {
			A int `db:"a"`
		}

		type Bar struct {
			Foo     // `db:""` is implied for an embedded struct
			B   int `db:"b"`
		}

		type Baz struct {
			A   int `db:"a"`
			Bar     // `db:""` is implied for an embedded struct
		}

		m := NewMapper("db")

		z := Baz{A: 1, Bar: Bar{Foo: Foo{A: 3}, B: 2}}

		zv := reflect.ValueOf(z)
		fields := m.TypeMap(reflect.TypeOf(z))

		assert.Equal(t, 5, len(fields.Index))

		assert.Equal(t, 3, m.FieldByName(zv, "a").Interface().(int))
		assert.Equal(t, 2, m.FieldByName(zv, "b").Interface().(int))
	})

	t.Run("InlineStructs", func(t *testing.T) {
		m := NewMapperTagFunc("db", strings.ToLower, nil)

		type Employee struct {
			Name string
			ID   int
		}

		type Boss Employee

		type person struct {
			Employee `db:"employee"`
			Boss     `db:"boss"`
		}

		em := person{
			Employee: Employee{
				Name: "Joe",
				ID:   2,
			},
			Boss: Boss{
				Name: "Rick",
				ID:   1,
			},
		}
		ev := reflect.ValueOf(em)

		fields := m.TypeMap(reflect.TypeOf(em))

		assert.Equal(t, 6, len(fields.Index))

		assert.Equal(t, "Joe", m.FieldByName(ev, "employee.name").Interface().(string))
		assert.Equal(t, 2, m.FieldByName(ev, "employee.id").Interface().(int))
		assert.Equal(t, "Rick", m.FieldByName(ev, "boss.name").Interface().(string))
		assert.Equal(t, 1, m.FieldByName(ev, "boss.id").Interface().(int))
	})

	t.Run("FieldsWithTags", func(t *testing.T) {
		m := NewMapper("db")

		type Person struct {
			Name string `db:"name"`
		}

		type Place struct {
			Name string `db:"name"`
		}

		type Article struct {
			Title string `db:"title"`
		}

		type PP struct {
			Person  `db:"person,required"`
			Place   `db:",someflag"`
			Article `db:",required"`
		}

		pp := PP{
			Person: Person{
				Name: "Peter",
			},
			Place: Place{
				Name: "Toronto",
			},
			Article: Article{
				Title: "Best city ever",
			},
		}

		ppv := reflect.ValueOf(pp)
		fields := m.TypeMap(reflect.TypeOf(pp))

		v := m.FieldByName(ppv, "person.name")

		assert.Equal(t, "Peter", v.Interface().(string))
		assert.Equal(t, "Toronto", m.FieldByName(ppv, "name").Interface().(string))
		assert.Equal(t, "Best city ever", m.FieldByName(ppv, "title").Interface().(string))

		fi := fields.GetByPath("person")

		{
			_, ok := fi.Options["required"]
			assert.True(t, ok)

			assert.Zero(t, fi.Options["required"])
		}
		assert.True(t, fi.Embedded)

		assert.Len(t, fi.Index, 1)
		assert.Equal(t, 0, fi.Index[0])

		assert.Equal(t, "person.name", fields.GetByPath("person.name").Path)

		assert.Equal(t, "name", fields.GetByTraversal([]int{1, 0}).Path)

		fi = fields.GetByTraversal([]int{2})
		assert.NotNil(t, fi)

		_, ok := fi.Options["required"]
		assert.True(t, ok)

		trs := m.TraversalsByName(reflect.TypeOf(pp), []string{"person.name", "name", "title"})
		assert.Equal(t, [][]int{{0, 0}, {1, 0}, {2, 0}}, trs)
	})

	t.Run("PointerFields", func(t *testing.T) {
		m := NewMapperTagFunc("db", strings.ToLower, nil)

		type Asset struct {
			Title string
		}

		type Post struct {
			*Asset `db:"asset"`
			Author string
		}

		post := &Post{
			Author: "Joe",
			Asset: &Asset{
				Title: "Hiyo",
			},
		}

		pv := reflect.ValueOf(post)

		fields := m.TypeMap(reflect.TypeOf(post))
		assert.Equal(t, 3, len(fields.Index))

		assert.Equal(t, "Hiyo", m.FieldByName(pv, "asset.title").Interface().(string))
		assert.Equal(t, "Joe", m.FieldByName(pv, "author").Interface().(string))
	})

	t.Run("PointerFieldsWithNames", func(t *testing.T) {
		m := NewMapperTagFunc("db", strings.ToLower, nil)

		type User struct {
			Name string
		}

		type Asset struct {
			Title string

			Owner *User `db:"owner"`
		}

		type Post struct {
			Author string

			Asset1 *Asset `db:"asset1"`
			Asset2 *Asset `db:"asset2"`
		}

		post := &Post{
			Author: "Joe",
			Asset1: &Asset{
				Title: "Hiyo",
				Owner: &User{"Username"},
			},
		} // Asset2 is nil

		pv := reflect.ValueOf(post)

		fields := m.TypeMap(reflect.TypeOf(post))
		assert.Equal(t, 9, len(fields.Index))

		assert.Equal(t, "Hiyo", m.FieldByName(pv, "asset1.title").Interface().(string))
		assert.Equal(t, "Username", m.FieldByName(pv, "asset1.owner.name").Interface().(string))
		assert.Equal(t, post.Asset2.Title, m.FieldByName(pv, "asset2.title").Interface().(string))
		assert.Equal(t, post.Asset2.Owner.Name, m.FieldByName(pv, "asset2.owner.name").Interface().(string))
		assert.Equal(t, post.Author, m.FieldByName(pv, "author").Interface().(string))
	})

	t.Run("NameMapping", func(t *testing.T) {
		type Strategy struct {
			StrategyID   string `protobuf:"bytes,1,opt,name=strategy_id" json:"strategy_id,omitempty"`
			StrategyName string
		}

		mapperTagFunc := NewMapperTagFunc("json", strings.ToUpper, func(value string) string {
			if strings.Contains(value, ",") {
				return strings.Split(value, ",")[0]
			}
			return value
		})

		strategy := Strategy{"1", "Alpha"}
		m := mapperTagFunc.TypeMap(reflect.TypeOf(strategy))

		assert.NotNil(t, m.GetByPath("strategy_id"))  // explicitly tagged
		assert.NotNil(t, m.GetByPath("STRATEGYNAME")) // mapped by name
		assert.Nil(t, m.GetByPath("strategyname"))    // not mapped by tag
		assert.Nil(t, m.GetByPath("STRATEGYID"))      // not mapped by tag
	})

	t.Run("MapperFuncWithTags", func(t *testing.T) {
		type Person struct {
			ID           int
			Name         string
			WearsGlasses bool `db:"wears_glasses"`
		}

		m := NewMapperFunc("db", strings.ToLower)
		p := Person{1, "Jason", true}
		mapping := m.TypeMap(reflect.TypeOf(p))

		assert.NotNil(t, mapping.GetByPath("id"))
		assert.NotNil(t, mapping.GetByPath("name"))
		assert.NotNil(t, mapping.GetByPath("wears_glasses"))

		type SportsPerson struct {
			Weight int
			Age    int
			Person
		}
		s := SportsPerson{Weight: 100, Age: 30, Person: p}
		mapping = m.TypeMap(reflect.TypeOf(s))

		assert.NotNil(t, mapping.GetByPath("id"))
		assert.NotNil(t, mapping.GetByPath("name"))
		assert.NotNil(t, mapping.GetByPath("wears_glasses"))
		assert.NotNil(t, mapping.GetByPath("weight"))
		assert.NotNil(t, mapping.GetByPath("age"))

		type RugbyPlayer struct {
			Position   int
			IsIntense  bool `db:"is_intense"`
			IsAllBlack bool `db:"-"`
			SportsPerson
		}
		r := RugbyPlayer{12, true, false, s}
		mapping = m.TypeMap(reflect.TypeOf(r))

		assert.NotNil(t, mapping.GetByPath("id"))
		assert.NotNil(t, mapping.GetByPath("name"))
		assert.NotNil(t, mapping.GetByPath("wears_glasses"))
		assert.NotNil(t, mapping.GetByPath("weight"))
		assert.NotNil(t, mapping.GetByPath("age"))
		assert.NotNil(t, mapping.GetByPath("position"))

		assert.Nil(t, mapping.GetByPath("isallblack"))
	})
}

func BenchmarkFieldNameL1(b *testing.B) {
	e4 := E4{D: 1}
	for i := 0; i < b.N; i++ {
		v := reflect.ValueOf(e4)
		f := v.FieldByName("D")
		if f.Interface().(int) != 1 {
			b.Fatal("Wrong value.")
		}
	}
}

func BenchmarkFieldNameL4(b *testing.B) {
	e4 := E4{}
	e4.A = 1
	for i := 0; i < b.N; i++ {
		v := reflect.ValueOf(e4)
		f := v.FieldByName("A")
		if f.Interface().(int) != 1 {
			b.Fatal("Wrong value.")
		}
	}
}

func BenchmarkFieldPosL1(b *testing.B) {
	e4 := E4{D: 1}
	for i := 0; i < b.N; i++ {
		v := reflect.ValueOf(e4)
		f := v.Field(1)
		if f.Interface().(int) != 1 {
			b.Fatal("Wrong value.")
		}
	}
}

func BenchmarkFieldPosL4(b *testing.B) {
	e4 := E4{}
	e4.A = 1
	for i := 0; i < b.N; i++ {
		v := reflect.ValueOf(e4)
		f := v.Field(0)
		f = f.Field(0)
		f = f.Field(0)
		f = f.Field(0)
		if f.Interface().(int) != 1 {
			b.Fatal("Wrong value.")
		}
	}
}

func BenchmarkFieldByIndexL4(b *testing.B) {
	e4 := E4{}
	e4.A = 1
	idx := []int{0, 0, 0, 0}
	for i := 0; i < b.N; i++ {
		v := reflect.ValueOf(e4)
		f := FieldByIndexes(v, idx)
		if f.Interface().(int) != 1 {
			b.Fatal("Wrong value.")
		}
	}
}
