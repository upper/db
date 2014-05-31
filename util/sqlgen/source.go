package sqlgen

import (
	"fmt"
)

type Source struct {
	v string
}

func (self Source) String() string {
	return mustParse(sqlEscape, Raw{fmt.Sprintf(`%v`, self.v)})
}
