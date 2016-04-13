// Copyright (c) 2012-2015 The upper.io/db authors. All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package sqlite // import "upper.io/db/sqlite"

import (
	"upper.io/db/builder/cache"
	"upper.io/db"
	"upper.io/db/util/sqlgen"
	"upper.io/db/util/sqlutil"
)

// Adapter is the public name of the adapter.
const Adapter = `sqlite`

var template *sqlutil.TemplateWithUtils

func init() {
	template = sqlutil.NewTemplateWithUtils(&sqlgen.Template{
		ColumnSeparator:     adapterColumnSeparator,
		IdentifierSeparator: adapterIdentifierSeparator,
		IdentifierQuote:     adapterIdentifierQuote,
		ValueSeparator:      adapterValueSeparator,
		ValueQuote:          adapterValueQuote,
		AndKeyword:          adapterAndKeyword,
		OrKeyword:           adapterOrKeyword,
		NotKeyword:          adapterNotKeyword,
		DescKeyword:         adapterDescKeyword,
		AscKeyword:          adapterAscKeyword,
		DefaultOperator:     adapterDefaultOperator,
		AssignmentOperator:  adapterAssignmentOperator,
		ClauseGroup:         adapterClauseGroup,
		ClauseOperator:      adapterClauseOperator,
		ColumnValue:         adapterColumnValue,
		TableAliasLayout:    adapterTableAliasLayout,
		ColumnAliasLayout:   adapterColumnAliasLayout,
		SortByColumnLayout:  adapterSortByColumnLayout,
		WhereLayout:         adapterWhereLayout,
		OrderByLayout:       adapterOrderByLayout,
		InsertLayout:        adapterInsertLayout,
		SelectLayout:        adapterSelectLayout,
		UpdateLayout:        adapterUpdateLayout,
		DeleteLayout:        adapterDeleteLayout,
		TruncateLayout:      adapterTruncateLayout,
		DropDatabaseLayout:  adapterDropDatabaseLayout,
		DropTableLayout:     adapterDropTableLayout,
		CountLayout:         adapterSelectCountLayout,
		GroupByLayout:       adapterGroupByLayout,
		Cache:               cache.NewCache(),
	})

	db.Register(Adapter, &database{})
}
