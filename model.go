// Copyright (c) 2012-present The upper.io/db authors. All rights reserved.
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

package db

// Model is the equivalence between concrete database schemas and Go values.
type Model interface {
	Collection(sess Session) Collection
}

// HasSave is an interface that defines an (optional) Save function for models
// that is called when persisting an item (creating or updating). If Save is
// not defined, upper/db will attempt to either create or update the item based
// on whether the values for item's primary key are defined.
type HasSave interface {
	Save(sess Session) error
}

// Validator is an interface that defined an (optional) Validate function for
// models that is called before persisting an item (creating or updating). If
// Validate returns an error the current operation is rolled back.
type Validator interface {
	Validate() error
}

// BeforeCreateHook is an interface that defines an BeforeCreate function for
// models that is called before creating an item. If BeforeCreate returns an
// error the create process is rolled back.
type BeforeCreateHook interface {
	BeforeCreate(Session) error
}

// AfterCreateHook is an interface that defines an AfterCreate function for
// models that is called after creating an item. If AfterCreate returns an
// error the create process is rolled back.
type AfterCreateHook interface {
	AfterCreate(Session) error
}

// BeforeUpdateHook is an interface that defines a BeforeUpdate function for
// models that is called before updating an item. If BeforeUpdate returns an
// error the update process is rolled back.
type BeforeUpdateHook interface {
	BeforeUpdate(Session) error
}

// AfterUpdateHook is an interface that defines an AfterUpdate function for
// models that is called after updating an item. If AfterUpdate returns an
// error the update process is rolled back.
type AfterUpdateHook interface {
	AfterUpdate(Session) error
}

// BeforeDeleteHook is an interface that defines a BeforeDelete function for
// models that is called before removing an item. If BeforeDelete returns an
// error the delete process is rolled back.
type BeforeDeleteHook interface {
	BeforeDelete(Session) error
}

// AfterDeleteHook is an interface that defines a AfterDelete function for
// models that is called after removing an item. If AfterDelete returns an
// error the delete process is rolled back.
type AfterDeleteHook interface {
	AfterDelete(Session) error
}
