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

package bond

type Model interface {
	HasStore
}

type HasCollectionName interface {
	CollectionName() string
}

type HasStore interface {
	Store(sess Session) Store
}

type HasSave interface {
	Save(sess Session) error
}

type HasValidate interface {
	Validate() error
}

type HasBeforeCreate interface {
	BeforeCreate(Session) error
}

type HasAfterCreate interface {
	AfterCreate(Session) error
}

type HasBeforeUpdate interface {
	BeforeUpdate(Session) error
}

type HasAfterUpdate interface {
	AfterUpdate(Session) error
}

type HasBeforeDelete interface {
	BeforeDelete(Session) error
}

type HasAfterDelete interface {
	AfterDelete(Session) error
}

type StoreFunc func(sess Session) Store
