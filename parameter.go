// Copyright 2010 Alexander Neumann. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgsql

import (
	"os"
)

// Parameter is used to set the value of a parameter in a Statement.
type Parameter struct {
	name  string
	stmt  *Statement
	typ   Type
	value interface{}
}

// NewParameter returns a new Parameter with the specified properties.
func NewParameter(name string, typ Type) *Parameter {
	return &Parameter{name: name, typ: typ}
}

// Name returns the name of the Parameter.
func (p *Parameter) Name() string {
	return p.name
}

// Type returns the PostgreSQL data type of the Parameter.
func (p *Parameter) Type() Type {
	return p.typ
}

// Value returns the current value of the Parameter.
func (p *Parameter) Value() interface{} {
	return p.value
}

// SetValue sets the current value of the Parameter.
func (p *Parameter) SetValue(v interface{}) (err os.Error) {
	defer func() {
		if x := recover(); x != nil {
			if p.stmt == nil {
				err = x.(os.Error)
			} else {
				err = p.stmt.conn.logAndConvertPanic(x)
			}
		}
	}()

	if p.stmt != nil && p.stmt.conn.LogLevel >= LogVerbose {
		defer p.stmt.conn.logExit(p.stmt.conn.logEnter("*Parameter.SetValue"))
	}

	var ok bool
	switch p.typ {
	case Boolean:
		p.value = v.(bool)

	case Char, Text, Varchar:
		p.value = v.(string)

	case Real:
		p.value = v.(float32)

	case Double:
		p.value = v.(float64)

	case Smallint:
		p.value = v.(int16)

	case Integer:
		p.value, ok = v.(int32)
		if !ok {
			p.value = v.(int)
		}

	case Bigint:
		p.value = v.(int64)
	}

	return
}
