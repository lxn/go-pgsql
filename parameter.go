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

	switch p.typ {
	case Bigint:
		switch val := v.(type) {
		case int:
			p.value = int64(val)

		case int16:
			p.value = int64(val)

		case int32:
			p.value = int64(val)

		case int64:
			p.value = val

		default:
			panic("invalid value for Bigint")
		}

	case Boolean:
		val, ok := v.(bool)
		if !ok {
			panic("invalid value for Boolean")
		}
		p.value = val

	case Char, Text, Varchar:
		val, ok := v.(string)
		if !ok {
			panic("invalid value for Char, Text or Varchar")
		}
		p.value = val

	case Double:
		switch val := v.(type) {
		case float:
			p.value = float64(val)

		case float32:
			p.value = float64(val)

		case float64:
			p.value = val

		default:
			panic("invalid value for Double")
		}

	case Integer:
		switch val := v.(type) {
		case int:
			p.value = int32(val)

		case int16:
			p.value = int32(val)

		case int32:
			p.value = val

		default:
			panic("invalid value for Integer")
		}

	case Real:
		switch val := v.(type) {
		case float:
			p.value = float32(val)

		case float32:
			p.value = val

		default:
			panic("invalid value for Real")
		}

	case Smallint:
		val, ok := v.(int16)
		if !ok {
			panic("invalid value for Smallint")
		}
		p.value = val
	}

	return
}
