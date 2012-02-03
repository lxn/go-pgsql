// Copyright 2010 Alexander Neumann. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgsql

import (
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"time"
)

// Parameter is used to set the value of a parameter in a Statement.
type Parameter struct {
	name           string
	stmt           *Statement
	typ            Type
	customTypeName string
	value          interface{}
}

// NewParameter returns a new Parameter with the specified name and type.
func NewParameter(name string, typ Type) *Parameter {
	return &Parameter{name: name, typ: typ}
}

// NewCustomTypeParameter returns a new Parameter with the specified name and
// custom data type.
//
// The value of customTypeName will be used to insert a type cast into the
// command text for each occurrence of the parameter.
//
// This constructor can be used for enum type parameters. In that case the value
// provided to SetValue is expected to be a string.
func NewCustomTypeParameter(name, customTypeName string) *Parameter {
	return &Parameter{name: name, customTypeName: customTypeName}
}

// CustomTypeName returns the custom type name of the Parameter.
func (p *Parameter) CustomTypeName() string {
	return p.customTypeName
}

// Name returns the name of the Parameter.
func (p *Parameter) Name() string {
	return p.name
}

// Statement returns the *Statement this Parameter is associated with.
func (p *Parameter) Statement() *Statement {
	return p.stmt
}

// Type returns the PostgreSQL data type of the Parameter.
func (p *Parameter) Type() Type {
	return p.typ
}

// Value returns the current value of the Parameter.
func (p *Parameter) Value() interface{} {
	return p.value
}

func (p *Parameter) panicInvalidValue(v interface{}) {
	panic(errors.New(fmt.Sprintf("Parameter %s: Invalid value for PostgreSQL type %s: '%v' (Go type: %T)",
		p.name, p.typ, v, v)))
}

func isNilPtr(v interface{}) bool {
	ptr := reflect.ValueOf(v)

	return ptr.Kind() == reflect.Ptr &&
		ptr.IsNil()
}

// SetValue sets the current value of the Parameter.
func (p *Parameter) SetValue(v interface{}) (err error) {
	if p.stmt != nil && p.stmt.conn.LogLevel >= LogVerbose {
		defer p.stmt.conn.logExit(p.stmt.conn.logEnter("*Parameter.SetValue"))
	}

	defer func() {
		if x := recover(); x != nil {
			if p.stmt == nil {
				switch ex := x.(type) {
				case error:
					err = ex

				case string:
					err = errors.New(ex)

				default:
					err = errors.New("pgsql.*Parameter.SetValue: D'oh!")
				}
			} else {
				err = p.stmt.conn.logAndConvertPanic(x)
			}
		}
	}()

	if v == nil {
		p.value = nil
		return
	}

	switch p.typ {
	case Bigint:
		switch val := v.(type) {
		case byte:
			p.value = int64(val)

		case int:
			p.value = int64(val)

		case int16:
			p.value = int64(val)

		case int32:
			p.value = int64(val)

		case uint:
			p.value = int64(val)

		case uint16:
			p.value = int64(val)

		case uint32:
			p.value = int64(val)

		case uint64:
			p.value = int64(val)

		case int64:
			p.value = val

		default:
			p.panicInvalidValue(v)
		}

	case Boolean:
		val, ok := v.(bool)
		if !ok {
			p.panicInvalidValue(v)
		}
		p.value = val

	case Char, Text, Varchar:
		val, ok := v.(string)
		if !ok {
			p.panicInvalidValue(v)
		}
		p.value = val

	case Custom:
		p.value = v

	case Date, Time, TimeTZ, Timestamp, TimestampTZ:
		switch val := v.(type) {
		case int64:
			p.value = val

		case time.Time:
			if isNilPtr(v) {
				p.value = nil
				return
			}

			t := &time.Time{}
			*t = val
			p.value = t

		case uint64:
			p.value = val

		default:
			p.panicInvalidValue(v)
		}

	case Double:
		switch val := v.(type) {
		case float32:
			p.value = float64(val)

		case float64:
			p.value = val

		default:
			p.panicInvalidValue(v)
		}

	case Integer:
		switch val := v.(type) {
		case byte:
			p.value = int32(val)

		case int:
			p.value = int32(val)

		case int16:
			p.value = int32(val)

		case uint:
			p.value = int32(val)

		case uint16:
			p.value = int32(val)

		case uint32:
			p.value = int32(val)

		case int32:
			p.value = val

		default:
			p.panicInvalidValue(v)
		}

	case Numeric:
		val, ok := v.(*big.Rat)
		if !ok {
			p.panicInvalidValue(v)
		}

		if isNilPtr(v) {
			p.value = nil
			return
		}

		p.value = val

	case Real:
		switch val := v.(type) {
		case float32:
			p.value = val

		default:
			p.panicInvalidValue(v)
		}

	case Smallint:
		switch val := v.(type) {
		case byte:
			p.value = int16(val)

		case uint16:
			p.value = int16(val)

		case int16:
			p.value = val

		default:
			p.panicInvalidValue(v)
		}
	}

	return
}
