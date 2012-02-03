// Copyright 2010 Alexander Neumann. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgsql

import (
	"fmt"
)

// Error contains detailed error information received from a PostgreSQL backend.
//
// Many go-pgsql functions return an os.Error value. In case of a backend error,
// a type assertion as shown below gives you a *pgsql.Error with all details:
//
//	...
//	_, err := rs.FetchNext()
//	if err != nil {
//		if pgerr, ok := err.(*pgsql.Error); ok {
//			// Do something with pgerr
//		}
//	}
//	...
type Error struct  {
	severity         string
	code             string
	message          string
	detail           string
	hint             string
	position         string
	internalPosition string
	internalQuery    string
	where            string
	file             string
	line             string
	routine          string
}

func (e *Error) Severity() string {
	return e.severity
}

func (e *Error) Code() string {
	return e.code
}

func (e *Error) Message() string {
	return e.message
}

func (e *Error) Detail() string {
	return e.detail
}

func (e *Error) Hint() string {
	return e.hint
}

func (e *Error) Position() string {
	return e.position
}

func (e *Error) InternalPosition() string {
	return e.internalPosition
}

func (e *Error) InternalQuery() string {
	return e.internalQuery
}

func (e *Error) Where() string {
	return e.where
}

func (e *Error) File() string {
	return e.file
}

func (e *Error) Line() string {
	return e.line
}

func (e *Error) Routine() string {
	return e.routine
}

func (e *Error) Error() string {
	return fmt.Sprintf(
		`Severity: %s
		Code: %s
		Message: %s
		Detail: %s
		Hint: %s
		Position: %s
		Internal Position: %s
		Internal Query: %s
		Where: %s
		File: %s
		Line: %s
		Routine: %s`,
		e.severity, e.code, e.message, e.detail, e.hint, e.position,
		e.internalPosition, e.internalQuery, e.where, e.file, e.line, e.routine)
}
