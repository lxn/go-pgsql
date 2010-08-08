// Copyright 2010 Alexander Neumann. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgsql

import (
	"encoding/binary"
	"math"
	"os"
	"strconv"
)

type fieldFormat int16

const (
	textFormat   fieldFormat = 0
	binaryFormat = 1
)

type field struct {
	name   string
	format fieldFormat
}

// ResultSet reads the results of a query, row by row, and provides methods to
// retrieve field values of the current row.
// Access is by 0-based field ordinal position.
type ResultSet struct {
	conn                  *Conn
	stmt                  *Statement
	hasCurrentRow         bool
	currentResultComplete bool
	allResultsComplete    bool
	rowsAffected          int64
	name2ord              map[string]int
	fields                []field
	values                [][]byte
}

func newResultSet(conn *Conn) *ResultSet {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("newResultSet"))
	}

	return &ResultSet{conn: conn}
}

func (res *ResultSet) initializeResult() {
	if res.conn.LogLevel >= LogDebug {
		defer res.conn.logExit(res.conn.logEnter("*ResultSet.initializeResult"))
	}

	// Just eat message length.
	res.conn.readInt32()

	fieldCount := res.conn.readInt16()

	res.fields = make([]field, fieldCount)
	res.values = make([][]byte, fieldCount)

	var ord int16
	for ord = 0; ord < fieldCount; ord++ {
		res.fields[ord].name = res.conn.readString()

		// Just eat table OID.
		res.conn.readInt32()

		// Just eat field OID.
		res.conn.readInt16()

		// Just eat field data type OID.
		res.conn.readInt32()

		// Just eat field size.
		res.conn.readInt16()

		// Just eat field type modifier.
		res.conn.readInt32()

		format := fieldFormat(res.conn.readInt16())
		switch format {
		case textFormat:
		case binaryFormat:
		default:
			panic("unsupported field format")
		}
		res.fields[ord].format = format
	}

	res.name2ord = make(map[string]int)

	for ord, field := range res.fields {
		res.name2ord[field.name] = ord
	}

	res.currentResultComplete = false
	res.hasCurrentRow = false
}

func (res *ResultSet) readRow() {
	if res.conn.LogLevel >= LogDebug {
		defer res.conn.logExit(res.conn.logEnter("*ResultSet.readRow"))
	}

	// Just eat message length.
	res.conn.readInt32()

	fieldCount := res.conn.readInt16()

	var ord int16
	for ord = 0; ord < fieldCount; ord++ {
		valLen := res.conn.readInt32()

		var val []byte

		if valLen == -1 {
			val = nil
		} else {
			val = make([]byte, valLen)
			res.conn.read(val)
		}

		res.values[ord] = val
	}

	res.hasCurrentRow = true
}

func (res *ResultSet) eatCurrentResultRows() (err os.Error) {
	var hasRow bool

	for {
		hasRow, err = res.FetchNext()
		if err != nil {
			// FIXME: How should we handle this?
			return
		}
		if !hasRow {
			return
		}
	}

	return
}

func (res *ResultSet) eatAllResultRows() (err os.Error) {
	var hasResult bool

	for {
		hasResult, err = res.NextResult()
		if err != nil {
			// FIXME: How should we handle this?
			return
		}
		if !hasResult {
			return
		}
	}

	return
}

// NextResult moves the ResultSet to the next result, if there is one.
// In this case true is returned, otherwise false.
// Statements support a single result only, use *Conn.Query if you need
// this functionality.
func (res *ResultSet) NextResult() (hasResult bool, err os.Error) {
	if res.conn.LogLevel >= LogDebug {
		defer res.conn.logExit(res.conn.logEnter("*ResultSet.NextResult"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = res.conn.logAndConvertPanic(x)
		}
	}()

	err = res.eatCurrentResultRows()
	if err != nil {
		panic(err)
	}

	if !res.allResultsComplete {
		res.conn.state.processBackendMessages(res.conn, res)
	}

	hasResult = !res.allResultsComplete

	return
}

// FetchNext reads the next row, if there is one.
// In this case true is returned, otherwise false.
func (res *ResultSet) FetchNext() (hasRow bool, err os.Error) {
	if res.conn.LogLevel >= LogDebug {
		defer res.conn.logExit(res.conn.logEnter("*ResultSet.FetchNext"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = res.conn.logAndConvertPanic(x)
		}
	}()

	if res.currentResultComplete {
		return
	}

	res.conn.state.processBackendMessages(res.conn, res)

	hasRow = !res.currentResultComplete

	return
}

// Close closes the ResultSet, so another query or command can be sent to
// the server over the same connection.
func (res *ResultSet) Close() (err os.Error) {
	if res.conn.LogLevel >= LogDebug {
		defer res.conn.logExit(res.conn.logEnter("*ResultSet.Close"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = res.conn.logAndConvertPanic(x)
		}
	}()

	if res.stmt != nil {
		defer res.conn.state.closePortal(res.stmt)
	}

	// TODO: Instead of eating all records, try to cancel the query processing.
	// (The required message has to be sent through another connection though.)
	err = res.eatAllResultRows()
	if err != nil {
		panic(err)
	}

	res.conn.state = readyState{}

	return
}

// IsNull returns if the value of the field with the specified ordinal is null.
func (res *ResultSet) IsNull(ord int) (isNull bool, err os.Error) {
	if res.conn.LogLevel >= LogVerbose {
		defer res.conn.logExit(res.conn.logEnter("*ResultSet.IsNull"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = res.conn.logAndConvertPanic(x)
		}
	}()

	// Since all field value retrieval methods call this method,
	// we only check for a valid current row here.
	if !res.hasCurrentRow {
		panic("invalid row")
	}

	isNull = res.values[ord] == nil
	return
}

// Ordinal returns the 0-based ordinal position of the field with the
// specified name, or -1 if the ResultSet has no field with such a name.
func (res *ResultSet) Ordinal(name string) int {
	if res.conn.LogLevel >= LogVerbose {
		defer res.conn.logExit(res.conn.logEnter("*ResultSet.Ordinal"))
	}

	ord, ok := res.name2ord[name]
	if !ok {
		return -1
	}

	return ord
}

// Bool returns the value of the field with the specified ordinal as bool.
func (res *ResultSet) Bool(ord int) (value, isNull bool, err os.Error) {
	if res.conn.LogLevel >= LogVerbose {
		defer res.conn.logExit(res.conn.logEnter("*ResultSet.Bool"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = res.conn.logAndConvertPanic(x)
		}
	}()

	isNull, err = res.IsNull(ord)
	if isNull || err != nil {
		return
	}

	val := res.values[ord]

	switch res.fields[ord].format {
	case textFormat:
		value = val[0] == 't'

	case binaryFormat:
		value = val[0] != 0
	}

	return
}

// Float32 returns the value of the field with the specified ordinal as float32.
func (res *ResultSet) Float32(ord int) (value float32, isNull bool, err os.Error) {
	if res.conn.LogLevel >= LogVerbose {
		defer res.conn.logExit(res.conn.logEnter("*ResultSet.Float32"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = res.conn.logAndConvertPanic(x)
		}
	}()

	isNull, err = res.IsNull(ord)
	if isNull || err != nil {
		return
	}

	val := res.values[ord]

	switch res.fields[ord].format {
	case textFormat:
		value, err = strconv.Atof32(string(val))
		if err != nil {
			panic(err)
		}

	case binaryFormat:
		value = math.Float32frombits(binary.BigEndian.Uint32(val))
	}

	return
}

// Float64 returns the value of the field with the specified ordinal as float64.
func (res *ResultSet) Float64(ord int) (value float64, isNull bool, err os.Error) {
	if res.conn.LogLevel >= LogVerbose {
		defer res.conn.logExit(res.conn.logEnter("*ResultSet.Float64"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = res.conn.logAndConvertPanic(x)
		}
	}()

	isNull, err = res.IsNull(ord)
	if isNull || err != nil {
		return
	}

	val := res.values[ord]

	switch res.fields[ord].format {
	case textFormat:
		value, err = strconv.Atof64(string(val))
		if err != nil {
			panic(err)
		}

	case binaryFormat:
		value = math.Float64frombits(binary.BigEndian.Uint64(val))
	}

	return
}

// Float returns the value of the field with the specified ordinal as float.
func (res *ResultSet) Float(ord int) (value float, isNull bool, err os.Error) {
	if res.conn.LogLevel >= LogVerbose {
		defer res.conn.logExit(res.conn.logEnter("*ResultSet.Float"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = res.conn.logAndConvertPanic(x)
		}
	}()

	val, isNull, err := res.Float32(ord)
	value = float(val)
	return
}

// Int16 returns the value of the field with the specified ordinal as int16.
func (res *ResultSet) Int16(ord int) (value int16, isNull bool, err os.Error) {
	if res.conn.LogLevel >= LogVerbose {
		defer res.conn.logExit(res.conn.logEnter("*ResultSet.Int16"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = res.conn.logAndConvertPanic(x)
		}
	}()

	isNull, err = res.IsNull(ord)
	if isNull || err != nil {
		return
	}

	val := res.values[ord]

	switch res.fields[ord].format {
	case textFormat:
		x, err := strconv.Atoi(string(val))
		if err != nil {
			panic(err)
		}
		value = int16(x)

	case binaryFormat:
		value = int16(binary.BigEndian.Uint16(val))
	}

	return
}

// Int32 returns the value of the field with the specified ordinal as int32.
func (res *ResultSet) Int32(ord int) (value int32, isNull bool, err os.Error) {
	if res.conn.LogLevel >= LogVerbose {
		defer res.conn.logExit(res.conn.logEnter("*ResultSet.Int32"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = res.conn.logAndConvertPanic(x)
		}
	}()

	isNull, err = res.IsNull(ord)
	if isNull || err != nil {
		return
	}

	val := res.values[ord]

	switch res.fields[ord].format {
	case textFormat:
		x, err := strconv.Atoi(string(val))
		if err != nil {
			panic(err)
		}
		value = int32(x)

	case binaryFormat:
		value = int32(binary.BigEndian.Uint32(val))
	}

	return
}

// Int64 returns the value of the field with the specified ordinal as int64.
func (res *ResultSet) Int64(ord int) (value int64, isNull bool, err os.Error) {
	if res.conn.LogLevel >= LogVerbose {
		defer res.conn.logExit(res.conn.logEnter("*ResultSet.Int64"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = res.conn.logAndConvertPanic(x)
		}
	}()

	isNull, err = res.IsNull(ord)
	if isNull || err != nil {
		return
	}

	val := res.values[ord]

	switch res.fields[ord].format {
	case textFormat:
		x, err := strconv.Atoi(string(val))
		if err != nil {
			panic(err)
		}
		value = int64(x)

	case binaryFormat:
		value = int64(binary.BigEndian.Uint64(val))
	}

	return
}

// Int returns the value of the field with the specified ordinal as int.
func (res *ResultSet) Int(ord int) (value int, isNull bool, err os.Error) {
	if res.conn.LogLevel >= LogVerbose {
		defer res.conn.logExit(res.conn.logEnter("*ResultSet.Int"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = res.conn.logAndConvertPanic(x)
		}
	}()

	val, isNull, err := res.Int32(ord)
	value = int(val)
	return
}

// String returns the value of the field with the specified ordinal as string.
func (res *ResultSet) String(ord int) (value string, isNull bool, err os.Error) {
	if res.conn.LogLevel >= LogVerbose {
		defer res.conn.logExit(res.conn.logEnter("*ResultSet.String"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = res.conn.logAndConvertPanic(x)
		}
	}()

	isNull, err = res.IsNull(ord)
	if isNull || err != nil {
		return
	}

	value = string(res.values[ord])

	return
}
