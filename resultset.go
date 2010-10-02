// Copyright 2010 Alexander Neumann. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgsql

import (
	"big"
	"encoding/binary"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

type fieldFormat int16

const (
	textFormat   fieldFormat = 0
	binaryFormat fieldFormat = 1
)

type field struct {
	name    string
	format  fieldFormat
	typeOID int32
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

func (rs *ResultSet) initializeResult() {
	if rs.conn.LogLevel >= LogDebug {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.initializeResult"))
	}

	rs.conn.readRowDescription(rs)

	rs.name2ord = make(map[string]int)

	for ord, field := range rs.fields {
		rs.name2ord[field.name] = ord
	}

	rs.currentResultComplete = false
	rs.hasCurrentRow = false
}

func (rs *ResultSet) readRow() {
	if rs.conn.LogLevel >= LogDebug {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.readRow"))
	}

	rs.conn.readDataRow(rs)

	rs.hasCurrentRow = true
}

func (rs *ResultSet) eatCurrentResultRows() (err os.Error) {
	var hasRow bool

	for {
		hasRow, err = rs.FetchNext()
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

func (rs *ResultSet) eatAllResultRows() (err os.Error) {
	var hasResult bool

	for {
		hasResult, err = rs.NextResult()
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

// Conn returns the *Conn this ResultSet is associated with.
func (rs *ResultSet) Conn() *Conn {
	return rs.conn
}

// Statement returns the *Statement this ResultSet is associated with.
func (rs *ResultSet) Statement() *Statement {
	return rs.stmt
}

// NextResult moves the ResultSet to the next result, if there is one.
// In this case true is returned, otherwise false.
// Statements support a single result only, use *Conn.Query if you need
// this functionality.
func (rs *ResultSet) NextResult() (hasResult bool, err os.Error) {
	if rs.conn.LogLevel >= LogDebug {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.NextResult"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = rs.conn.logAndConvertPanic(x)
		}
	}()

	err = rs.eatCurrentResultRows()
	if err != nil {
		panic(err)
	}

	if !rs.allResultsComplete {
		rs.conn.readBackendMessages(rs)
	}

	hasResult = !rs.allResultsComplete

	return
}

// FetchNext reads the next row, if there is one.
// In this case true is returned, otherwise false.
func (rs *ResultSet) FetchNext() (hasRow bool, err os.Error) {
	if rs.conn.LogLevel >= LogDebug {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.FetchNext"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = rs.conn.logAndConvertPanic(x)
		}
	}()

	if rs.currentResultComplete {
		return
	}

	rs.conn.readBackendMessages(rs)

	hasRow = !rs.currentResultComplete

	return
}

// Close closes the ResultSet, so another query or command can be sent to
// the server over the same connection.
func (rs *ResultSet) Close() (err os.Error) {
	if rs.conn.LogLevel >= LogDebug {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.Close"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = rs.conn.logAndConvertPanic(x)
		}
	}()

	if rs.stmt != nil {
		defer rs.conn.writeClose('P', rs.stmt.portalName)
	}

	// TODO: Instead of eating all records, try to cancel the query processing.
	// (The required message has to be sent through another connection though.)
	err = rs.eatAllResultRows()
	if err != nil {
		panic(err)
	}

	rs.conn.state = readyState{}

	return
}

// IsNull returns if the value of the field with the specified ordinal is null.
func (rs *ResultSet) IsNull(ord int) (isNull bool, err os.Error) {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.IsNull"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = rs.conn.logAndConvertPanic(x)
		}
	}()

	// Since all field value retrieval methods call this method,
	// we only check for a valid current row here.
	if !rs.hasCurrentRow {
		panic("invalid row")
	}

	isNull = rs.values[ord] == nil
	return
}

// FieldCount returns the number of fields in the current result of the ResultSet.
func (rs *ResultSet) FieldCount() int {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.FieldCount"))
	}

	return len(rs.fields)
}

// Name returns the name of the field with the specified ordinal.
func (rs *ResultSet) Name(ord int) (name string, err os.Error) {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.Name"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = rs.conn.logAndConvertPanic(x)
		}
	}()

	return rs.fields[ord].name, nil
}

// Type returns the PostgreSQL type of the field with the specified ordinal.
func (rs *ResultSet) Type(ord int) (typ Type, err os.Error) {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.Type"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = rs.conn.logAndConvertPanic(x)
		}
	}()

	switch typ := rs.fields[ord].typeOID; typ {
	case _BOOLOID, _CHAROID, _DATEOID, _FLOAT4OID, _FLOAT8OID, _INT2OID,
		_INT4OID, _INT8OID, _TEXTOID, _TIMEOID, _TIMETZOID, _TIMESTAMPOID,
		_TIMESTAMPTZOID, _VARCHAROID:
		return Type(typ), nil
	}

	return Custom, nil
}

// Ordinal returns the 0-based ordinal position of the field with the
// specified name, or -1 if the ResultSet has no field with such a name.
func (rs *ResultSet) Ordinal(name string) int {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.Ordinal"))
	}

	ord, ok := rs.name2ord[name]
	if !ok {
		return -1
	}

	return ord
}

// Bool returns the value of the field with the specified ordinal as bool.
func (rs *ResultSet) Bool(ord int) (value, isNull bool, err os.Error) {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.Bool"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = rs.conn.logAndConvertPanic(x)
		}
	}()

	isNull, err = rs.IsNull(ord)
	if isNull || err != nil {
		return
	}

	val := rs.values[ord]

	switch rs.fields[ord].format {
	case textFormat:
		value = val[0] == 't'

	case binaryFormat:
		value = val[0] != 0
	}

	return
}

// Float32 returns the value of the field with the specified ordinal as float32.
func (rs *ResultSet) Float32(ord int) (value float32, isNull bool, err os.Error) {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.Float32"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = rs.conn.logAndConvertPanic(x)
		}
	}()

	isNull, err = rs.IsNull(ord)
	if isNull || err != nil {
		return
	}

	val := rs.values[ord]

	switch rs.fields[ord].format {
	case textFormat:
		// strconv.Atof32 does not handle NaN
		if string(val) == "NaN" {
			value = float32(math.NaN())
		} else {
			value, err = strconv.Atof32(string(val))
			if err != nil {
				panic(err)
			}
		}

	case binaryFormat:
		value = math.Float32frombits(binary.BigEndian.Uint32(val))
	}

	return
}

// Float64 returns the value of the field with the specified ordinal as float64.
func (rs *ResultSet) Float64(ord int) (value float64, isNull bool, err os.Error) {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.Float64"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = rs.conn.logAndConvertPanic(x)
		}
	}()

	isNull, err = rs.IsNull(ord)
	if isNull || err != nil {
		return
	}

	val := rs.values[ord]

	switch rs.fields[ord].format {
	case textFormat:
		// strconv.Atof64 does not handle NaN
		if string(val) == "NaN" {
			value = math.NaN()
		} else {
			value, err = strconv.Atof64(string(val))
			if err != nil {
				panic(err)
			}
		}

	case binaryFormat:
		value = math.Float64frombits(binary.BigEndian.Uint64(val))
	}

	return
}

// Float returns the value of the field with the specified ordinal as float.
func (rs *ResultSet) Float(ord int) (value float, isNull bool, err os.Error) {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.Float"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = rs.conn.logAndConvertPanic(x)
		}
	}()

	val, isNull, err := rs.Float32(ord)
	value = float(val)
	return
}

// Int16 returns the value of the field with the specified ordinal as int16.
func (rs *ResultSet) Int16(ord int) (value int16, isNull bool, err os.Error) {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.Int16"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = rs.conn.logAndConvertPanic(x)
		}
	}()

	isNull, err = rs.IsNull(ord)
	if isNull || err != nil {
		return
	}

	val := rs.values[ord]

	switch rs.fields[ord].format {
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
func (rs *ResultSet) Int32(ord int) (value int32, isNull bool, err os.Error) {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.Int32"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = rs.conn.logAndConvertPanic(x)
		}
	}()

	isNull, err = rs.IsNull(ord)
	if isNull || err != nil {
		return
	}

	val := rs.values[ord]

	switch rs.fields[ord].format {
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
func (rs *ResultSet) Int64(ord int) (value int64, isNull bool, err os.Error) {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.Int64"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = rs.conn.logAndConvertPanic(x)
		}
	}()

	isNull, err = rs.IsNull(ord)
	if isNull || err != nil {
		return
	}

	val := rs.values[ord]

	switch rs.fields[ord].format {
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
func (rs *ResultSet) Int(ord int) (value int, isNull bool, err os.Error) {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.Int"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = rs.conn.logAndConvertPanic(x)
		}
	}()

	val, isNull, err := rs.Int32(ord)
	value = int(val)
	return
}

// Rat returns the value of the field with the specified ordinal as *big.Rat.
func (rs *ResultSet) Rat(ord int) (value *big.Rat, isNull bool, err os.Error) {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.Rat"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = rs.conn.logAndConvertPanic(x)
		}
	}()

	isNull, err = rs.IsNull(ord)
	if isNull || err != nil {
		return
	}

	val := rs.values[ord]

	switch rs.fields[ord].format {
	case textFormat:
		x := big.NewRat(1, 1)
		if _, ok := x.SetString(string(val)); !ok {
			panic("*big.Rat.SetString failed")
		}
		value = x

	case binaryFormat:
		panic("not implemented")
	}

	return
}

// String returns the value of the field with the specified ordinal as string.
func (rs *ResultSet) String(ord int) (value string, isNull bool, err os.Error) {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.String"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = rs.conn.logAndConvertPanic(x)
		}
	}()

	isNull, err = rs.IsNull(ord)
	if isNull || err != nil {
		return
	}

	value = string(rs.values[ord])

	return
}

// Time returns the value of the field with the specified ordinal as *time.Time.
func (rs *ResultSet) Time(ord int) (value *time.Time, isNull bool, err os.Error) {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.Time"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = rs.conn.logAndConvertPanic(x)
		}
	}()

	// We need to convert the parsed *time.Time to seconds and back,
	// because otherwise the Weekday field will always equal 0 (Sunday).
	// See http://code.google.com/p/go/issues/detail?id=1025
	seconds, isNull, err := rs.TimeSeconds(ord)
	if err != nil {
		return
	}
	if isNull {
		return
	}

	value = time.SecondsToUTC(seconds)

	return
}

// TimeSeconds returns the value of the field with the specified ordinal as int64.
func (rs *ResultSet) TimeSeconds(ord int) (value int64, isNull bool, err os.Error) {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.TimeSeconds"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = rs.conn.logAndConvertPanic(x)
		}
	}()

	isNull, err = rs.IsNull(ord)
	if isNull || err != nil {
		return
	}

	val := rs.values[ord]

	var t *time.Time

	switch rs.fields[ord].format {
	case textFormat:
		var format string
		switch rs.fields[ord].typeOID {
		case _DATEOID:
			format = rs.conn.dateFormat

		case _TIMEOID, _TIMETZOID:
			format = rs.conn.timeFormat

		case _TIMESTAMPOID, _TIMESTAMPTZOID:
			format = rs.conn.timestampFormat
		}

		switch rs.fields[ord].typeOID {
		case _TIMETZOID:
			format += "-07"

		case _TIMESTAMPTZOID:
			format += rs.conn.timestampTimezoneFormat
		}

		s := string(val)

		if rs.fields[ord].typeOID != _DATEOID {
			// The resolution of time.Time is seconds, so we will have to drop
			// fractions, if present.
			lastSemicolon := strings.LastIndex(s, ":")
			lastDot := strings.LastIndex(s, ".")
			if lastSemicolon < lastDot {
				// There are fractions
				plusOrMinus := strings.IndexAny(s[lastDot:], "+-")
				if -1 < plusOrMinus {
					// There is a time zone
					s = s[0:lastDot] + s[lastDot+plusOrMinus:]
				} else {
					s = s[0:lastDot]
				}
			}
		}

		t, err = time.Parse(format, s)
		if err != nil {
			panic(err)
		}

	case binaryFormat:
		panic("not implemented")
	}

	value = t.Seconds()

	return
}

// Uint returns the value of the field with the specified ordinal as uint.
func (rs *ResultSet) Uint(ord int) (value uint, isNull bool, err os.Error) {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.Uint"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = rs.conn.logAndConvertPanic(x)
		}
	}()

	val, isNull, err := rs.Int32(ord)
	value = uint(val)
	return
}

// Uint16 returns the value of the field with the specified ordinal as uint16.
func (rs *ResultSet) Uint16(ord int) (value uint16, isNull bool, err os.Error) {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.Uint16"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = rs.conn.logAndConvertPanic(x)
		}
	}()

	val, isNull, err := rs.Int16(ord)
	value = uint16(val)
	return
}

// Uint32 returns the value of the field with the specified ordinal as uint32.
func (rs *ResultSet) Uint32(ord int) (value uint32, isNull bool, err os.Error) {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.Uint32"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = rs.conn.logAndConvertPanic(x)
		}
	}()

	val, isNull, err := rs.Int32(ord)
	value = uint32(val)
	return
}

// Uint64 returns the value of the field with the specified ordinal as uint64.
func (rs *ResultSet) Uint64(ord int) (value uint64, isNull bool, err os.Error) {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.Uint64"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = rs.conn.logAndConvertPanic(x)
		}
	}()

	val, isNull, err := rs.Int64(ord)
	value = uint64(val)
	return
}

// Any returns the value of the field with the specified ordinal as interface{}.
//
// Types are mapped as follows:
//
// PostgreSQL	Go
//
// Bigint		int64
//
// Boolean		bool
//
// Char			string
//
// Date			int64
//
// Double		float64
//
// Integer		int
//
// Numeric		*big.Rat
//
// Real			float
//
// Smallint		int16
//
// Text			string
//
// Time			int64
//
// TimeTZ		int64
//
// Timestamp	int64
//
// TimestampTZ	int64
//
// Varchar		string
func (rs *ResultSet) Any(ord int) (value interface{}, isNull bool, err os.Error) {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.Any"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = rs.conn.logAndConvertPanic(x)
		}
	}()

	if rs.values[ord] == nil {
		isNull = true
		return
	}

	switch rs.fields[ord].typeOID {
	case _BOOLOID:
		return rs.Bool(ord)

	case _CHAROID, _VARCHAROID, _TEXTOID:
		return rs.String(ord)

	case _DATEOID, _TIMEOID, _TIMETZOID, _TIMESTAMPOID, _TIMESTAMPTZOID:
		return rs.TimeSeconds(ord)

	case _FLOAT4OID:
		return rs.Float(ord)

	case _FLOAT8OID:
		return rs.Float64(ord)

	case _INT2OID:
		return rs.Int16(ord)

	case _INT4OID:
		return rs.Int(ord)

	case _INT8OID:
		return rs.Int64(ord)

	case _NUMERICOID:
		return rs.Rat(ord)

	default:
		panic("unexpected field data type")
	}

	return
}

// Scan scans the fields of the current row in the ResultSet, trying
// to store field values into the specified arguments. The arguments
// must be of pointer types.
func (rs *ResultSet) Scan(args ...interface{}) (err os.Error) {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.Scan"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = rs.conn.logAndConvertPanic(x)
		}
	}()

	if len(args) != len(rs.fields) {
		panic("wrong argument count")
	}

	for i, arg := range args {
		switch a := arg.(type) {
		case *bool:
			*a, _, err = rs.Bool(i)

		case *float:
			*a, _, err = rs.Float(i)

		case *float32:
			*a, _, err = rs.Float32(i)

		case *float64:
			*a, _, err = rs.Float64(i)

		case *int:
			*a, _, err = rs.Int(i)

		case *int16:
			*a, _, err = rs.Int16(i)

		case *int32:
			*a, _, err = rs.Int32(i)

		case *int64:
			switch rs.fields[i].typeOID {
			case _DATEOID, _TIMEOID, _TIMETZOID, _TIMESTAMPOID, _TIMESTAMPTZOID:
				*a, _, err = rs.TimeSeconds(i)

			default:
				*a, _, err = rs.Int64(i)
			}

		case *interface{}:
			*a, _, err = rs.Any(i)

		case **big.Rat:
			var r *big.Rat
			r, _, err = rs.Rat(i)
			if err == nil {
				*a = r
			}

		case *string:
			*a, _, err = rs.String(i)

		case **time.Time:
			var t *time.Time
			t, _, err = rs.Time(i)
			if err == nil {
				*a = t
			}

		case *uint:
			*a, _, err = rs.Uint(i)

		case *uint16:
			*a, _, err = rs.Uint16(i)

		case *uint32:
			*a, _, err = rs.Uint32(i)

		case *uint64:
			switch rs.fields[i].typeOID {
			case _DATEOID, _TIMEOID, _TIMETZOID, _TIMESTAMPOID, _TIMESTAMPTZOID:
				var seconds int64
				seconds, _, err = rs.TimeSeconds(i)
				*a = uint64(seconds)

			default:
				*a, _, err = rs.Uint64(i)
			}
		}

		if err != nil {
			panic(err)
		}
	}

	return
}

// ScanNext scans the fields of the next row in the ResultSet, trying
// to store field values into the specified arguments. The arguments
// must be of pointer types. If a row has been fetched, fetched will
// be true, otherwise false.
func (rs *ResultSet) ScanNext(args ...interface{}) (fetched bool, err os.Error) {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.ScanNext"))
	}

	fetched, err = rs.FetchNext()
	if !fetched || err != nil {
		return
	}

	return true, rs.Scan(args...)
}
