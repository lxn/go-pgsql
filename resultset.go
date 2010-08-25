// Copyright 2010 Alexander Neumann. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgsql

import (
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

func (res *ResultSet) initializeResult() {
	if res.conn.LogLevel >= LogDebug {
		defer res.conn.logExit(res.conn.logEnter("*ResultSet.initializeResult"))
	}

	res.conn.readRowDescription(res)

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

	res.conn.readDataRow(res)

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

// Conn returns the *Conn this ResultSet is associated with.
func (res *ResultSet) Conn() *Conn {
	return res.conn
}

// Statement returns the *Statement this ResultSet is associated with.
func (res *ResultSet) Statement() *Statement {
	return res.stmt
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
		res.conn.readBackendMessages(res)
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

	res.conn.readBackendMessages(res)

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
		defer res.conn.writeClose('P', res.stmt.portalName)
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

// Time returns the value of the field with the specified ordinal as *time.Time.
func (res *ResultSet) Time(ord int) (value *time.Time, isNull bool, err os.Error) {
	if res.conn.LogLevel >= LogVerbose {
		defer res.conn.logExit(res.conn.logEnter("*ResultSet.Time"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = res.conn.logAndConvertPanic(x)
		}
	}()

	// We need to convert the parsed *time.Time to seconds and back,
	// because otherwise the Weekday field will always equal 0 (Sunday).
	// See http://code.google.com/p/go/issues/detail?id=1025
	seconds, isNull, err := res.TimeSeconds(ord)
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
func (res *ResultSet) TimeSeconds(ord int) (value int64, isNull bool, err os.Error) {
	if res.conn.LogLevel >= LogVerbose {
		defer res.conn.logExit(res.conn.logEnter("*ResultSet.TimeSeconds"))
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

	var t *time.Time

	switch res.fields[ord].format {
	case textFormat:
		var format string
		switch res.fields[ord].typeOID {
		case _DATEOID:
			format = res.conn.dateFormat

		case _TIMEOID, _TIMETZOID:
			format = res.conn.timeFormat

		case _TIMESTAMPOID, _TIMESTAMPTZOID:
			format = res.conn.timestampFormat
		}

		var tzFormat, tzValueExtra string
		switch res.fields[ord].typeOID {
		case _TIMETZOID:
			tzFormat = "-0700"
			tzValueExtra = "00"

		case _TIMESTAMPTZOID:
			tzFormat = res.conn.timestampTimezoneFormat
			tzValueExtra = res.conn.timestampTimezoneValueExtra
		}

		s := string(val)

		if res.fields[ord].typeOID != _DATEOID {
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

		t, err = time.Parse(format+tzFormat, s+tzValueExtra)
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
func (res *ResultSet) Uint(ord int) (value uint, isNull bool, err os.Error) {
	if res.conn.LogLevel >= LogVerbose {
		defer res.conn.logExit(res.conn.logEnter("*ResultSet.Uint"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = res.conn.logAndConvertPanic(x)
		}
	}()

	val, isNull, err := res.Int32(ord)
	value = uint(val)
	return
}

// Uint16 returns the value of the field with the specified ordinal as uint16.
func (res *ResultSet) Uint16(ord int) (value uint16, isNull bool, err os.Error) {
	if res.conn.LogLevel >= LogVerbose {
		defer res.conn.logExit(res.conn.logEnter("*ResultSet.Uint16"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = res.conn.logAndConvertPanic(x)
		}
	}()

	val, isNull, err := res.Int16(ord)
	value = uint16(val)
	return
}

// Uint32 returns the value of the field with the specified ordinal as uint32.
func (res *ResultSet) Uint32(ord int) (value uint32, isNull bool, err os.Error) {
	if res.conn.LogLevel >= LogVerbose {
		defer res.conn.logExit(res.conn.logEnter("*ResultSet.Uint32"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = res.conn.logAndConvertPanic(x)
		}
	}()

	val, isNull, err := res.Int32(ord)
	value = uint32(val)
	return
}

// Uint64 returns the value of the field with the specified ordinal as uint64.
func (res *ResultSet) Uint64(ord int) (value uint64, isNull bool, err os.Error) {
	if res.conn.LogLevel >= LogVerbose {
		defer res.conn.logExit(res.conn.logEnter("*ResultSet.Uint64"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = res.conn.logAndConvertPanic(x)
		}
	}()

	val, isNull, err := res.Int64(ord)
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
func (res *ResultSet) Any(ord int) (value interface{}, isNull bool, err os.Error) {
	if res.conn.LogLevel >= LogVerbose {
		defer res.conn.logExit(res.conn.logEnter("*ResultSet.Any"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = res.conn.logAndConvertPanic(x)
		}
	}()

	if res.values[ord] == nil {
		isNull = true
		return
	}

	switch res.fields[ord].typeOID {
	case _BOOLOID:
		return res.Bool(ord)

	case _CHAROID, _VARCHAROID, _TEXTOID:
		return res.String(ord)

	case _DATEOID, _TIMEOID, _TIMETZOID, _TIMESTAMPOID, _TIMESTAMPTZOID:
		return res.TimeSeconds(ord)

	case _FLOAT4OID:
		return res.Float(ord)

	case _FLOAT8OID:
		return res.Float64(ord)

	case _INT2OID:
		return res.Int16(ord)

	case _INT4OID:
		return res.Int(ord)

	case _INT8OID:
		return res.Int64(ord)

	default:
		panic("unexpected field data type")
	}

	return
}

// Scan scans the fields of the current row in the ResultSet, trying
// to store field values into the specified arguments. The arguments
// must be of pointer types.
func (res *ResultSet) Scan(args ...interface{}) (err os.Error) {
	if res.conn.LogLevel >= LogVerbose {
		defer res.conn.logExit(res.conn.logEnter("*ResultSet.Scan"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = res.conn.logAndConvertPanic(x)
		}
	}()

	if len(args) != len(res.fields) {
		panic("wrong argument count")
	}

	for i, arg := range args {
		switch a := arg.(type) {
		case *bool:
			*a, _, err = res.Bool(i)

		case *float:
			*a, _, err = res.Float(i)

		case *float32:
			*a, _, err = res.Float32(i)

		case *float64:
			*a, _, err = res.Float64(i)

		case *int:
			*a, _, err = res.Int(i)

		case *int16:
			*a, _, err = res.Int16(i)

		case *int32:
			*a, _, err = res.Int32(i)

		case *int64:
			switch res.fields[i].typeOID {
			case _DATEOID, _TIMEOID, _TIMETZOID, _TIMESTAMPOID, _TIMESTAMPTZOID:
				*a, _, err = res.TimeSeconds(i)

			default:
				*a, _, err = res.Int64(i)
			}

		case *interface{}:
			*a, _, err = res.Any(i)

		case *string:
			*a, _, err = res.String(i)

		case **time.Time:
			var t *time.Time
			t, _, err = res.Time(i)
			if err == nil {
				*a = t
			}

		case *uint:
			*a, _, err = res.Uint(i)

		case *uint16:
			*a, _, err = res.Uint16(i)

		case *uint32:
			*a, _, err = res.Uint32(i)

		case *uint64:
			switch res.fields[i].typeOID {
			case _DATEOID, _TIMEOID, _TIMETZOID, _TIMESTAMPOID, _TIMESTAMPTZOID:
				var seconds int64
				seconds, _, err = res.TimeSeconds(i)
				*a = uint64(seconds)

			default:
				*a, _, err = res.Uint64(i)
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
func (res *ResultSet) ScanNext(args ...interface{}) (fetched bool, err os.Error) {
	if res.conn.LogLevel >= LogVerbose {
		defer res.conn.logExit(res.conn.logEnter("*ResultSet.ScanNext"))
	}

	fetched, err = res.FetchNext()
	if !fetched || err != nil {
		return
	}

	return true, res.Scan(args)
}
