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

func (rs *ResultSet) eatCurrentResultRows() {
	for {
		hasRow := rs.fetchNext()
		if !hasRow {
			return
		}
	}
}

func (rs *ResultSet) eatAllResultRows() {
	for {
		hasResult := rs.nextResult()
		if !hasResult {
			return
		}
	}
}

// Conn returns the *Conn this ResultSet is associated with.
func (rs *ResultSet) Conn() *Conn {
	return rs.conn
}

// Statement returns the *Statement this ResultSet is associated with.
func (rs *ResultSet) Statement() *Statement {
	return rs.stmt
}

func (rs *ResultSet) nextResult() bool {
	if rs.conn.LogLevel >= LogDebug {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.nextResult"))
	}

	rs.eatCurrentResultRows()

	if !rs.allResultsComplete {
		rs.conn.readBackendMessages(rs)
	}

	return !rs.allResultsComplete
}

// NextResult moves the ResultSet to the next result, if there is one.
// In this case true is returned, otherwise false.
// Statements support a single result only, use *Conn.Query if you need
// this functionality.
func (rs *ResultSet) NextResult() (hasResult bool, err os.Error) {
	err = rs.conn.withRecover("*ResultSet.NextResult", func() {
		hasResult = rs.nextResult()
	})

	return
}

func (rs *ResultSet) fetchNext() bool {
	if rs.conn.LogLevel >= LogDebug {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.fetchNext"))
	}

	if rs.currentResultComplete {
		return false
	}

	rs.conn.readBackendMessages(rs)

	return !rs.currentResultComplete
}

// FetchNext reads the next row, if there is one.
// In this case true is returned, otherwise false.
func (rs *ResultSet) FetchNext() (hasRow bool, err os.Error) {
	err = rs.conn.withRecover("*ResultSet.FetchNext", func() {
		hasRow = rs.fetchNext()
	})

	return
}

func (rs *ResultSet) close() {
	if rs.conn.LogLevel >= LogDebug {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.close"))
	}

	if rs.stmt != nil {
		defer rs.conn.writeClose('P', rs.stmt.portalName)
	}

	// TODO: Instead of eating all records, try to cancel the query processing.
	// (The required message has to be sent through another connection though.)
	rs.eatAllResultRows()

	rs.conn.state = readyState{}
}

// Close closes the ResultSet, so another query or command can be sent to
// the server over the same connection.
func (rs *ResultSet) Close() (err os.Error) {
	err = rs.conn.withRecover("*ResultSet.Close", func() {
		rs.close()
	})

	return
}

func (rs *ResultSet) isNull(ord int) bool {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.isNull"))
	}

	// Since all field value retrieval methods call this method,
	// we only check for a valid current row here.
	if !rs.hasCurrentRow {
		panic("invalid row")
	}

	return rs.values[ord] == nil
}

// IsNull returns if the value of the field with the specified ordinal is null.
func (rs *ResultSet) IsNull(ord int) (isNull bool, err os.Error) {
	err = rs.conn.withRecover("*ResultSet.IsNull", func() {
		isNull = rs.isNull(ord)
	})

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

func (rs *ResultSet) bool_(ord int) (value, isNull bool) {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.bool_"))
	}

	isNull = rs.isNull(ord)
	if isNull {
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

// Bool returns the value of the field with the specified ordinal as bool.
func (rs *ResultSet) Bool(ord int) (value, isNull bool, err os.Error) {
	err = rs.conn.withRecover("*ResultSet.Bool", func() {
		value, isNull = rs.bool_(ord)
	})

	return
}

func (rs *ResultSet) float32_(ord int) (value float32, isNull bool) {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.float32_"))
	}

	isNull = rs.isNull(ord)
	if isNull {
		return
	}

	val := rs.values[ord]

	switch rs.fields[ord].format {
	case textFormat:
		// strconv.Atof32 does not handle NaN
		if string(val) == "NaN" {
			value = float32(math.NaN())
		} else {
			var err os.Error
			value, err = strconv.Atof32(string(val))
			panicIfErr(err)
		}

	case binaryFormat:
		value = math.Float32frombits(binary.BigEndian.Uint32(val))
	}

	return
}

// Float32 returns the value of the field with the specified ordinal as float32.
func (rs *ResultSet) Float32(ord int) (value float32, isNull bool, err os.Error) {
	err = rs.conn.withRecover("*ResultSet.Float32", func() {
		value, isNull = rs.float32_(ord)
	})

	return
}

func (rs *ResultSet) float64_(ord int) (value float64, isNull bool) {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.float64_"))
	}

	isNull = rs.isNull(ord)
	if isNull {
		return
	}

	val := rs.values[ord]

	switch rs.fields[ord].format {
	case textFormat:
		// strconv.Atof64 does not handle NaN
		if string(val) == "NaN" {
			value = math.NaN()
		} else {
			var err os.Error
			value, err = strconv.Atof64(string(val))
			panicIfErr(err)
		}

	case binaryFormat:
		value = math.Float64frombits(binary.BigEndian.Uint64(val))
	}

	return
}

// Float64 returns the value of the field with the specified ordinal as float64.
func (rs *ResultSet) Float64(ord int) (value float64, isNull bool, err os.Error) {
	err = rs.conn.withRecover("*ResultSet.Float64", func() {
		value, isNull = rs.float64_(ord)
	})

	return
}

func (rs *ResultSet) float_(ord int) (value float, isNull bool) {
	var val float32
	val, isNull = rs.float32_(ord)
	value = float(val)

	return
}

// Float returns the value of the field with the specified ordinal as float.
func (rs *ResultSet) Float(ord int) (value float, isNull bool, err os.Error) {
	err = rs.conn.withRecover("*ResultSet.Float", func() {
		value, isNull = rs.float_(ord)
	})

	return
}

func (rs *ResultSet) int16_(ord int) (value int16, isNull bool) {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.int16_"))
	}

	isNull = rs.isNull(ord)
	if isNull {
		return
	}

	val := rs.values[ord]

	switch rs.fields[ord].format {
	case textFormat:
		x, err := strconv.Atoi(string(val))
		panicIfErr(err)
		value = int16(x)

	case binaryFormat:
		value = int16(binary.BigEndian.Uint16(val))
	}

	return
}

// Int16 returns the value of the field with the specified ordinal as int16.
func (rs *ResultSet) Int16(ord int) (value int16, isNull bool, err os.Error) {
	err = rs.conn.withRecover("*ResultSet.Int16", func() {
		value, isNull = rs.int16_(ord)
	})

	return
}

func (rs *ResultSet) int32_(ord int) (value int32, isNull bool) {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.int32_"))
	}

	isNull = rs.isNull(ord)
	if isNull {
		return
	}

	val := rs.values[ord]

	switch rs.fields[ord].format {
	case textFormat:
		x, err := strconv.Atoi(string(val))
		panicIfErr(err)
		value = int32(x)

	case binaryFormat:
		value = int32(binary.BigEndian.Uint32(val))
	}

	return
}

// Int32 returns the value of the field with the specified ordinal as int32.
func (rs *ResultSet) Int32(ord int) (value int32, isNull bool, err os.Error) {
	err = rs.conn.withRecover("*ResultSet.Int32", func() {
		value, isNull = rs.int32_(ord)
	})

	return
}

func (rs *ResultSet) int64_(ord int) (value int64, isNull bool) {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.int64_"))
	}

	isNull = rs.isNull(ord)
	if isNull {
		return
	}

	val := rs.values[ord]

	switch rs.fields[ord].format {
	case textFormat:
		x, err := strconv.Atoi(string(val))
		panicIfErr(err)
		value = int64(x)

	case binaryFormat:
		value = int64(binary.BigEndian.Uint64(val))
	}

	return
}

// Int64 returns the value of the field with the specified ordinal as int64.
func (rs *ResultSet) Int64(ord int) (value int64, isNull bool, err os.Error) {
	err = rs.conn.withRecover("*ResultSet.Int64", func() {
		value, isNull = rs.int64_(ord)
	})

	return
}

func (rs *ResultSet) int_(ord int) (value int, isNull bool) {
	var val int32
	val, isNull = rs.int32_(ord)
	value = int(val)

	return
}

// Int returns the value of the field with the specified ordinal as int.
func (rs *ResultSet) Int(ord int) (value int, isNull bool, err os.Error) {
	err = rs.conn.withRecover("*ResultSet.Int", func() {
		value, isNull = rs.int_(ord)
	})

	return
}

func (rs *ResultSet) rat(ord int) (value *big.Rat, isNull bool) {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.rat"))
	}

	isNull = rs.isNull(ord)
	if isNull {
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
		panicNotImplemented()
	}

	return
}

// Rat returns the value of the field with the specified ordinal as *big.Rat.
func (rs *ResultSet) Rat(ord int) (value *big.Rat, isNull bool, err os.Error) {
	err = rs.conn.withRecover("*ResultSet.Rat", func() {
		value, isNull = rs.rat(ord)
	})

	return
}

func (rs *ResultSet) string_(ord int) (value string, isNull bool) {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.string_"))
	}

	isNull = rs.isNull(ord)
	if isNull {
		return
	}

	value = string(rs.values[ord])

	return
}

// String returns the value of the field with the specified ordinal as string.
func (rs *ResultSet) String(ord int) (value string, isNull bool, err os.Error) {
	err = rs.conn.withRecover("*ResultSet.String", func() {
		value, isNull = rs.string_(ord)
	})

	return
}

func (rs *ResultSet) time(ord int) (value *time.Time, isNull bool) {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.Time"))
	}

	// We need to convert the parsed *time.Time to seconds and back,
	// because otherwise the Weekday field will always equal 0 (Sunday).
	// See http://code.google.com/p/go/issues/detail?id=1025
	seconds, isNull := rs.timeSeconds(ord)
	if isNull {
		return
	}

	value = time.SecondsToUTC(seconds)

	return
}

// Time returns the value of the field with the specified ordinal as *time.Time.
func (rs *ResultSet) Time(ord int) (value *time.Time, isNull bool, err os.Error) {
	err = rs.conn.withRecover("*ResultSet.Time", func() {
		value, isNull = rs.time(ord)
	})

	return
}

func (rs *ResultSet) timeSeconds(ord int) (value int64, isNull bool) {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.timeSeconds"))
	}

	isNull = rs.isNull(ord)
	if isNull {
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
					s = s[:lastDot] + s[lastDot+plusOrMinus:]
				} else {
					s = s[:lastDot]
				}
			}
		}

		var err os.Error
		t, err = time.Parse(format, s)
		panicIfErr(err)

	case binaryFormat:
		panicNotImplemented()
	}

	value = t.Seconds()

	return
}

// TimeSeconds returns the value of the field with the specified ordinal as int64.
func (rs *ResultSet) TimeSeconds(ord int) (value int64, isNull bool, err os.Error) {
	err = rs.conn.withRecover("*ResultSet.TimeSeconds", func() {
		value, isNull = rs.timeSeconds(ord)
	})

	return
}

func (rs *ResultSet) uint_(ord int) (value uint, isNull bool) {
	var val uint32
	val, isNull = rs.uint32_(ord)
	value = uint(val)

	return
}

// Uint returns the value of the field with the specified ordinal as uint.
func (rs *ResultSet) Uint(ord int) (value uint, isNull bool, err os.Error) {
	err = rs.conn.withRecover("*ResultSet.Uint", func() {
		value, isNull = rs.uint_(ord)
	})

	return
}

func (rs *ResultSet) uint16_(ord int) (value uint16, isNull bool) {
	var val int16
	val, isNull = rs.int16_(ord)
	value = uint16(val)

	return
}

// Uint16 returns the value of the field with the specified ordinal as uint16.
func (rs *ResultSet) Uint16(ord int) (value uint16, isNull bool, err os.Error) {
	err = rs.conn.withRecover("*ResultSet.Uint16", func() {
		value, isNull = rs.uint16_(ord)
	})

	return
}

func (rs *ResultSet) uint32_(ord int) (value uint32, isNull bool) {
	var val int32
	val, isNull = rs.int32_(ord)
	value = uint32(val)

	return
}

// Uint32 returns the value of the field with the specified ordinal as uint32.
func (rs *ResultSet) Uint32(ord int) (value uint32, isNull bool, err os.Error) {
	err = rs.conn.withRecover("*ResultSet.Uint32", func() {
		value, isNull = rs.uint32_(ord)
	})

	return
}

func (rs *ResultSet) uint64_(ord int) (value uint64, isNull bool) {
	var val int64
	val, isNull = rs.int64_(ord)
	value = uint64(val)

	return
}

// Uint64 returns the value of the field with the specified ordinal as uint64.
func (rs *ResultSet) Uint64(ord int) (value uint64, isNull bool, err os.Error) {
	err = rs.conn.withRecover("*ResultSet.Uint64", func() {
		value, isNull = rs.uint64_(ord)
	})

	return
}

func (rs *ResultSet) any(ord int) (value interface{}, isNull bool) {
	if rs.values[ord] == nil {
		isNull = true
		return
	}

	switch rs.fields[ord].typeOID {
	case _BOOLOID:
		value, isNull = rs.bool_(ord)

	case _CHAROID, _VARCHAROID, _TEXTOID:
		value, isNull = rs.string_(ord)

	case _DATEOID, _TIMEOID, _TIMETZOID, _TIMESTAMPOID, _TIMESTAMPTZOID:
		value, isNull = rs.timeSeconds(ord)

	case _FLOAT4OID:
		value, isNull = rs.float_(ord)

	case _FLOAT8OID:
		value, isNull = rs.float64_(ord)

	case _INT2OID:
		value, isNull = rs.int16_(ord)

	case _INT4OID:
		value, isNull = rs.int_(ord)

	case _INT8OID:
		value, isNull = rs.int64_(ord)

	case _NUMERICOID:
		value, isNull = rs.rat(ord)

	default:
		panic("unexpected field data type")
	}

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
	err = rs.conn.withRecover("*ResultSet.Any", func() {
		value, isNull = rs.any(ord)
	})

	return
}

func (rs *ResultSet) scan(args ...interface{}) {
	if rs.conn.LogLevel >= LogVerbose {
		defer rs.conn.logExit(rs.conn.logEnter("*ResultSet.Scan"))
	}

	if len(args) != len(rs.fields) {
		panic("wrong argument count")
	}

	for i, arg := range args {
		switch a := arg.(type) {
		case *bool:
			*a, _ = rs.bool_(i)

		case *float:
			*a, _ = rs.float_(i)

		case *float32:
			*a, _ = rs.float32_(i)

		case *float64:
			*a, _ = rs.float64_(i)

		case *int:
			*a, _ = rs.int_(i)

		case *int16:
			*a, _ = rs.int16_(i)

		case *int32:
			*a, _ = rs.int32_(i)

		case *int64:
			switch rs.fields[i].typeOID {
			case _DATEOID, _TIMEOID, _TIMETZOID, _TIMESTAMPOID, _TIMESTAMPTZOID:
				*a, _ = rs.timeSeconds(i)

			default:
				*a, _ = rs.int64_(i)
			}

		case *interface{}:
			*a, _ = rs.any(i)

		case **big.Rat:
			var r *big.Rat
			r, _ = rs.rat(i)
			*a = r

		case *string:
			*a, _ = rs.string_(i)

		case **time.Time:
			var t *time.Time
			t, _ = rs.time(i)
			*a = t

		case *uint:
			*a, _ = rs.uint_(i)

		case *uint16:
			*a, _ = rs.uint16_(i)

		case *uint32:
			*a, _ = rs.uint32_(i)

		case *uint64:
			switch rs.fields[i].typeOID {
			case _DATEOID, _TIMEOID, _TIMETZOID, _TIMESTAMPOID, _TIMESTAMPTZOID:
				var seconds int64
				seconds, _ = rs.timeSeconds(i)
				*a = uint64(seconds)

			default:
				*a, _ = rs.uint64_(i)
			}
		}
	}

	return
}

// Scan scans the fields of the current row in the ResultSet, trying
// to store field values into the specified arguments. The arguments
// must be of pointer types.
func (rs *ResultSet) Scan(args ...interface{}) (err os.Error) {
	err = rs.conn.withRecover("*ResultSet.Scan", func() {
		rs.scan(args...)
	})

	return
}

func (rs *ResultSet) scanNext(args ...interface{}) (fetched bool) {
	fetched = rs.fetchNext()
	if !fetched {
		return
	}

	rs.scan(args...)

	return
}

// ScanNext scans the fields of the next row in the ResultSet, trying
// to store field values into the specified arguments. The arguments
// must be of pointer types. If a row has been fetched, fetched will
// be true, otherwise false.
func (rs *ResultSet) ScanNext(args ...interface{}) (fetched bool, err os.Error) {
	err = rs.conn.withRecover("*ResultSet.ScanNext", func() {
		fetched = rs.scanNext(args...)
	})

	return
}
