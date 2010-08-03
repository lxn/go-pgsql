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

// Reader reads the results of a query, row by row, and provides methods to
// retrieve field values of the current row.
// Access is by 0-based field ordinal position.
type Reader struct {
	conn         *Conn
	rowsAffected int64
	row          int64
	name2ord     map[string]int
	fields       []field
	values       [][]byte
}

func newReader(conn *Conn) *Reader {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("newReader"))
	}

	return &Reader{conn: conn, row: -2}
}

func (r *Reader) initialize() {
	if r.conn.LogLevel >= LogDebug {
		defer r.conn.logExit(r.conn.logEnter("*Reader.initialize"))
	}

	// Just eat message length.
	r.conn.readInt32()

	fieldCount := r.conn.readInt16()

	r.fields = make([]field, fieldCount)
	r.values = make([][]byte, fieldCount)

	var ord int16
	for ord = 0; ord < fieldCount; ord++ {
		r.fields[ord].name = r.conn.readString()

		// Just eat table OID.
		r.conn.readInt32()

		// Just eat field OID.
		r.conn.readInt16()

		// Just eat field data type OID.
		r.conn.readInt32()

		// Just eat field size.
		r.conn.readInt16()

		// Just eat field type modifier.
		r.conn.readInt32()

		format := fieldFormat(r.conn.readInt16())
		switch format {
		case textFormat:
		case binaryFormat:
		default:
			panic("unsupported field format")
		}
		r.fields[ord].format = format
	}

	r.name2ord = make(map[string]int)

	for ord, field := range r.fields {
		r.name2ord[field.name] = ord
	}

	r.row = -1
}

func (r *Reader) readRow() {
	if r.conn.LogLevel >= LogDebug {
		defer r.conn.logExit(r.conn.logEnter("*Reader.readRow"))
	}

	// Just eat message length.
	r.conn.readInt32()

	fieldCount := r.conn.readInt16()

	var ord int16
	for ord = 0; ord < fieldCount; ord++ {
		valLen := r.conn.readInt32()

		var val []byte

		if valLen == -1 {
			val = nil
		} else {
			val = make([]byte, valLen)
			r.conn.read(val)
		}

		r.values[ord] = val
	}
}

// ReadNext reads the next row, if there is one.
// If a new row has been read it returns true, otherwise false.
func (r *Reader) ReadNext() (hasData bool, err os.Error) {
	defer func() {
		if x := recover(); x != nil {
			err = r.conn.logAndConvertPanic(x)
		}
	}()

	if r.conn.LogLevel >= LogDebug {
		defer r.conn.logExit(r.conn.logEnter("*Reader.ReadNext"))
	}

	hasData = r.row >= -1

	if !hasData {
		return
	}

	r.conn.state.processBackendMessages(r.conn, r)

	r.row++

	hasData = r.row >= -1

	return
}

// Close closes the reader, so another query or command can be sent to
// the server over the same connection.
func (r *Reader) Close() (err os.Error) {
	defer func() {
		if x := recover(); x != nil {
			err = r.conn.logAndConvertPanic(x)
		}
	}()

	if r.conn.LogLevel >= LogDebug {
		defer r.conn.logExit(r.conn.logEnter("*Reader.Close"))
	}

	// TODO: Instead of eating all records, try to cancel the query processing.
	// (The required message has to be sent through another connection though.)
	for {
		hasData, err := r.ReadNext()
		if err != nil {
			// FIXME: How should we handle this?
			return
		}
		if !hasData {
			r.conn.state = readyState{}
			return
		}
	}

	return
}

// IsNull returns if the value of the field with the specified ordinal is null.
func (r *Reader) IsNull(ord int) (isNull bool, err os.Error) {
	defer func() {
		if x := recover(); x != nil {
			err = r.conn.logAndConvertPanic(x)
		}
	}()

	if r.conn.LogLevel >= LogVerbose {
		defer r.conn.logExit(r.conn.logEnter("*Reader.IsNull"))
	}

	// Since all field value retrieval methods call this method,
	// we only check for a valid current row here.
	if r.row < 0 {
		panic("invalid row")
	}

	isNull = r.values[ord] == nil
	return
}

// Ordinal returns the 0-based ordinal position of the field with the
// specified name, or -1 if the reader has no field with such a name.
func (r *Reader) Ordinal(name string) int {
	if r.conn.LogLevel >= LogVerbose {
		defer r.conn.logExit(r.conn.logEnter("*Reader.Ordinal"))
	}

	ord, ok := r.name2ord[name]
	if !ok {
		return -1
	}

	return ord
}

// Bool returns the value of the field with the specified ordinal as bool.
func (r *Reader) Bool(ord int) (value, isNull bool, err os.Error) {
	defer func() {
		if x := recover(); x != nil {
			err = r.conn.logAndConvertPanic(x)
		}
	}()

	if r.conn.LogLevel >= LogVerbose {
		defer r.conn.logExit(r.conn.logEnter("*Reader.Bool"))
	}

	isNull, err = r.IsNull(ord)
	if isNull || err != nil {
		return
	}

	val := r.values[ord]

	switch r.fields[ord].format {
	case textFormat:
		value = val[0] == 't'

	case binaryFormat:
		value = val[0] != 0
	}

	return
}

// Byte returns the value of the field with the specified ordinal as byte.
func (r *Reader) Byte(ord int) (value byte, isNull bool, err os.Error) {
	defer func() {
		if x := recover(); x != nil {
			err = r.conn.logAndConvertPanic(x)
		}
	}()

	if r.conn.LogLevel >= LogVerbose {
		defer r.conn.logExit(r.conn.logEnter("*Reader.Byte"))
	}

	isNull, err = r.IsNull(ord)
	if isNull || err != nil {
		return
	}

	val := r.values[ord]

	switch r.fields[ord].format {
	case textFormat:
		x, err := strconv.Atoi(string(val))
		if err != nil {
			panic(err)
		}
		value = byte(x)

	case binaryFormat:
		value = val[0]
	}

	return
}

// Float32 returns the value of the field with the specified ordinal as float32.
func (r *Reader) Float32(ord int) (value float32, isNull bool, err os.Error) {
	defer func() {
		if x := recover(); x != nil {
			err = r.conn.logAndConvertPanic(x)
		}
	}()

	if r.conn.LogLevel >= LogVerbose {
		defer r.conn.logExit(r.conn.logEnter("*Reader.Float32"))
	}

	isNull, err = r.IsNull(ord)
	if isNull || err != nil {
		return
	}

	val := r.values[ord]

	switch r.fields[ord].format {
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
func (r *Reader) Float64(ord int) (value float64, isNull bool, err os.Error) {
	defer func() {
		if x := recover(); x != nil {
			err = r.conn.logAndConvertPanic(x)
		}
	}()

	if r.conn.LogLevel >= LogVerbose {
		defer r.conn.logExit(r.conn.logEnter("*Reader.Float64"))
	}

	isNull, err = r.IsNull(ord)
	if isNull || err != nil {
		return
	}

	val := r.values[ord]

	switch r.fields[ord].format {
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

// Int16 returns the value of the field with the specified ordinal as int16.
func (r *Reader) Int16(ord int) (value int16, isNull bool, err os.Error) {
	defer func() {
		if x := recover(); x != nil {
			err = r.conn.logAndConvertPanic(x)
		}
	}()

	if r.conn.LogLevel >= LogVerbose {
		defer r.conn.logExit(r.conn.logEnter("*Reader.Int16"))
	}

	isNull, err = r.IsNull(ord)
	if isNull || err != nil {
		return
	}

	val := r.values[ord]

	switch r.fields[ord].format {
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
func (r *Reader) Int32(ord int) (value int32, isNull bool, err os.Error) {
	defer func() {
		if x := recover(); x != nil {
			err = r.conn.logAndConvertPanic(x)
		}
	}()

	if r.conn.LogLevel >= LogVerbose {
		defer r.conn.logExit(r.conn.logEnter("*Reader.Int32"))
	}

	isNull, err = r.IsNull(ord)
	if isNull || err != nil {
		return
	}

	val := r.values[ord]

	switch r.fields[ord].format {
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
func (r *Reader) Int64(ord int) (value int64, isNull bool, err os.Error) {
	defer func() {
		if x := recover(); x != nil {
			err = r.conn.logAndConvertPanic(x)
		}
	}()

	if r.conn.LogLevel >= LogVerbose {
		defer r.conn.logExit(r.conn.logEnter("*Reader.Int64"))
	}

	isNull, err = r.IsNull(ord)
	if isNull || err != nil {
		return
	}

	val := r.values[ord]

	switch r.fields[ord].format {
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

// String returns the value of the field with the specified ordinal as string.
func (r *Reader) String(ord int) (value string, isNull bool, err os.Error) {
	defer func() {
		if x := recover(); x != nil {
			err = r.conn.logAndConvertPanic(x)
		}
	}()

	if r.conn.LogLevel >= LogVerbose {
		defer r.conn.logExit(r.conn.logEnter("*Reader.String"))
	}

	isNull, err = r.IsNull(ord)
	if isNull || err != nil {
		return
	}

	value = string(r.values[ord])

	return
}
