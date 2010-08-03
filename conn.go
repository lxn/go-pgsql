// Copyright 2010 Alexander Neumann. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The pgsql package partially implements a PostgreSQL frontend
// for use with protocol version 3.0.
package pgsql

import (
	"bufio"
	"bytes"
	"fmt"
	"encoding/binary"
	"log"
	"net"
	"os"
	"runtime"
)

type LogLevel int

const (
	LogNothing LogLevel = iota
	LogFatal
	LogError
	LogWarning
	LogDebug
	LogVerbose
)

type ConnParams struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
}

type ConnStatus int

const (
	StatusDisconnected ConnStatus = iota
	StatusConnected
	StatusAuthenticating
	StatusReady
	StatusProcessingQuery
)

func (s ConnStatus) String() string {
	switch s {
	case StatusDisconnected:
		return "Disconnected"

	case StatusConnected:
		return "Connected"

	case StatusAuthenticating:
		return "Authenticating"

	case StatusReady:
		return "Ready"

	case StatusProcessingQuery:
		return "Processing Query"
	}

	return "Unknown"
}

type Conn struct {
	LogLevel         LogLevel
	conn             net.Conn
	params           ConnParams
	state            state
	backendPID       int32
	backendSecretKey int32
	reader           *bufio.Reader
	writer           *bufio.Writer
}

// NewConn returns a new Conn initialized with the specified parameters.
func NewConn(params *ConnParams) (conn *Conn, err os.Error) {
	if params == nil {
		err = os.NewError("params cannot be nil")
		return
	}

	conn = new(Conn)
	conn.params = *params
	conn.state = disconnectedState{}

	if conn.params.Host == "" {
		conn.params.Host = "127.0.0.1"
	}
	if conn.params.Port == 0 {
		conn.params.Port = 5432
	}

	return
}

func (conn *Conn) log(level LogLevel, v ...interface{}) {
	log.Stdout(v)
}

func (conn *Conn) logf(level LogLevel, format string, v ...interface{}) {
	log.Stdoutf(format, v)
}

func (conn *Conn) logError(level LogLevel, err os.Error) {
	if conn.LogLevel >= level {
		conn.log(level, err)
	}
}

func (conn *Conn) logEnter(funcName string) string {
	conn.log(LogDebug, "entering: ", "pgsql."+funcName)
	return funcName
}

func (conn *Conn) logExit(funcName string) {
	conn.log(LogDebug, "exiting: ", "pgsql."+funcName)
}

func (conn *Conn) logAndConvertPanic(x interface{}) (err os.Error) {
	buf := bytes.NewBuffer(nil)

	buf.WriteString(fmt.Sprintf("Error: %v\nStack Trace:\n", x))
	buf.WriteString("=======================================================\n")

	i := 0
	for {
		pc, file, line, ok := runtime.Caller(i + 3)
		if !ok {
			break
		}
		if i > 0 {
			buf.WriteString("-------------------------------------------------------\n")
		}

		fun := runtime.FuncForPC(pc)
		name := fun.Name()

		buf.WriteString(fmt.Sprintf("%s (%s, Line %d)\n", name, file, line))

		i++
	}
	buf.WriteString("=======================================================\n")

	if conn.LogLevel >= LogError {
		conn.log(LogError, buf)
	}

	err, ok := x.(os.Error)
	if !ok {
		err = os.NewError(buf.String())
	}

	return
}

func (conn *Conn) flush() {
	err := conn.writer.Flush()
	if err != nil {
		panic(fmt.Sprintf("flush failed: %s", err))
	}
}

func (conn *Conn) read(b []byte) {
	_, err := conn.reader.Read(b)
	if err != nil {
		panic(fmt.Sprintf("read failed: %s", err))
	}
}

func (conn *Conn) readByte() byte {
	b, err := conn.reader.ReadByte()
	if err != nil {
		panic(fmt.Sprintf("readByte failed: %s", err))
	}

	return b
}

func (conn *Conn) readBytes(delim byte) []byte {
	b, err := conn.reader.ReadBytes(delim)
	if err != nil {
		panic(fmt.Sprintf("readBytes failed: %s", err))
	}

	return b
}

func (conn *Conn) readInt16() int16 {
	var buf [2]byte
	b := buf[0:]

	conn.read(b)
	return int16(binary.BigEndian.Uint16(b))
}

func (conn *Conn) readInt32() int32 {
	var buf [4]byte
	b := buf[0:]

	conn.read(b)
	return int32(binary.BigEndian.Uint32(b))
}

func (conn *Conn) readString() string {
	b := conn.readBytes(0)
	return string(b[0 : len(b)-1])
}

func (conn *Conn) write(b []byte) {
	_, err := conn.writer.Write(b)
	if err != nil {
		panic(fmt.Sprintf("write failed: %s", err))
	}
}

func (conn *Conn) writeByte(b byte) {
	err := conn.writer.WriteByte(b)
	if err != nil {
		panic(fmt.Sprintf("writeByte failed: %s", err))
	}
}

func (conn *Conn) writeInt16(i int16) {
	var buf [2]byte
	b := buf[0:]

	binary.BigEndian.PutUint16(b, uint16(i))
	conn.write(b)
}

func (conn *Conn) writeInt32(i int32) {
	var buf [4]byte
	b := buf[0:]

	binary.BigEndian.PutUint32(b, uint32(i))
	conn.write(b)
}

func (conn *Conn) writeString(s string) {
	_, err := conn.writer.WriteString(s)
	if err != nil {
		panic(fmt.Sprintf("writeString failed: %s", err))
	}
}

func (conn *Conn) writeString0(s string) {
	conn.writeString(s)
	conn.writeByte(0)
}

// Close closes the connection to the database.
func (conn *Conn) Close() (err os.Error) {
	defer func() {
		if x := recover(); x != nil {
			err = conn.logAndConvertPanic(x)
		}
	}()

	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.Close"))
	}

	if conn.Status() == StatusDisconnected {
		err = os.NewError("connection already closed")
		conn.logError(LogWarning, err)
		return
	}

	conn.state.disconnect(conn)

	return
}

// Status returns the current connection status.
func (conn *Conn) Status() ConnStatus {
	return conn.state.code()
}

// Open opens a database connection and waits until it is ready to issue commands.
func (conn *Conn) Open() (err os.Error) {
	defer func() {
		if x := recover(); x != nil {
			err = conn.logAndConvertPanic(x)
		}
	}()

	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.Open"))
	}

	conn.state.connect(conn)
	conn.state.startup(conn)

	return
}

// Query sends a SQL query to the server and returns a
// *pgsql.Reader for row-by-row retrieval of the results.
// The returned reader must be closed before sending another
// query or command to the server over the same connection.
func (conn *Conn) Query(sql string) (reader *Reader, err os.Error) {
	defer func() {
		if x := recover(); x != nil {
			err = conn.logAndConvertPanic(x)
		}
	}()

	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.Query"))
	}

	r := newReader(conn)

	conn.state.query(conn, r, sql)

	reader = r

	return
}

// Execute sends a SQL command to the server and returns the number
// of rows affected. If the results of a query are needed, use the
// Query method instead.
func (conn *Conn) Execute(sql string) (rowsAffected int64, err os.Error) {
	defer func() {
		if x := recover(); x != nil {
			err = conn.logAndConvertPanic(x)
		}
	}()

	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.Execute"))
	}

	reader, err := conn.Query(sql)
	if err != nil {
		return
	}

	err = reader.Close()

	rowsAffected = reader.rowsAffected
	return
}
