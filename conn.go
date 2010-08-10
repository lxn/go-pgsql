// Copyright 2010 Alexander Neumann. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The pgsql package implements a PostgreSQL frontend library.
// It is compatible with servers of version 7.4 and later.
package pgsql

import (
	"bufio"
	"bytes"
	"fmt"
	"encoding/binary"
	"log"
	"math"
	"net"
	"os"
	"runtime"
)

// LogLevel is used to control which messages are written to the log.
type LogLevel int

const (
	LogNothing LogLevel = iota
	LogFatal
	LogError
	LogWarning
	LogDebug
	LogVerbose
)

var DefaultLogLevel LogLevel

type ConnParams struct {
	Host           string
	Port           int
	User           string
	Password       string
	Database       string
	TimeoutSeconds int
}

// ConnStatus represents the status of a connection.
type ConnStatus int

const (
	StatusDisconnected ConnStatus = iota
	StatusReady
	StatusProcessingQuery
)

func (s ConnStatus) String() string {
	switch s {
	case StatusDisconnected:
		return "Disconnected"

	case StatusReady:
		return "Ready"

	case StatusProcessingQuery:
		return "Processing Query"
	}

	return "Unknown"
}

// Conn represents a PostgreSQL database connection.
type Conn struct {
	LogLevel                        LogLevel
	tcpConn                         net.Conn
	params                          *ConnParams
	state                           state
	backendPID                      int32
	backendSecretKey                int32
	onErrorDontRequireReadyForQuery bool
	reader                          *bufio.Reader
	writer                          *bufio.Writer
}

// Connect establishes a database connection.
func Connect(parameters *ConnParams) (conn *Conn, err os.Error) {
	newConn := new(Conn)

	newConn.LogLevel = DefaultLogLevel
	newConn.state = disconnectedState{}

	if newConn.LogLevel >= LogDebug {
		defer newConn.logExit(newConn.logEnter("Connect"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = newConn.logAndConvertPanic(x)
		}
	}()

	params := new(ConnParams)
	*params = *parameters
	newConn.params = params

	if params.Host == "" {
		params.Host = "127.0.0.1"
	}
	if params.Port == 0 {
		params.Port = 5432
	}

	tcpConn, err := net.Dial("tcp", "", fmt.Sprintf("%s:%d", params.Host, params.Port))
	if err != nil {
		panic(err)
	}

	err = tcpConn.SetReadTimeout(int64(params.TimeoutSeconds * 1000 * 1000 * 1000))
	if err != nil {
		panic(err)
	}

	newConn.tcpConn = tcpConn

	newConn.reader = bufio.NewReader(tcpConn)
	newConn.writer = bufio.NewWriter(tcpConn)

	newConn.writeStartup(params)

	newConn.state.processBackendMessages(newConn, nil)

	newConn.state = readyState{}
	newConn.params = nil

	conn = newConn

	return
}

func (conn *Conn) writeStartup(params *ConnParams) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.writeStartup"))
	}

	msglen := int32(4 + 4 +
		len("user") + 1 + len(params.User) + 1 +
		len("database") + 1 + len(params.Database) + 1 + 1)

	conn.writeInt32(msglen)

	// For now we only support protocol version 3.0.
	conn.writeInt32(3 << 16)

	conn.writeString0("user")
	conn.writeString0(params.User)

	conn.writeString0("database")
	conn.writeString0(params.Database)

	conn.writeByte(0)

	conn.flush()
}

func (conn *Conn) writePasswordMessage(password string) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.writePasswordMessage"))
	}

	msgLen := int32(4 + len(password) + 1)

	conn.writeFrontendMessageCode(_PasswordMessage)
	conn.writeInt32(msgLen)
	conn.writeString0(password)

	conn.flush()
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

func (conn *Conn) writeFloat32(f float32) {
	var buf [4]byte
	b := buf[0:]

	binary.BigEndian.PutUint32(b, math.Float32bits(f))
	conn.write(b)
}

func (conn *Conn) writeFloat64(f float64) {
	var buf [8]byte
	b := buf[0:]

	binary.BigEndian.PutUint64(b, math.Float64bits(f))
	conn.write(b)
}

func (conn *Conn) writeFrontendMessageCode(code frontendMessageCode) {
	err := conn.writer.WriteByte(byte(code))
	if err != nil {
		panic(fmt.Sprintf("writeFrontendMessageCode failed: %s", err))
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

func (conn *Conn) writeInt64(i int64) {
	var buf [8]byte
	b := buf[0:]

	binary.BigEndian.PutUint64(b, uint64(i))
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
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.Close"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = conn.logAndConvertPanic(x)
		}
	}()

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

// Query sends a SQL query to the server and returns a
// ResultSet for row-by-row retrieval of the results.
// The returned ResultSet must be closed before sending another
// query or command to the server over the same connection.
func (conn *Conn) Query(command string) (res *ResultSet, err os.Error) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.Query"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = conn.logAndConvertPanic(x)
		}
	}()

	r := newResultSet(conn)

	conn.state.query(conn, r, command)

	res = r

	return
}

// Execute sends a SQL command to the server and returns the number
// of rows affected. If the results of a query are needed, use the
// Query method instead.
func (conn *Conn) Execute(command string) (rowsAffected int64, err os.Error) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.Execute"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = conn.logAndConvertPanic(x)
		}
	}()

	res, err := conn.Query(command)
	if err != nil {
		return
	}

	err = res.Close()

	rowsAffected = res.rowsAffected
	return
}

// Prepare returns a new prepared Statement, optimized to be executed multiple
// times with different parameter values.
func (conn *Conn) Prepare(command string, params []*Parameter) (stmt *Statement, err os.Error) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.Prepare"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = conn.logAndConvertPanic(x)
		}
	}()

	statement := newStatement(conn, command, params)

	conn.state.prepare(statement)

	stmt = statement
	return
}

// WithTransaction starts a transaction, then calls function f.
// If f returns an error or panicks, the transaction is rolled back,
// otherwise it is committed.
func (conn *Conn) WithTransaction(f func() os.Error) (err os.Error) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.WithTransaction"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = conn.logAndConvertPanic(x)
		}
		if err != nil {
			conn.Execute("ROLLBACK;")
		}
	}()

	_, err = conn.Execute("BEGIN;")
	if err != nil {
		panic(err)
	}

	err = f()
	if err != nil {
		panic(err)
	}

	_, err = conn.Execute("COMMIT;")
	return
}
