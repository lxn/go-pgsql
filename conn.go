// Copyright 2010 Alexander Neumann. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The pgsql package implements a PostgreSQL frontend library.
// It is compatible with servers of version 7.4 and later.
package pgsql

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

// LogLevel is used to control what written to the log.
type LogLevel int

const (
	// Log nothing.
	LogNothing LogLevel = iota

	// Log fatal errors.
	LogFatal

	// Log all errors.
	LogError

	// Log errors and warnings.
	LogWarning

	// Log errors, warnings and sent commands.
	LogCommand

	// Log errors, warnings, sent commands and additional debug info.
	LogDebug

	// Log everything.
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

// IsolationLevel represents the isolation level of a transaction.
type IsolationLevel int

const (
	ReadCommittedIsolation IsolationLevel = iota
	SerializableIsolation
)

func (il IsolationLevel) String() string {
	switch il {
	case ReadCommittedIsolation:
		return "Read Committed"

	case SerializableIsolation:
		return "Serializable"
	}

	return "Unknown"
}

// TransactionStatus represents the transaction status of a connection.
type TransactionStatus byte

const (
	NotInTransaction    TransactionStatus = 'I'
	InTransaction       TransactionStatus = 'T'
	InFailedTransaction TransactionStatus = 'E'
)

func (s TransactionStatus) String() string {
	switch s {
	case NotInTransaction:
		return "Not In Transaction"

	case InTransaction:
		return "In Transaction"

	case InFailedTransaction:
		return "In Failed Transaction"
	}

	return "Unknown"
}

// Conn represents a PostgreSQL database connection.
type Conn struct {
	LogLevel                        LogLevel
	tcpConn                         net.Conn
	reader                          *bufio.Reader
	writer                          *bufio.Writer
	params                          *ConnParams
	state                           state
	backendPID                      int32
	backendSecretKey                int32
	onErrorDontRequireReadyForQuery bool
	runtimeParameters               map[string]string
	nextStatementId                 uint64
	nextPortalId                    uint64
	nextSavepointId                 uint64
	transactionStatus               TransactionStatus
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
		params.Host = "localhost"
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

	newConn.runtimeParameters = make(map[string]string)

	newConn.writeStartup()

	newConn.readBackendMessages(nil)

	newConn.state = readyState{}
	newConn.params = nil

	newConn.transactionStatus = NotInTransaction

	conn = newConn

	return
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

// PrepareSlice returns a new prepared Statement, optimized to be executed multiple
// times with different parameter values.
func (conn *Conn) PrepareSlice(command string, params []*Parameter) (stmt *Statement, err os.Error) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.PrepareSlice"))
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

// Prepare returns a new prepared Statement, optimized to be executed multiple
// times with different parameter values.
func (conn *Conn) Prepare(command string, params ...*Parameter) (stmt *Statement, err os.Error) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.Prepare"))
	}

	return conn.PrepareSlice(command, params)
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

// RuntimeParameter returns the value of the specified runtime parameter.
// If the value was successfully retrieved, ok is true, otherwise false.
func (conn *Conn) RuntimeParameter(name string) (value string, ok bool) {
	if conn.LogLevel >= LogVerbose {
		defer conn.logExit(conn.logEnter("*Conn.RuntimeParameter"))
	}

	value, ok = conn.runtimeParameters[name]
	return
}

// Scan executes the command and scans the fields of the first row
// in the ResultSet, trying to store field values into the specified
// arguments. The arguments must be of pointer types. If a row has
// been fetched, fetched will be true, otherwise false.
func (conn *Conn) Scan(command string, args ...interface{}) (fetched bool, err os.Error) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.Scan"))
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
	defer res.Close()

	return res.ScanNext(args)
}

// Status returns the current connection status.
func (conn *Conn) Status() ConnStatus {
	return conn.state.code()
}

// TransactionStatus returns the current transaction status of the connection.
func (conn *Conn) TransactionStatus() TransactionStatus {
	return conn.transactionStatus
}

// WithTransaction starts a new transaction, if none is in progress, then
// calls f. If f returns an error or panicks, the transaction is rolled back,
// otherwise it is committed. If the connection is in a failed transaction when
// calling WithTransaction, this function immediately returns with an error,
// without calling f. In case of an active transaction without error,
// WithTransaction just calls f.
func (conn *Conn) WithTransaction(isolation IsolationLevel, f func() os.Error) (err os.Error) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.WithTransaction"))
	}

	oldStatus := conn.transactionStatus

	if oldStatus == InFailedTransaction {
		return conn.logAndConvertPanic("error in transaction")
	}

	defer func() {
		if x := recover(); x != nil {
			err = conn.logAndConvertPanic(x)
		}
		if err == nil && conn.transactionStatus == InFailedTransaction {
			err = conn.logAndConvertPanic("error in transaction")
		}
		if err != nil && oldStatus == NotInTransaction {
			conn.Execute("ROLLBACK;")
		}
	}()

	if oldStatus == NotInTransaction {
		var isol string
		if isolation == SerializableIsolation {
			isol = "SERIALIZABLE"
		} else {
			isol = "READ COMMITTED"
		}
		cmd := fmt.Sprintf("BEGIN; SET TRANSACTION ISOLATION LEVEL %s;", isol)
		_, err = conn.Execute(cmd)
		if err != nil {
			panic(err)
		}
	}

	err = f()
	if err != nil {
		panic(err)
	}

	if oldStatus == NotInTransaction && conn.transactionStatus == InTransaction {
		_, err = conn.Execute("COMMIT;")
	}
	return
}

// WithSavepoint creates a transaction savepoint, if the connection is in an
// active transaction without errors, then calls f. If f returns an error or
// panicks, the transaction is rolled back to the savepoint. If the connection
// is in a failed transaction when calling WithSavepoint, this function
// immediately returns with an error, without calling f. If no transaction is in
// progress, instead of creating a savepoint, a new transaction is started.
func (conn *Conn) WithSavepoint(isolation IsolationLevel, f func() os.Error) (err os.Error) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.WithSavepoint"))
	}

	oldStatus := conn.transactionStatus

	switch oldStatus {
	case InFailedTransaction:
		return conn.logAndConvertPanic("error in transaction")

	case NotInTransaction:
		return conn.WithTransaction(isolation, f)
	}

	savepointName := fmt.Sprintf("sp%d", conn.nextSavepointId)
	conn.nextSavepointId++

	defer func() {
		if x := recover(); x != nil {
			err = conn.logAndConvertPanic(x)
		}
		if err == nil && conn.transactionStatus == InFailedTransaction {
			err = conn.logAndConvertPanic("error in transaction")
		}
		if err != nil {
			conn.Execute(fmt.Sprintf("ROLLBACK TO %s;", savepointName))
		}
	}()

	_, err = conn.Execute(fmt.Sprintf("SAVEPOINT %s;", savepointName))
	if err != nil {
		panic(err)
	}

	err = f()
	if err != nil {
		panic(err)
	}

	return
}
