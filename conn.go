// Copyright 2010 The go-pgsql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The pgsql package implements a PostgreSQL frontend library.
// It is compatible with servers of version 7.4 and later.
package pgsql

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// LogLevel is used to control what is written to the log.
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

type connParams struct {
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
	StatusCopy
)

func (s ConnStatus) String() string {
	switch s {
	case StatusDisconnected:
		return "Disconnected"

	case StatusReady:
		return "Ready"

	case StatusProcessingQuery:
		return "Processing Query"

	case StatusCopy:
		return "Bulk Copy"
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
	params                          *connParams
	state                           state
	backendPID                      int32
	backendSecretKey                int32
	onErrorDontRequireReadyForQuery bool
	runtimeParameters               map[string]string
	nextStatementId                 uint64
	nextPortalId                    uint64
	nextSavepointId                 uint64
	transactionStatus               TransactionStatus
	dateFormat                      string
	timeFormat                      string
	timestampFormat                 string
	timestampTimezoneFormat         string
}

func (conn *Conn) withRecover(funcName string, f func()) (err error) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter(funcName))
	}

	defer func() {
		if x := recover(); x != nil {
			err = conn.logAndConvertPanic(x)
		}
	}()

	f()

	return
}

func parseParamsInUnquotedSubstring(s string, name2value map[string]string) (lastKeyword string) {
	var words []string

	for {
		index := strings.IndexAny(s, "= \n\r\t")
		if index == -1 {
			break
		}

		word := s[0:index]
		if word != "" {
			words = append(words, word)
		}
		s = s[index+1:]
	}
	if len(s) > 0 {
		words = append(words, s)
	}

	for i := 0; i < len(words)-1; i += 2 {
		name2value[words[i]] = words[i+1]
	}

	if len(words) > 0 && len(words)%2 == 1 {
		lastKeyword = words[len(words)-1]
	}

	return
}

func (conn *Conn) parseParams(s string) *connParams {
	name2value := make(map[string]string)

	quoteIndexPairs := quoteRegExp.FindAllStringIndex(s, -1)
	prevQuoteEnd := 0

	for _, pair := range quoteIndexPairs {
		quoteStart := pair[0]
		quoteEnd := pair[1]

		lastKeyword := parseParamsInUnquotedSubstring(s[prevQuoteEnd:quoteStart], name2value)
		if lastKeyword != "" {
			name2value[lastKeyword] = s[quoteStart+1 : quoteEnd-1]
		}

		prevQuoteEnd = quoteEnd
	}

	if prevQuoteEnd > 0 {
		parseParamsInUnquotedSubstring(s[prevQuoteEnd:], name2value)
	} else {
		parseParamsInUnquotedSubstring(s, name2value)
	}

	params := &connParams{}

	params.Host = name2value["host"]
	params.Port, _ = strconv.Atoi(name2value["port"])
	params.Database = name2value["dbname"]
	params.User = name2value["user"]
	params.Password = name2value["password"]
	if params.Password == "" {
		params.Password, _ = passwordfromfile(params.Host, params.Port, params.Database, params.User)
	}
	params.TimeoutSeconds, _ = strconv.Atoi(name2value["timeout"])

	if conn.LogLevel >= LogDebug {
		buf := bytes.NewBuffer(nil)

		for name, value := range name2value {
			buf.WriteString(fmt.Sprintf("%s = '%s'\n", name, value))
		}

		conn.log(LogDebug, "Parsed connection parameter settings:\n", buf)
	}

	return params
}

// Connect establishes a database connection.
//
// Parameter settings in connStr have to be separated by whitespace and are
// expected in keyword = value form. Spaces around equal signs are optional.
// Use single quotes for empty values or values containing spaces.
//
// Currently these keywords are supported:
//
//	host 		= Name of the host to connect to (default: localhost)
//	port 		= Integer port number the server listens on (default: 5432)
//	dbname 		= Database name (default: same as user)
//	user 		= User to connect as
//	password	= Password for password based authentication methods
//	timeout		= Timeout in seconds, 0 or not specified disables timeout (default: 0)
func Connect(connStr string, logLevel LogLevel) (conn *Conn, err error) {
	newConn := &Conn{}

	newConn.LogLevel = logLevel
	newConn.state = disconnectedState{}

	if newConn.LogLevel >= LogDebug {
		defer newConn.logExit(newConn.logEnter("Connect"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = newConn.logAndConvertPanic(x)
		}
	}()

	params := newConn.parseParams(connStr)
	newConn.params = params

	var env string // Reusable environment variable used to capture PG environment variables - PGHOST, PGPORT, PGDATABASE, PGUSER

	if params.Host == "" {
		params.Host = "localhost"
	}
	env = os.Getenv("PGHOST")
	if env != "" {
		params.Host = env
	}
	if params.Port == 0 {
		params.Port = 5432
	}
	env = os.Getenv("PGPORT")
	if env != "" {
		params.Port, _ = strconv.Atoi(env)
	}
	if params.Database == "" {
		params.Database = params.User
	}
	env = os.Getenv("PGDATABASE")
	if env != "" {
		params.Database = env
	}
	env = os.Getenv("PGUSER")
	if env != "" {
		params.User = env
	}

	log.Print("1st LOG")
	tcpConn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", params.Host, params.Port))
	panicIfErr(err)

	//panicIfErr(tcpConn.SetDeadline(time.Unix(int64(params.TimeoutSeconds*1000*1000*1000), 0)))

	newConn.tcpConn = tcpConn

	newConn.reader = bufio.NewReader(tcpConn)
	newConn.writer = bufio.NewWriter(tcpConn)

	newConn.runtimeParameters = make(map[string]string)

	newConn.onErrorDontRequireReadyForQuery = true
	defer func() {
		newConn.onErrorDontRequireReadyForQuery = false
	}()

	log.Print("2nd LOG")
	newConn.writeStartup()

	log.Print("3rd LOG")
	newConn.readBackendMessages(nil)

	newConn.state = readyState{}
	newConn.params = nil

	newConn.transactionStatus = NotInTransaction

	conn = newConn
	//conn.log(LogPanic, "FIRST LOG")

	return
}

// Close closes the connection to the database.
func (conn *Conn) Close() (err error) {
	return conn.withRecover("*Conn.Close", func() {
		if conn.Status() == StatusDisconnected {
			err = errors.New("connection already closed")
			conn.logError(LogWarning, err)
			return
		}

		conn.writeTerminate()

		panicIfErr(conn.tcpConn.Close())

		conn.state = disconnectedState{}
	})
}

func (conn *Conn) copyFrom(command string, r io.Reader) int64 {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.copyFrom"))
	}

	conn.writeQuery(command)
	conn.readBackendMessages(nil)
	if stateCode := conn.state.code(); stateCode != StatusCopy {
		panic("wrong state, expected: StatusCopy, have: " + stateCode.String())
		return 0
	}

	// FIXME: magic number; wild guess without any reason.
	const CopyBufferSize = 32 << 10
	buf := make([]byte, CopyBufferSize)
	var nr int
	var err error
	for {
		nr, err = r.Read(buf)
		if err != nil && err != io.EOF {
			message := err.Error()
			conn.writeFrontendMessageCode(_CopyFail)
			conn.writeInt32(int32(5 + len(message)))
			conn.writeString0(message)
			panic(err)
		}
		if nr > 0 {
			conn.writeFrontendMessageCode(_CopyData_FE)
			conn.writeInt32(int32(4 + nr))
			conn.write(buf[:nr])
			conn.flush()
		}
		// TODO: peek backend message. Maybe there was error in data
		// and we can stop sending early.
		if err == io.EOF {
			break
		}
	}
	conn.writeFrontendMessageCode(_CopyDone_FE)
	conn.writeInt32(4)
	conn.flush()

	rs := newResultSet(conn)
	conn.readBackendMessages(rs)
	rs.close()

	return rs.rowsAffected
}

// CopyFrom sends a `COPY table FROM STDIN` SQL command to the server and
// returns the number of rows affected.
func (conn *Conn) CopyFrom(command string, r io.Reader) (rowsAffected int64, err error) {
	err = conn.withRecover("*Conn.CopyFrom", func() {
		rowsAffected = conn.copyFrom(command, r)
	})

	return
}

func getpgpassfilename() string {
	var env string
	env = os.Getenv("PGPASSFILE")
	if env != "" {
		return env
	}
	env = os.Getenv("HOME")
	return fmt.Sprintf("%s/.pgpass", env)
}

func passwordfromfile(hostname string, port int, dbname string, username string) (string, error) {
	var sport string
	var lhostname string
	if dbname == "" {
		return "", nil
	}
	if username == "" {
		return "", nil
	}
	if hostname == "" {
		lhostname = "localhost"
	} else {
		lhostname = hostname
	}
	if port == 0 {
		sport = "5432"
	} else {
		sport = fmt.Sprintf("%d", port)
	}
	pgfile := getpgpassfilename()
	fileinfo, err := os.Stat(pgfile)
	if err != nil {
		err := errors.New(fmt.Sprintf("WARNING: password file \"%s\" is not a plain file\n", pgfile))
		return "", err
	}
	if (fileinfo.Mode() & 077) != 0 {
		err := errors.New(fmt.Sprintf("WARNING: password file \"%s\" has group or world access; permissions should be u=rw (0600) or less", pgfile))
		return "", err
	}
	fp, err := os.Open(pgfile)
	if err != nil {
		err := errors.New(fmt.Sprintf("Problem opening pgpass file \"%s\"", pgfile))
		return "", err
	}
	br := bufio.NewReader(fp)
	for {
		line, ok := br.ReadString('\n')
		if ok == io.EOF {
			return "", nil
		}
		// Now, split the line into pieces
		// hostname:port:database:username:password
		// and * matches anything
		pieces := strings.Split(line, ":")
		phost := pieces[0]
		pport := pieces[1]
		pdb := pieces[2]
		puser := pieces[3]
		ppass := pieces[4]

		if (phost == lhostname || phost == "*") &&
			(pport == "*" || pport == sport) &&
			(pdb == "*" || pdb == dbname) &&
			(puser == "*" || puser == username) {

			return ppass, nil
		}
	}
	return "", nil
}

func (conn *Conn) execute(command string, params ...*Parameter) int64 {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.execute"))
	}

	rs := conn.query(command, params...)
	rs.close()

	return rs.rowsAffected
}

// Execute sends a SQL command to the server and returns the number
// of rows affected.
//
// If the results of a query are needed, use the
// Query method instead.
func (conn *Conn) Execute(command string, params ...*Parameter) (rowsAffected int64, err error) {
	err = conn.withRecover("*Conn.Execute", func() {
		rowsAffected = conn.execute(command, params...)
	})

	return
}

func (conn *Conn) prepare(command string, params ...*Parameter) *Statement {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.prepare"))
	}

	stmt := newStatement(conn, command, params)

	conn.state.prepare(stmt)

	return stmt
}

// Prepare returns a new prepared Statement, optimized to be executed multiple
// times with different parameter values.
func (conn *Conn) Prepare(command string, params ...*Parameter) (stmt *Statement, err error) {
	err = conn.withRecover("*Conn.Prepare", func() {
		stmt = conn.prepare(command, params...)
	})

	return
}

func (conn *Conn) query(command string, params ...*Parameter) (rs *ResultSet) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.query"))
	}

	var stmt *Statement
	if len(params) == 0 {
		r := newResultSet(conn)

		conn.state.query(conn, r, command)

		rs = r
	} else {
		stmt = conn.prepare(command, params...)
		defer stmt.close()

		rs = stmt.query()
	}

	return
}

// Query sends a SQL query to the server and returns a
// ResultSet for row-by-row retrieval of the results.
//
// The returned ResultSet must be closed before sending another
// query or command to the server over the same connection.
func (conn *Conn) Query(command string, params ...*Parameter) (rs *ResultSet, err error) {
	err = conn.withRecover("*Conn.Query", func() {
		rs = conn.query(command, params...)
	})

	return
}

// RuntimeParameter returns the value of the specified runtime parameter.
//
// If the value was successfully retrieved, ok is true, otherwise false.
func (conn *Conn) RuntimeParameter(name string) (value string, ok bool) {
	if conn.LogLevel >= LogVerbose {
		defer conn.logExit(conn.logEnter("*Conn.RuntimeParameter"))
	}

	value, ok = conn.runtimeParameters[name]
	return
}

func (conn *Conn) scan(command string, args ...interface{}) (*ResultSet, bool) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.scan"))
	}

	rs := conn.query(command)

	return rs, rs.scanNext(args...)
}

// Scan executes the command and scans the fields of the first row
// in the ResultSet, trying to store field values into the specified
// arguments.
//
// The arguments must be of pointer types. If a row has
// been fetched, fetched will be true, otherwise false.
func (conn *Conn) Scan(command string, args ...interface{}) (fetched bool, err error) {
	err = conn.withRecover("*Conn.Scan", func() {
		var rs *ResultSet
		rs, fetched = conn.scan(command, args...)
		rs.close()
	})

	return
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
// calls f.
//
// If f returns an error or panicks, the transaction is rolled back,
// otherwise it is committed. If the connection is in a failed transaction when
// calling WithTransaction, this function immediately returns with an error,
// without calling f. In case of an active transaction without error,
// WithTransaction just calls f.
func (conn *Conn) WithTransaction(isolation IsolationLevel, f func() error) (err error) {
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
			conn.execute("ROLLBACK;")
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
		conn.execute(cmd)
	}

	panicIfErr(f())

	if oldStatus == NotInTransaction && conn.transactionStatus == InTransaction {
		conn.execute("COMMIT;")
	}
	return
}

// WithSavepoint creates a transaction savepoint, if the connection is in an
// active transaction without errors, then calls f.
//
// If f returns an error or
// panicks, the transaction is rolled back to the savepoint. If the connection
// is in a failed transaction when calling WithSavepoint, this function
// immediately returns with an error, without calling f. If no transaction is in
// progress, instead of creating a savepoint, a new transaction is started.
func (conn *Conn) WithSavepoint(isolation IsolationLevel, f func() error) (err error) {
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
			conn.execute(fmt.Sprintf("ROLLBACK TO %s;", savepointName))
		}
	}()

	conn.execute(fmt.Sprintf("SAVEPOINT %s;", savepointName))

	panicIfErr(f())

	return
}

func (conn *Conn) updateTimeFormats() {
	style := conn.runtimeParameters["DateStyle"]

	switch style {
	case "ISO", "ISO, DMY", "ISO, MDY", "ISO, YMD":
		conn.dateFormat = "2006-01-02"
		conn.timeFormat = "15:04:05"
		conn.timestampFormat = "2006-01-02 15:04:05"
		conn.timestampTimezoneFormat = "-07"

	case "SQL", "SQL, MDY":
		conn.dateFormat = "01/02/2006"
		conn.timeFormat = "15:04:05"
		conn.timestampFormat = "01/02/2006 15:04:05"
		conn.timestampTimezoneFormat = " MST"

	case "SQL, DMY":
		conn.dateFormat = "02/01/2006"
		conn.timeFormat = "15:04:05"
		conn.timestampFormat = "02/01/2006 15:04:05"
		conn.timestampTimezoneFormat = " MST"

	case "Postgres", "Postgres, DMY":
		conn.dateFormat = "02-01-2006"
		conn.timeFormat = "15:04:05"
		conn.timestampFormat = "Mon 02 Jan 15:04:05 2006"
		conn.timestampTimezoneFormat = " MST"

	case "Postgres, MDY":
		conn.dateFormat = "01-02-2006"
		conn.timeFormat = "15:04:05"
		conn.timestampFormat = "Mon Jan 02 15:04:05 2006"
		conn.timestampTimezoneFormat = " MST"

	case "German", "German, DMY", "German, MDY":
		conn.dateFormat = "02.01.2006"
		conn.timeFormat = "15:04:05"
		conn.timestampFormat = "02.01.2006 15:04:05"
		conn.timestampTimezoneFormat = " MST"

	default:
		if conn.LogLevel >= LogWarning {
			conn.log(LogWarning, "Unknown DateStyle: "+style)
		}
		conn.dateFormat = ""
		conn.timeFormat = ""
		conn.timestampFormat = ""
		conn.timestampTimezoneFormat = ""
	}
}
