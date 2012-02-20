// Copyright 2010 The go-pgsql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgsql

import (
	"bytes"
	"fmt"
	"regexp"
)

var quoteRegExp = regexp.MustCompile("['][^']*[']")

// Statement is a means to efficiently execute a parameterized SQL command multiple times.
//
// Call *Conn.Prepare to create a new prepared Statement.
type Statement struct {
	conn          *Conn
	name          string
	portalName    string
	command       string
	actualCommand string
	isClosed      bool
	params        []*Parameter
	name2param    map[string]*Parameter
}

func replaceParameterNameInSubstring(s, old, new string, buf *bytes.Buffer, paramRegExp *regexp.Regexp) {
	matchIndexPairs := paramRegExp.FindAllStringIndex(s, -1)
	prevMatchEnd := 1

	for _, pair := range matchIndexPairs {
		matchStart := pair[0]
		matchEnd := pair[1]

		buf.WriteString(s[prevMatchEnd-1 : matchStart+1])
		buf.WriteString(new)

		prevMatchEnd = matchEnd
	}

	if prevMatchEnd > 1 {
		buf.WriteString(s[prevMatchEnd-1:])
		return
	}

	buf.WriteString(s)
}

func replaceParameterName(command, old, new string) string {
	paramRegExp := regexp.MustCompile("[\\- |\n\r\t,)(;=+/<>][:|@]" + old[1:] + "([\\- |\n\r\t,)(;=+/<>]|$)")

	buf := bytes.NewBuffer(nil)

	quoteIndexPairs := quoteRegExp.FindAllStringIndex(command, -1)
	prevQuoteEnd := 0

	for _, pair := range quoteIndexPairs {
		quoteStart := pair[0]
		quoteEnd := pair[1]

		replaceParameterNameInSubstring(command[prevQuoteEnd:quoteStart], old, new, buf, paramRegExp)
		buf.WriteString(command[quoteStart:quoteEnd])

		prevQuoteEnd = quoteEnd
	}

	if buf.Len() > 0 {
		replaceParameterNameInSubstring(command[prevQuoteEnd:], old, new, buf, paramRegExp)

		return buf.String()
	}

	replaceParameterNameInSubstring(command, old, new, buf, paramRegExp)

	return buf.String()
}

func adjustCommand(command string, params []*Parameter) string {
	for i, p := range params {
		var cast string
		if p.customTypeName != "" {
			cast = fmt.Sprintf("::%s", p.customTypeName)
		}
		command = replaceParameterName(command, p.name, fmt.Sprintf("$%d%s", i+1, cast))
	}

	return command
}

func newStatement(conn *Conn, command string, params []*Parameter) *Statement {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("newStatement"))
	}

	stmt := &Statement{}

	stmt.name2param = make(map[string]*Parameter)

	for _, param := range params {
		if param == nil {
			panic("received a nil parameter")
		}
		if param.stmt != nil {
			panic(fmt.Sprintf("parameter '%s' already used in another statement", param.name))
		}
		param.stmt = stmt

		stmt.name2param[param.name] = param
	}

	stmt.conn = conn

	stmt.name = fmt.Sprint("stmt", conn.nextStatementId)
	conn.nextStatementId++

	stmt.portalName = fmt.Sprint("prtl", conn.nextPortalId)
	conn.nextPortalId++

	stmt.command = command
	stmt.actualCommand = adjustCommand(command, params)

	stmt.params = make([]*Parameter, len(params))
	copy(stmt.params, params)

	return stmt
}

// Conn returns the *Conn this Statement is associated with.
func (stmt *Statement) Conn() *Conn {
	return stmt.conn
}

// Parameter returns the Parameter with the specified name or nil, if the Statement has no Parameter with that name.
func (stmt *Statement) Parameter(name string) *Parameter {
	conn := stmt.conn

	if conn.LogLevel >= LogVerbose {
		defer conn.logExit(conn.logEnter("*Statement.Parameter"))
	}

	param, ok := stmt.name2param[name]
	if !ok {
		return nil
	}

	return param
}

// Parameters returns a slice containing the parameters of the Statement.
func (stmt *Statement) Parameters() []*Parameter {
	conn := stmt.conn

	if conn.LogLevel >= LogVerbose {
		defer conn.logExit(conn.logEnter("*Statement.Parameters"))
	}

	params := make([]*Parameter, len(stmt.params))
	copy(params, stmt.params)
	return params
}

// IsClosed returns if the Statement has been closed.
func (stmt *Statement) IsClosed() bool {
	conn := stmt.conn

	if conn.LogLevel >= LogVerbose {
		defer conn.logExit(conn.logEnter("*Statement.IsClosed"))
	}

	return stmt.isClosed
}

func (stmt *Statement) close() {
	conn := stmt.conn

	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Statement.close"))
	}

	stmt.conn.writeClose('S', stmt.name)

	stmt.isClosed = true
	return
}

// Close closes the Statement, releasing resources on the server.
func (stmt *Statement) Close() (err error) {
	err = stmt.conn.withRecover("*Statement.Close", func() {
		stmt.close()
	})

	return
}

// ActualCommand returns the actual command text that is sent to the server.
//
// The original command is automatically adjusted if it contains parameters so
// it complies with what PostgreSQL expects. Refer to the return value of this
// method to make sense of the position information contained in many error
// messages.
func (stmt *Statement) ActualCommand() string {
	conn := stmt.conn

	if conn.LogLevel >= LogVerbose {
		defer conn.logExit(conn.logEnter("*Statement.ActualCommand"))
	}

	return stmt.actualCommand
}

// Command is the original command text as given to *Conn.Prepare.
func (stmt *Statement) Command() string {
	conn := stmt.conn

	if conn.LogLevel >= LogVerbose {
		defer conn.logExit(conn.logEnter("*Statement.Command"))
	}

	return stmt.command
}

func (stmt *Statement) query() (rs *ResultSet) {
	conn := stmt.conn

	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Statement.query"))
	}

	if conn.LogLevel >= LogCommand {
		buf := bytes.NewBuffer(nil)

		buf.WriteString("\n=================================================\n")

		buf.WriteString("ActualCommand:\n")
		buf.WriteString(stmt.actualCommand)
		buf.WriteString("\n-------------------------------------------------\n")
		buf.WriteString("Parameters:\n")

		for i, p := range stmt.params {
			buf.WriteString(fmt.Sprintf("$%d (%s) = '%v'\n", i+1, p.name, p.value))
		}

		buf.WriteString("=================================================\n")

		conn.log(LogCommand, buf.String())
	}

	r := newResultSet(conn)

	conn.state.execute(stmt, r)

	rs = r

	return
}

// Query executes the Statement and returns a
// ResultSet for row-by-row retrieval of the results.
//
// The returned ResultSet must be closed before sending another
// query or command to the server over the same connection.
func (stmt *Statement) Query() (rs *ResultSet, err error) {
	err = stmt.conn.withRecover("*Statement.Query", func() {
		rs = stmt.query()
	})

	return
}

func (stmt *Statement) execute() (rowsAffected int64) {
	conn := stmt.conn

	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Statement.Execute"))
	}

	rs := stmt.query()
	rs.close()

	return rs.rowsAffected
}

// Execute executes the Statement and returns the number
// of rows affected.
//
// If the results of a query are needed, use the
// Query method instead.
func (stmt *Statement) Execute() (rowsAffected int64, err error) {
	err = stmt.conn.withRecover("*Statement.Execute", func() {
		rowsAffected = stmt.execute()
	})

	return
}

func (stmt *Statement) scan(args ...interface{}) (*ResultSet, bool) {
	conn := stmt.conn

	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Statement.Scan"))
	}

	rs := stmt.query()

	return rs, rs.scanNext(args...)
}

// Scan executes the statement and scans the fields of the first row
// in the ResultSet, trying to store field values into the specified
// arguments.
//
// The arguments must be of pointer types. If a row has
// been fetched, fetched will be true, otherwise false.
func (stmt *Statement) Scan(args ...interface{}) (fetched bool, err error) {
	err = stmt.conn.withRecover("*Statement.Scan", func() {
		var rs *ResultSet
		rs, fetched = stmt.scan(args...)
		rs.close()
	})

	return
}
