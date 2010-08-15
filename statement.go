// Copyright 2010 Alexander Neumann. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgsql

import (
	"bytes"
	"fmt"
	"os"
	"regexp"
)

var quoteRegExp = regexp.MustCompile("['][^']*[']")

// Statement is a means to efficiently execute a parameterized SQL command multiple times.
// Call *Conn.Prepare to create a new prepared Statement.
type Statement struct {
	conn                                     *Conn
	name, portalName, command, actualCommand string
	isClosed                                 bool
	params                                   []*Parameter
	name2param                               map[string]*Parameter
}

func replaceParameterNameInSubstring(s, old, new string, buf *bytes.Buffer, paramRegExp *regexp.Regexp) {
	matchIndices := paramRegExp.FindStringIndex(s)
	prevMatchEnd := 1

	for i := 0; i < len(matchIndices); i += 2 {
		matchStart := matchIndices[i]
		matchEnd := matchIndices[i+1]

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

	quoteIndices := quoteRegExp.FindStringIndex(command)
	prevQuoteEnd := 0

	for i := 0; i < len(quoteIndices); i += 2 {
		quoteStart := quoteIndices[i]
		quoteEnd := quoteIndices[i+1]

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
		command = replaceParameterName(command, p.name, fmt.Sprintf("$%d", i+1))
	}

	return command
}

func newStatement(conn *Conn, command string, params []*Parameter) *Statement {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("newStatement"))
	}

	stmt := new(Statement)

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

// Close closes the Statement, releasing resources on the server.
func (stmt *Statement) Close() (err os.Error) {
	conn := stmt.conn

	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Statement.Close"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = conn.logAndConvertPanic(x)
		}
	}()

	stmt.conn.state.closeStatement(stmt)

	stmt.isClosed = true
	return
}

// ActualCommand returns the actual command text that is sent to the server.
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

// Query executes the Statement and returns a
// ResultSet for row-by-row retrieval of the results.
// The returned ResultSet must be closed before sending another
// query or command to the server over the same connection.
func (stmt *Statement) Query() (res *ResultSet, err os.Error) {
	conn := stmt.conn

	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Statement.Query"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = conn.logAndConvertPanic(x)
		}
	}()

	r := newResultSet(conn)

	conn.state.execute(stmt, r)

	res = r

	return
}

// Execute executes the Statement and returns the number
// of rows affected. If the results of a query are needed, use the
// Query method instead.
func (stmt *Statement) Execute() (rowsAffected int64, err os.Error) {
	conn := stmt.conn

	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Statement.Execute"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = conn.logAndConvertPanic(x)
		}
	}()

	res, err := stmt.Query()
	if err != nil {
		return
	}

	err = res.Close()

	rowsAffected = res.rowsAffected
	return
}

// Scan executes the statement and scans the fields of the first row
// in the ResultSet, trying to store field values into the specified
// arguments. The arguments must be of pointer types. If a row has
// been fetched, fetched will be true, otherwise false.
func (stmt *Statement) Scan(args ...interface{}) (fetched bool, err os.Error) {
	conn := stmt.conn

	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Statement.Scan"))
	}

	defer func() {
		if x := recover(); x != nil {
			err = conn.logAndConvertPanic(x)
		}
	}()

	res, err := stmt.Query()
	if err != nil {
		return
	}
	defer res.Close()

	return res.ScanNext(args)
}
