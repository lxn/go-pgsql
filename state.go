// Copyright 2010 The go-pgsql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgsql

const invalidOpForStateMsg = "invalid operation for this state"

// state is the interface that all states must implement.
type state interface {
	// code returns the ConnStatus that matches the state.
	code() ConnStatus

	// execute sends Bind and Execute packets to the server.
	execute(stmt *Statement, rs *ResultSet)

	// flush sends a Flush packet to the server.
	flush(conn *Conn)

	// prepare sends a Parse packet to the server.
	prepare(stmt *Statement)

	// query sends a Query packet to the server.
	query(conn *Conn, rs *ResultSet, sql string)
}

// abstractState can be embedded in any real state struct, so it satisfies
// the state interface without implementing all state methods itself.
type abstractState struct{}

func (abstractState) execute(stmt *Statement, rs *ResultSet) {
	panic(invalidOpForStateMsg)
}

func (abstractState) flush(conn *Conn) {
	panic(invalidOpForStateMsg)
}

func (abstractState) prepare(stmt *Statement) {
	panic(invalidOpForStateMsg)
}

func (abstractState) query(conn *Conn, rs *ResultSet, sql string) {
	panic(invalidOpForStateMsg)
}

// copyState is the state that is active when the connection is used
// to exchange CopyData messages for bulk import/export.
type copyState struct {
	abstractState
}

func (copyState) code() ConnStatus {
	return StatusCopy
}

// disconnectedState is the initial state before a connection is established.
type disconnectedState struct {
	abstractState
}

func (disconnectedState) code() ConnStatus {
	return StatusDisconnected
}

// processingQueryState is the state that is active when
// the results of a query are being processed.
type processingQueryState struct {
	abstractState
}

func (processingQueryState) code() ConnStatus {
	return StatusProcessingQuery
}

// readyState is the state that is active when the connection to the
// PostgreSQL server is ready for queries.
type readyState struct {
	abstractState
}

func (readyState) code() ConnStatus {
	return StatusReady
}

func (readyState) execute(stmt *Statement, rs *ResultSet) {
	conn := stmt.conn

	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("readyState.execute"))
	}

	succeeded := false
	conn.onErrorDontRequireReadyForQuery = true
	defer func() {
		conn.onErrorDontRequireReadyForQuery = false

		if !succeeded {
			conn.writeSync()

			conn.readBackendMessages(nil)
		}
	}()

	conn.writeBind(stmt)

	conn.readBackendMessages(rs)

	conn.writeDescribe(stmt)

	conn.readBackendMessages(rs)

	conn.writeExecute(stmt)

	conn.writeSync()

	conn.state = processingQueryState{}

	succeeded = true
}

func (readyState) prepare(stmt *Statement) {
	conn := stmt.conn

	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("readyState.prepare"))
	}

	conn.writeParse(stmt)

	conn.onErrorDontRequireReadyForQuery = true
	defer func() { conn.onErrorDontRequireReadyForQuery = false }()

	conn.readBackendMessages(nil)
}

func (readyState) query(conn *Conn, rs *ResultSet, command string) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("readyState.query"))
	}

	conn.writeQuery(command)

	conn.readBackendMessages(rs)

	conn.state = processingQueryState{}
}
