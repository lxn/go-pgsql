// Copyright 2010 Alexander Neumann. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgsql

// readyState is the state that is active when the connection to the
// PostgreSQL server is ready for queries.
type readyState struct {
	abstractState
}

func (readyState) closeStatement(stmt *Statement) {
	conn := stmt.conn

	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("readyState.closeStatement"))
	}

	conn.writeClose('S', stmt.name)
}

func (readyState) code() ConnStatus {
	return StatusReady
}

func (readyState) disconnect(conn *Conn) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("readyState.disconnect"))
	}

	conn.writeTerminate()

	err := conn.tcpConn.Close()
	if err != nil {
		panic(err)
	}

	conn.state = disconnectedState{}
}

func (readyState) execute(stmt *Statement, res *ResultSet) {
	conn := stmt.conn

	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("readyState.execute"))
	}

	conn.writeBind(stmt)

	conn.readBackendMessages(res)

	conn.writeDescribe(stmt)

	conn.readBackendMessages(res)

	conn.writeExecute(stmt)

	conn.writeSync()

	conn.state = processingQueryState{}
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

func (readyState) query(conn *Conn, res *ResultSet, command string) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("readyState.query"))
	}

	conn.writeQuery(command)

	conn.readBackendMessages(res)

	conn.state = processingQueryState{}
}
