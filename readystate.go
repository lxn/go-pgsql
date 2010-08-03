// Copyright 2010 Alexander Neumann. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgsql

// readyState is the state that is active when the connection to the
// PostgreSQL server is ready for queries.
type readyState struct {
	abstractState
}

func (readyState) code() ConnStatus {
	return StatusReady
}

func (readyState) disconnect(conn *Conn) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("readyState.disconnect"))
	}

	conn.writeByte(_Terminate)
	conn.writeInt32(4)

	conn.flush()

	conn.state = connectedState{}

	err := conn.conn.Close()
	if err != nil {
		conn.logError(LogError, err)
		return
	}

	conn.state = disconnectedState{}
}

func (state readyState) query(conn *Conn, reader *Reader, sql string) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("readyState.query"))
	}

	conn.writeByte(_Query)
	conn.writeInt32(int32(4 + len(sql) + 1))
	conn.writeString0(sql)

	conn.flush()

	conn.state = processingQueryState{}

	state.processBackendMessages(conn, reader)
}
