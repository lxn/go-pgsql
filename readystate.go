// Copyright 2010 Alexander Neumann. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgsql

import (
	"strconv"
)

// readyState is the state that is active when the connection to the
// PostgreSQL server is ready for queries.
type readyState struct {
	abstractState
}

func (state readyState) closeStatement(stmt *Statement) {
	conn := stmt.conn

	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("readyState.closeStatement"))
	}

	state.close(conn, 'S', stmt.name)
}

func (readyState) code() ConnStatus {
	return StatusReady
}

func (readyState) disconnect(conn *Conn) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("readyState.disconnect"))
	}

	conn.writeFrontendMessageCode(_Terminate)
	conn.writeInt32(4)

	conn.flush()

	conn.state = connectedState{}

	err := conn.conn.Close()
	if err != nil {
		panic(err)
	}

	conn.state = disconnectedState{}
}

func (state readyState) execute(stmt *Statement, reader *Reader) {
	conn := stmt.conn

	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("readyState.execute"))
	}

	values := make([]string, len(stmt.params))

	// Send Bind packet to server.
	var paramValuesLen int
	for i, param := range stmt.params {
		switch val := param.value.(type) {
		case bool:
			if val {
				values[i] = "t"
			} else {
				values[i] = "f"
			}

		case byte:
			values[i] = string([]byte{val})

		case float:
			values[i] = strconv.Ftoa(val, 'f', -1)

		case float32:
			values[i] = strconv.Ftoa32(val, 'f', -1)

		case float64:
			values[i] = strconv.Ftoa64(val, 'f', -1)

		case int:
			values[i] = strconv.Itoa(val)

		case int16:
			values[i] = strconv.Itoa(int(val))

		case int32:
			values[i] = strconv.Itoa(int(val))

		case int64:
			values[i] = strconv.Itoa64(val)

		case nil:

		case string:
			values[i] = val

		default:
			panic("unsupported parameter type")
		}

		paramValuesLen += len(values[i])
	}

	msgLen := int32(4 +
		len(stmt.portalName) + 1 +
		len(stmt.name) + 1 +
		2 + 2 +
		2 + len(stmt.params)*4 + paramValuesLen +
		2 + 2)

	conn.writeFrontendMessageCode(_Bind)
	conn.writeInt32(msgLen)
	conn.writeString0(stmt.portalName)
	conn.writeString0(stmt.name)
	conn.writeInt16(1)
	conn.writeInt16(int16(textFormat))
	conn.writeInt16(int16(len(stmt.params)))

	for i, param := range stmt.params {
		if param.value == nil {
			conn.writeInt32(-1)
		} else {
			conn.writeInt32(int32(len(values[i])))
			conn.writeString(values[i])
		}
	}

	conn.writeInt16(1)
	conn.writeInt16(binaryFormat)

	state.flush(conn)
	state.processBackendMessages(conn, reader)

	// Send Describe packet to server.
	msgLen = int32(4 + 1 + len(stmt.portalName) + 1)

	conn.writeFrontendMessageCode(_Describe)
	conn.writeInt32(msgLen)
	conn.writeByte('P')
	conn.writeString0(stmt.portalName)

	state.flush(conn)
	state.processBackendMessages(conn, reader)

	// Send Execute packet to server.
	msgLen = int32(4 + len(stmt.portalName) + 1 + 4)

	conn.writeFrontendMessageCode(_Execute)
	conn.writeInt32(msgLen)
	conn.writeString0(stmt.portalName)
	conn.writeInt32(0)

	state.flush(conn)

	// Send Sync packet to server.
	conn.writeFrontendMessageCode(_Sync)
	conn.writeInt32(4)

	state.flush(conn)

	conn.state = processingQueryState{}
}

func (readyState) flush(conn *Conn) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("readyState.flush"))
	}

	conn.writeFrontendMessageCode(_Flush)
	conn.writeInt32(4)

	conn.flush()
}

func (state readyState) prepare(stmt *Statement) {
	conn := stmt.conn

	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("readyState.prepare"))
	}

	msgLen := int32(4 +
		len(stmt.name) + 1 +
		len(stmt.actualCommand) + 1 +
		2 + len(stmt.params)*4)

	conn.writeFrontendMessageCode(_Parse)
	conn.writeInt32(msgLen)
	conn.writeString0(stmt.name)
	conn.writeString0(stmt.actualCommand)

	conn.writeInt16(int16(len(stmt.params)))
	for _, param := range stmt.params {
		conn.writeInt32(int32(param.typ))
	}

	state.flush(conn)

	state.processBackendMessages(conn, nil)
}

func (state readyState) query(conn *Conn, reader *Reader, command string) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("readyState.query"))
	}

	conn.writeFrontendMessageCode(_Query)
	conn.writeInt32(int32(4 + len(command) + 1))
	conn.writeString0(command)

	conn.flush()

	state.processBackendMessages(conn, reader)
	
	conn.state = processingQueryState{}
}
