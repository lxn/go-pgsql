// Copyright 2010 Alexander Neumann. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgsql

// connectedState is the state that is active after a network connection is
// established and before the startup phase begins.
type connectedState struct {
	abstractState
}

func (s connectedState) startup(conn *Conn) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("connectedState.startup"))
	}

	msglen := int32(4 + 4 +
		len("user") + 1 + len(conn.params.User) + 1 +
		len("database") + 1 + len(conn.params.Database) + 1 + 1)

	conn.writeInt32(msglen)

	// For now we only support protocol version 3.0.
	conn.writeInt32(3 << 16)

	conn.writeString0("user")
	conn.writeString0(conn.params.User)

	conn.writeString0("database")
	conn.writeString0(conn.params.Database)

	conn.writeByte(0)

	conn.flush()

	s.processBackendMessages(conn, nil)
}

func (connectedState) code() ConnStatus {
	return StatusConnected
}
