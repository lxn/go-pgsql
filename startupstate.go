// Copyright 2010 Alexander Neumann. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgsql

// startupState is the state that is active after sending a StartupMessage
// to the PostgreSQL server and before authentication.
type startupState struct {
	abstractState
}

func (startupState) authenticate(conn *Conn, password string) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("startupState.authenticate"))
	}

	msgLen := int32(4 + len(password) + 1)

	conn.writeByte(_PasswordMessage)
	conn.writeInt32(msgLen)
	conn.writeString0(password)

	conn.flush()
}

func (startupState) code() ConnStatus {
	return StatusAuthenticating
}
