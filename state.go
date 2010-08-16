// Copyright 2010 Alexander Neumann. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgsql

const invalidOpForStateMsg = "invalid operation for this state"

// state is the interface that all states must implement.
type state interface {
	// code returns the ConnStatus that matches the state.
	code() ConnStatus

	// execute sends Bind and Execute packets to the server.
	execute(stmt *Statement, res *ResultSet)

	// flush sends a Flush packet to the server.
	flush(conn *Conn)

	// prepare sends a Parse packet to the server.
	prepare(stmt *Statement)

	// query sends a Query packet to the server.
	query(conn *Conn, res *ResultSet, sql string)
}


// abstractState can be embedded in any real state struct, so it satisfies
// the state interface without implementing all state methods itself.
type abstractState struct{}

func (abstractState) execute(stmt *Statement, res *ResultSet) {
	panic(invalidOpForStateMsg)
}

func (abstractState) flush(conn *Conn) {
	panic(invalidOpForStateMsg)
}

func (abstractState) prepare(stmt *Statement) {
	panic(invalidOpForStateMsg)
}

func (abstractState) query(conn *Conn, res *ResultSet, sql string) {
	panic(invalidOpForStateMsg)
}
