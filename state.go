// Copyright 2010 Alexander Neumann. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgsql

const invalidOpForStateMsg = "invalid operation for this state"

// state is the interface that all states must implement.
type state interface {
	// closePortal sends a Close packet for the statements current portal to the server.
	closePortal(stmt *Statement)

	// closeStatement sends a Close packet for a statement to the server.
	closeStatement(stmt *Statement)

	// code returns the ConnStatus that matches the state.
	code() ConnStatus

	// disconnect sends a Terminate packet to the server and closes the network connection.
	disconnect(conn *Conn)

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

func (abstractState) closePortal(stmt *Statement) {
	panic(invalidOpForStateMsg)
}

func (abstractState) closeStatement(stmt *Statement) {
	panic(invalidOpForStateMsg)
}

func (abstractState) disconnect(conn *Conn) {
	panic(invalidOpForStateMsg)
}

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
