// Copyright 2010 Alexander Neumann. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgsql

// processingQueryState is the state that is active when
// the results of a query are being processed.
type processingQueryState struct {
	abstractState
}

func (processingQueryState) closePortal(stmt *Statement) {
	conn := stmt.conn

	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("processingQueryState.closePortal"))
	}

	conn.writeClose('P', stmt.portalName)
}

func (processingQueryState) code() ConnStatus {
	return StatusProcessingQuery
}
