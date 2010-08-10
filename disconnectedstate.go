// Copyright 2010 Alexander Neumann. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgsql

// disconnectedState is the initial state before a connection is established.
type disconnectedState struct {
	abstractState
}

func (disconnectedState) code() ConnStatus {
	return StatusDisconnected
}
