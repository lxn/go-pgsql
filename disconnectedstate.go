// Copyright 2010 Alexander Neumann. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgsql

import (
	"bufio"
	"fmt"
	"net"
)

// disconnectedState is the initial state before a network connection is established.
type disconnectedState struct {
	abstractState
}

func (disconnectedState) code() ConnStatus {
	return StatusDisconnected
}

func (disconnectedState) connect(conn *Conn) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("disconnectedState.connect"))
	}

	c, err := net.Dial("tcp", "", fmt.Sprintf("%s:%d", conn.params.Host, conn.params.Port))
	if err != nil {
		panic(err)
	}

	err = c.SetReadTimeout(15000000000)
	if err != nil {
		panic(err)
	}

	conn.conn = c

	conn.reader = bufio.NewReader(c)
	conn.writer = bufio.NewWriter(c)

	conn.state = connectedState{}
}
