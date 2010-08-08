// Copyright 2010 Alexander Neumann. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgsql

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

const invalidOpForStateMsg = "invalid operation for this state"

// state is the interface that all states must implement.
type state interface {
	// authenticate sends a PasswordMessage packet to the server.
	authenticate(conn *Conn, password string)

	// closePortal sends a Close packet for the statements current portal to the server.
	closePortal(stmt *Statement)

	// closeStatement sends a Close packet for a statement to the server.
	closeStatement(stmt *Statement)

	// code returns the ConnStatus that matches the state.
	code() ConnStatus

	// connect establishes a network connection to the server.
	connect(conn *Conn)

	// disconnect sends a Terminate packet to the server and closes the network connection.
	disconnect(conn *Conn)

	// execute sends Bind and Execute packets to the server.
	execute(stmt *Statement, res *ResultSet)

	// flush sends a Flush packet to the server.
	flush(conn *Conn)

	// prepare sends a Parse packet to the server.
	prepare(stmt *Statement)

	// processBackendMessages processes messages from the server.
	processBackendMessages(conn *Conn, res *ResultSet)

	// query sends a Query packet to the server.
	query(conn *Conn, res *ResultSet, sql string)

	// startup sends a StartupMessage packet to the server.
	startup(conn *Conn)
}


// abstractState can be embedded in any real state struct, so it satisfies
// the state interface without implementing all state methods itself.
type abstractState struct{}

func (abstractState) authenticate(conn *Conn, password string) {
	panic(invalidOpForStateMsg)
}

func (abstractState) closePortal(stmt *Statement) {
	panic(invalidOpForStateMsg)
}

func (abstractState) closeStatement(stmt *Statement) {
	panic(invalidOpForStateMsg)
}

func (abstractState) code() ConnStatus {
	panic(invalidOpForStateMsg)
}

func (abstractState) connect(conn *Conn) {
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

func (abstractState) startup(conn *Conn) {
	panic(invalidOpForStateMsg)
}

func (state abstractState) close(conn *Conn, itemType byte, itemName string) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("abstractState.close"))
	}

	msgLen := int32(4 + 1 + len(itemName) + 1)

	conn.writeFrontendMessageCode(_Close)
	conn.writeInt32(msgLen)
	conn.writeByte(itemType)
	conn.writeString0(itemName)

	conn.flush()
}

func (abstractState) processAuthenticationRequest(conn *Conn) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("abstractState.processAuthenticationRequest"))
	}

	// Just eat message length.
	conn.readInt32()

	authType := conn.readInt32()
	switch authenticationType(authType) {
	case _AuthenticationOk:
		// nop

		//		case _AuthenticationKerberosV5 authenticationType:

		//		case _AuthenticationCleartextPassword:

	case _AuthenticationMD5Password:
		salt := make([]byte, 4)

		conn.read(salt)

		md5Hasher := md5.New()

		_, err := md5Hasher.Write([]byte(conn.params.Password))
		if err != nil {
			panic("md5Hasher.Write failed")
		}
		_, err = md5Hasher.Write([]byte(conn.params.User))
		if err != nil {
			panic("md5Hasher.Write failed")
		}

		md5HashHex1 := hex.EncodeToString(md5Hasher.Sum())

		md5Hasher.Reset()

		_, err = md5Hasher.Write([]byte(md5HashHex1))
		if err != nil {
			panic("md5Hasher.Write failed")
		}
		_, err = md5Hasher.Write(salt)
		if err != nil {
			panic("md5Hasher.Write failed")
		}

		md5HashHex2 := hex.EncodeToString(md5Hasher.Sum())

		password := "md5" + md5HashHex2

		conn.state = startupState{}
		conn.state.authenticate(conn, password)

		//		case _AuthenticationSCMCredential:

		//		case _AuthenticationGSS:

		//		case _AuthenticationGSSContinue:

		//		case _AuthenticationSSPI:

	default:
		panic(fmt.Sprintf("unsupported authentication type: %d", authType))
	}
}

func (abstractState) processBackendKeyData(conn *Conn) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("abstractState.processBackendKeyData"))
	}

	// Just eat message length.
	conn.readInt32()

	conn.backendPID = conn.readInt32()
	conn.backendSecretKey = conn.readInt32()
}

func (abstractState) processBindComplete(conn *Conn) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("abstractState.processBindComplete"))
	}

	// Just eat message length.
	conn.readInt32()
}

func (abstractState) processCloseComplete(conn *Conn) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("abstractState.processCloseComplete"))
	}

	// Just eat message length.
	conn.readInt32()
}

func (abstractState) processCommandComplete(conn *Conn, res *ResultSet) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("abstractState.processCommandComplete"))
	}

	// Just eat message length.
	conn.readInt32()

	// Retrieve the number of affected rows from the command tag.
	tag := conn.readString()

	if res != nil {
		parts := strings.Split(tag, " ", -1)

		rowsAffected, err := strconv.Atoi64(parts[len(parts)-1])
		if err != nil {
			if conn.LogLevel >= LogWarning {
				conn.log(LogWarning, "failed to retrieve affected row count")
			}
		}

		res.rowsAffected = rowsAffected
		res.currentResultComplete = true
	}
}

func (abstractState) processEmptyQueryResponse(conn *Conn) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("abstractState.processEmptyQueryResponse"))
	}

	// Just eat message length.
	conn.readInt32()
}

func (state abstractState) processErrorOrNoticeResponse(conn *Conn, isError bool) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("abstractState.processErrorOrNoticeResponse"))
	}

	// Just eat message length.
	conn.readInt32()

	err := new(Error)

	// Read all fields, just ignore unknown ones.
	for {
		fieldType := conn.readByte()

		if fieldType == 0 {
			if isError {
				// Before panicking, we have to wait for a ReadyForQuery message.
				state.processBackendMessages(conn, nil)

				// We panic with our error as parameter, so the right thing (TM) will happen.
				panic(err)
			} else {
				// For now, we just log notices.
				conn.logError(LogDebug, err)
				return
			}
		}

		str := conn.readString()

		switch fieldType {
		case 'S':
			err.severity = str

		case 'C':
			err.code = str

		case 'M':
			err.message = str

		case 'D':
			err.detail = str

		case 'H':
			err.hint = str

		case 'P':
			err.position = str

		case 'p':
			err.internalPosition = str

		case 'q':
			err.internalQuery = str

		case 'W':
			err.where = str

		case 'F':
			err.file = str

		case 'L':
			err.line = str

		case 'R':
			err.routine = str
		}
	}
}

func (abstractState) processParameterStatus(conn *Conn) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("abstractState.processParameterStatus"))
	}

	// Just eat message length.
	conn.readInt32()

	name := conn.readString()
	value := conn.readString()

	if conn.LogLevel >= LogDebug {
		conn.logf(LogDebug, "ParameterStatus: Name: '%s', Value: '%s'", name, value)
	}
}

func (abstractState) processParseComplete(conn *Conn) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("abstractState.processParseComplete"))
	}

	// Just eat message length.
	conn.readInt32()
}

func (abstractState) processReadyForQuery(conn *Conn, res *ResultSet) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("abstractState.processReadyForQuery"))
	}

	// Just eat message length.
	conn.readInt32()

	txStatus := conn.readByte()

	if conn.LogLevel >= LogDebug {
		conn.log(LogDebug, "Transaction Status: ", string([]byte{txStatus}))
	}

	// TODO: Find out if and how we should handle these.
	/*	switch txStatus {
		case 'I':

		case 'T':

		case 'E':

		default:
			panic("unknown transaction status")
	}*/

	if res != nil {
		res.allResultsComplete = true
	}

	conn.state = readyState{}
}

func (state abstractState) processBackendMessages(conn *Conn, res *ResultSet) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("abstractState.processBackendMessages"))
	}

	for {
		msgCode := backendMessageCode(conn.readByte())

		if conn.LogLevel >= LogDebug {
			conn.logf(LogDebug, "received '%s' backend message", msgCode)
		}

		switch msgCode {
		case _AuthenticationRequest:
			state.processAuthenticationRequest(conn)

		case _BackendKeyData:
			state.processBackendKeyData(conn)

		case _BindComplete:
			state.processBindComplete(conn)
			return

		case _CloseComplete:
			state.processCloseComplete(conn)

		case _CommandComplete:
			state.processCommandComplete(conn, res)
			return

		case _DataRow:
			res.readRow()
			return

		case _EmptyQueryResponse:
			state.processEmptyQueryResponse(conn)

		case _ErrorResponse:
			state.processErrorOrNoticeResponse(conn, true)

		case _NoticeResponse:
			state.processErrorOrNoticeResponse(conn, false)

		case _ParameterStatus:
			state.processParameterStatus(conn)

		case _ParseComplete:
			state.processParseComplete(conn)
			return

		case _ReadyForQuery:
			state.processReadyForQuery(conn, res)
			return

		case _RowDescription:
			res.initializeResult()
			return
		}
	}
}
