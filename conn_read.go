// Copyright 2010 Alexander Neumann. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgsql

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

func (conn *Conn) read(b []byte) {
	_, err := conn.reader.Read(b)
	if err != nil {
		panic(fmt.Sprintf("read failed: %s", err))
	}
}

func (conn *Conn) readByte() byte {
	b, err := conn.reader.ReadByte()
	if err != nil {
		panic(fmt.Sprintf("readByte failed: %s", err))
	}

	return b
}

func (conn *Conn) readBytes(delim byte) []byte {
	b, err := conn.reader.ReadBytes(delim)
	if err != nil {
		panic(fmt.Sprintf("readBytes failed: %s", err))
	}

	return b
}

func (conn *Conn) readInt16() int16 {
	var buf [2]byte
	b := buf[0:]

	conn.read(b)
	return int16(binary.BigEndian.Uint16(b))
}

func (conn *Conn) readInt32() int32 {
	var buf [4]byte
	b := buf[0:]

	conn.read(b)
	return int32(binary.BigEndian.Uint32(b))
}

func (conn *Conn) readString() string {
	b := conn.readBytes(0)
	return string(b[0 : len(b)-1])
}

func (conn *Conn) readDataRow(res *ResultSet) {
	// Just eat message length.
	conn.readInt32()

	fieldCount := conn.readInt16()

	var ord int16
	for ord = 0; ord < fieldCount; ord++ {
		valLen := conn.readInt32()

		var val []byte

		if valLen == -1 {
			val = nil
		} else {
			val = make([]byte, valLen)
			conn.read(val)
		}

		res.values[ord] = val
	}
}

func (conn *Conn) readRowDescription(res *ResultSet) {
	// Just eat message length.
	conn.readInt32()

	fieldCount := conn.readInt16()

	res.fields = make([]field, fieldCount)
	res.values = make([][]byte, fieldCount)

	var ord int16
	for ord = 0; ord < fieldCount; ord++ {
		res.fields[ord].name = conn.readString()

		// Just eat table OID.
		conn.readInt32()

		// Just eat field OID.
		conn.readInt16()

		// Just eat field data type OID.
		conn.readInt32()

		// Just eat field size.
		conn.readInt16()

		// Just eat field type modifier.
		conn.readInt32()

		format := fieldFormat(conn.readInt16())
		switch format {
		case textFormat:
		case binaryFormat:
		default:
			panic("unsupported field format")
		}
		res.fields[ord].format = format
	}
}

func (conn *Conn) readAuthenticationRequest() {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.readAuthenticationRequest"))
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

		conn.writePasswordMessage(password)

		//		case _AuthenticationSCMCredential:

		//		case _AuthenticationGSS:

		//		case _AuthenticationGSSContinue:

		//		case _AuthenticationSSPI:

	default:
		panic(fmt.Sprintf("unsupported authentication type: %d", authType))
	}
}

func (conn *Conn) readBackendKeyData() {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.readBackendKeyData"))
	}

	// Just eat message length.
	conn.readInt32()

	conn.backendPID = conn.readInt32()
	conn.backendSecretKey = conn.readInt32()
}

func (conn *Conn) readBindComplete() {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.readBindComplete"))
	}

	// Just eat message length.
	conn.readInt32()
}

func (conn *Conn) readCloseComplete() {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.readCloseComplete"))
	}

	// Just eat message length.
	conn.readInt32()
}

func (conn *Conn) readCommandComplete(res *ResultSet) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.readCommandComplete"))
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

func (conn *Conn) readEmptyQueryResponse() {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.readEmptyQueryResponse"))
	}

	// Just eat message length.
	conn.readInt32()
}

func (conn *Conn) readErrorOrNoticeResponse(isError bool) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.readErrorOrNoticeResponse"))
	}

	// Just eat message length.
	conn.readInt32()

	err := new(Error)

	// Read all fields, just ignore unknown ones.
	for {
		fieldType := conn.readByte()

		if fieldType == 0 {
			if isError {
				if !conn.onErrorDontRequireReadyForQuery {
					// Before panicking, we have to wait for a ReadyForQuery message.
					conn.readBackendMessages(nil)
				}

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

func (conn *Conn) readParameterStatus() {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.readParameterStatus"))
	}

	// Just eat message length.
	conn.readInt32()

	name := conn.readString()
	value := conn.readString()

	if conn.LogLevel >= LogDebug {
		conn.logf(LogDebug, "ParameterStatus: Name: '%s', Value: '%s'", name, value)
	}
}

func (conn *Conn) readParseComplete() {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.readParseComplete"))
	}

	// Just eat message length.
	conn.readInt32()
}

func (conn *Conn) readReadyForQuery(res *ResultSet) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.readReadyForQuery"))
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

func (conn *Conn) readBackendMessages(res *ResultSet) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.readBackendMessages"))
	}

	for {
		msgCode := backendMessageCode(conn.readByte())

		if conn.LogLevel >= LogDebug {
			conn.logf(LogDebug, "received '%s' backend message", msgCode)
		}

		switch msgCode {
		case _AuthenticationRequest:
			conn.readAuthenticationRequest()

		case _BackendKeyData:
			conn.readBackendKeyData()

		case _BindComplete:
			conn.readBindComplete()
			return

		case _CloseComplete:
			conn.readCloseComplete()

		case _CommandComplete:
			conn.readCommandComplete(res)
			return

		case _DataRow:
			res.readRow()
			return

		case _EmptyQueryResponse:
			conn.readEmptyQueryResponse()

		case _ErrorResponse:
			conn.readErrorOrNoticeResponse(true)

		case _NoticeResponse:
			conn.readErrorOrNoticeResponse(false)

		case _ParameterStatus:
			conn.readParameterStatus()

		case _ParseComplete:
			conn.readParseComplete()
			return

		case _ReadyForQuery:
			conn.readReadyForQuery(res)
			return

		case _RowDescription:
			res.initializeResult()
			return
		}
	}
}
