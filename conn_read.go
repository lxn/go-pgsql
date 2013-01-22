// Copyright 2010 The go-pgsql Authors. All rights reserved.
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
	readTotal := 0
	for {
		n, err := conn.reader.Read(b[readTotal:])
		panicIfErr(err)

		readTotal += n
		if readTotal == len(b) {
			break
		}
	}
}

func (conn *Conn) readByte() byte {
	b, err := conn.reader.ReadByte()
	panicIfErr(err)

	return b
}

func (conn *Conn) readBytes(delim byte) []byte {
	b, err := conn.reader.ReadBytes(delim)
	panicIfErr(err)

	return b
}

func (conn *Conn) readInt16() int16 {
	var buf [2]byte
	b := buf[:]

	conn.read(b)
	return int16(binary.BigEndian.Uint16(b))
}

func (conn *Conn) readInt32() int32 {
	var buf [4]byte
	b := buf[:]

	conn.read(b)
	return int32(binary.BigEndian.Uint32(b))
}

func (conn *Conn) readString() string {
	b := conn.readBytes(0)
	return string(b[:len(b)-1])
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
		panicIfErr(err)

		_, err = md5Hasher.Write([]byte(conn.params.User))
		panicIfErr(err)

		md5HashHex1 := hex.EncodeToString(md5Hasher.Sum(nil))

		md5Hasher.Reset()

		_, err = md5Hasher.Write([]byte(md5HashHex1))
		panicIfErr(err)

		_, err = md5Hasher.Write(salt)
		panicIfErr(err)

		md5HashHex2 := hex.EncodeToString(md5Hasher.Sum(nil))

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

func (conn *Conn) readCommandComplete(rs *ResultSet) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.readCommandComplete"))
	}

	// Just eat message length.
	conn.readInt32()

	// Retrieve the number of affected rows from the command tag.
	tag := conn.readString()

	if rs != nil {
		parts := strings.Split(tag, " ")

		rs.rowsAffected, _ = strconv.ParseInt(parts[len(parts)-1], 10, 64)
		rs.currentResultComplete = true
	}
}

// As of PostgreSQL 9.2 (protocol 3.0), CopyOutResponse and CopyBothResponse
// are exactly the same.
func (conn *Conn) readCopyInResponse() {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.readCopyInResponse"))
	}

	// Just eat message length.
	conn.readInt32()

	// Just eat overall COPY format. 0 - textual, 1 - binary.
	conn.readByte()

	numColumns := conn.readInt16()
	for i := int16(0); i < numColumns; i++ {
		// Just eat column formats.
		conn.readInt16()
	}

	conn.state = copyState{}
}

func (conn *Conn) readDataRow(rs *ResultSet) {
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

		rs.values[ord] = val
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

	err := &Error{}

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

func (conn *Conn) readNoData() {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.readNoData"))
	}

	// Just eat message length.
	conn.readInt32()
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

	conn.runtimeParameters[name] = value

	if name == "DateStyle" {
		conn.updateTimeFormats()
	}
}

func (conn *Conn) readParseComplete() {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.readParseComplete"))
	}

	// Just eat message length.
	conn.readInt32()
}

func (conn *Conn) readReadyForQuery(rs *ResultSet) {
	if conn.LogLevel >= LogDebug {
		defer conn.logExit(conn.logEnter("*Conn.readReadyForQuery"))
	}

	// Just eat message length.
	conn.readInt32()

	txStatus := conn.readByte()

	if conn.LogLevel >= LogDebug {
		conn.log(LogDebug, "Transaction Status: ", string([]byte{txStatus}))
	}

	conn.transactionStatus = TransactionStatus(txStatus)

	if rs != nil {
		rs.allResultsComplete = true
	}

	conn.state = readyState{}
}

func (conn *Conn) readRowDescription(rs *ResultSet) {
	// Just eat message length.
	conn.readInt32()

	fieldCount := conn.readInt16()

	rs.fields = make([]field, fieldCount)
	rs.values = make([][]byte, fieldCount)

	var ord int16
	for ord = 0; ord < fieldCount; ord++ {
		rs.fields[ord].name = conn.readString()

		// Just eat table OID.
		conn.readInt32()

		// Just eat field OID.
		conn.readInt16()

		rs.fields[ord].typeOID = conn.readInt32()

		// Just eat field size.
		conn.readInt16()

		// Just eat field type modifier.
		conn.readInt32()

		format := fieldFormat(conn.readInt16())
		switch format {
		case textFormat, binaryFormat:
			// nop

		default:
			panic("unsupported field format")
		}
		rs.fields[ord].format = format
	}
}

func (conn *Conn) readBackendMessages(rs *ResultSet) {
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
			conn.readCommandComplete(rs)
			return

		case _CopyInResponse:
			conn.readCopyInResponse()
			return

		case _DataRow:
			rs.readRow()
			return

		case _EmptyQueryResponse:
			conn.readEmptyQueryResponse()

		case _ErrorResponse:
			conn.readErrorOrNoticeResponse(true)

		case _NoData:
			conn.readNoData()
			return

		case _NoticeResponse:
			conn.readErrorOrNoticeResponse(false)

		case _ParameterStatus:
			conn.readParameterStatus()

		case _ParseComplete:
			conn.readParseComplete()
			return

		case _ReadyForQuery:
			conn.readReadyForQuery(rs)
			return

		case _RowDescription:
			rs.initializeResult()
			return
		}
	}
}
