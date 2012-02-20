// Copyright 2010 The go-pgsql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgsql

//------------------------------------------------------------------------------

type backendMessageCode byte

const (
	_AuthenticationRequest backendMessageCode = 'R'
	_BackendKeyData        backendMessageCode = 'K'
	_BindComplete          backendMessageCode = '2'
	_CloseComplete         backendMessageCode = '3'
	_CommandComplete       backendMessageCode = 'C'
	_CopyData_BE           backendMessageCode = 'd'
	_CopyDone_BE           backendMessageCode = 'c'
	_CopyInResponse        backendMessageCode = 'G'
	_CopyOutResponse       backendMessageCode = 'H'
	_DataRow               backendMessageCode = 'D'
	_EmptyQueryResponse    backendMessageCode = 'I'
	_ErrorResponse         backendMessageCode = 'E'
	_FunctionCallResponse  backendMessageCode = 'V'
	_NoData                backendMessageCode = 'n'
	_NoticeResponse        backendMessageCode = 'N'
	_NotificationResponse  backendMessageCode = 'A'
	_ParameterDescription  backendMessageCode = 't'
	_ParameterStatus       backendMessageCode = 'S'
	_ParseComplete         backendMessageCode = '1'
	_PortalSuspended       backendMessageCode = 's'
	_ReadyForQuery         backendMessageCode = 'Z'
	_RowDescription        backendMessageCode = 'T'
)

var backendMsgCode2String map[backendMessageCode]string

func (x backendMessageCode) String() string {
	s, ok := backendMsgCode2String[x]
	if !ok {
		return "unkown backendMessageCode"
	}

	return s
}

//------------------------------------------------------------------------------

type frontendMessageCode byte

const (
	_Bind            frontendMessageCode = 'B'
	_Close           frontendMessageCode = 'C'
	_CopyData_FE     frontendMessageCode = 'd'
	_CopyDone_FE     frontendMessageCode = 'c'
	_CopyFail        frontendMessageCode = 'f'
	_Describe        frontendMessageCode = 'D'
	_Execute         frontendMessageCode = 'E'
	_Flush           frontendMessageCode = 'H'
	_FunctionCall    frontendMessageCode = 'F'
	_Parse           frontendMessageCode = 'P'
	_PasswordMessage frontendMessageCode = 'p'
	_Query           frontendMessageCode = 'Q'
	_SSLRequest      frontendMessageCode = '8'
	_Sync            frontendMessageCode = 'S'
	_Terminate       frontendMessageCode = 'X'
)

var frontendMsgCode2String map[frontendMessageCode]string

func (x frontendMessageCode) String() string {
	s, ok := frontendMsgCode2String[x]
	if !ok {
		return "unkown frontendMessageCode"
	}

	return s
}

//------------------------------------------------------------------------------

type authenticationType int32

const (
	_AuthenticationOk                authenticationType = 0
	_AuthenticationKerberosV5        authenticationType = 2
	_AuthenticationCleartextPassword authenticationType = 3
	_AuthenticationMD5Password       authenticationType = 5
	_AuthenticationSCMCredential     authenticationType = 6
	_AuthenticationGSS               authenticationType = 7
	_AuthenticationGSSContinue       authenticationType = 8
	_AuthenticationSSPI              authenticationType = 9
)

var authType2String map[authenticationType]string

func (x authenticationType) String() string {
	s, ok := authType2String[x]
	if !ok {
		return "unkown authenticationType"
	}

	return s
}

//------------------------------------------------------------------------------

func init() {

	backendMsgCode2String = make(map[backendMessageCode]string)

	backendMsgCode2String[_AuthenticationRequest] = "AuthenticationRequest"
	backendMsgCode2String[_BackendKeyData] = "BackendKeyData"
	backendMsgCode2String[_BindComplete] = "BindComplete"
	backendMsgCode2String[_CloseComplete] = "CloseComplete"
	backendMsgCode2String[_CommandComplete] = "CommandComplete"
	backendMsgCode2String[_CopyData_BE] = "CopyData"
	backendMsgCode2String[_CopyDone_BE] = "CopyDone"
	backendMsgCode2String[_CopyInResponse] = "CopyInResponse"
	backendMsgCode2String[_CopyOutResponse] = "CopyOutResponse"
	backendMsgCode2String[_DataRow] = "DataRow"
	backendMsgCode2String[_EmptyQueryResponse] = "EmptyQueryResponse"
	backendMsgCode2String[_ErrorResponse] = "ErrorResponse"
	backendMsgCode2String[_FunctionCallResponse] = "FunctionCallResponse"
	backendMsgCode2String[_NoData] = "NoData"
	backendMsgCode2String[_NoticeResponse] = "NoticeResponse"
	backendMsgCode2String[_NotificationResponse] = "NotificationResponse"
	backendMsgCode2String[_ParameterDescription] = "ParameterDescription"
	backendMsgCode2String[_ParameterStatus] = "ParameterStatus"
	backendMsgCode2String[_ParseComplete] = "ParseComplete"
	backendMsgCode2String[_PortalSuspended] = "PortalSuspended"
	backendMsgCode2String[_ReadyForQuery] = "ReadyForQuery"
	backendMsgCode2String[_RowDescription] = "RowDescription"

	//--------

	frontendMsgCode2String = make(map[frontendMessageCode]string)

	frontendMsgCode2String[_Bind] = "Bind"
	frontendMsgCode2String[_Close] = "Close"
	frontendMsgCode2String[_CopyData_FE] = "CopyData"
	frontendMsgCode2String[_CopyDone_FE] = "CopyDone"
	frontendMsgCode2String[_CopyFail] = "CopyFail"
	frontendMsgCode2String[_Describe] = "Describe"
	frontendMsgCode2String[_Execute] = "Execute"
	frontendMsgCode2String[_Flush] = "Flush"
	frontendMsgCode2String[_FunctionCall] = "FunctionCall"
	frontendMsgCode2String[_Parse] = "Parse"
	frontendMsgCode2String[_PasswordMessage] = "PasswordMessage"
	frontendMsgCode2String[_Query] = "Query"
	frontendMsgCode2String[_SSLRequest] = "SSLRequest"
	frontendMsgCode2String[_Sync] = "Sync"
	frontendMsgCode2String[_Terminate] = "Terminate"

	//--------

	authType2String = make(map[authenticationType]string)

	authType2String[_AuthenticationOk] = "AuthenticationOk"
	authType2String[_AuthenticationKerberosV5] = "AuthenticationKerberosV5"
	authType2String[_AuthenticationCleartextPassword] = "AuthenticationCleartextPassword"
	authType2String[_AuthenticationMD5Password] = "AuthenticationMD5Password"
	authType2String[_AuthenticationSCMCredential] = "AuthenticationSCMCredential"
	authType2String[_AuthenticationGSS] = "AuthenticationGSS"
	authType2String[_AuthenticationGSSContinue] = "AuthenticationGSSContinue"
	authType2String[_AuthenticationSSPI] = "AuthenticationSSPI"
}
