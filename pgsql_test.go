// Copyright 2010 Alexander Neumann. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgsql

import (
	"os"
	"testing"
)

func validParams() *ConnParams {
	return &ConnParams{
		Host:     "127.0.0.1",
		Database: "testdatabase",
		User:     "testuser",
		Password: "testpassword",
	}
}

func openConn() (conn *Conn, err os.Error) {
	conn, err = NewConn(validParams())
	if conn == nil || err != nil {
		return
	}
	return conn, conn.Open()
}

func Test_NewConn_NilParams_ExpectErrNotNil(t *testing.T) {
	_, err := NewConn(nil)
	if err == nil {
		t.Fail()
	}
}

func Test_NewConn_NilParams_ExpectConnNil(t *testing.T) {
	conn, _ := NewConn(nil)
	if conn != nil {
		t.Fail()
	}
}

func Test_ConnOpen_ValidParams_ExpectErrNil(t *testing.T) {
	conn, err := openConn()
	if err != nil {
		t.Fail()
	}
	if conn != nil {
		conn.Close()
	}
}

func Test_ConnOpen_InvalidPassword_ExpectErrNotNil(t *testing.T) {
	params := validParams()
	params.Password = "wrongpassword"

	conn, _ := NewConn(params)
	if conn == nil {
		t.Error("conn == nil")
		return
	}

	err := conn.Open()
	if err == nil {
		conn.Close()
		t.Fail()
	}
}

func doSimpleQueryReaderTest(t *testing.T, test func(reader *Reader) (have, want interface{}, name string)) {
	conn, err := openConn()
	if err != nil {
		t.Error("openConn: err != nil")
		return
	}
	if conn == nil {
		t.Error("openConn: conn == nil")
		return
	}
	defer conn.Close()

	reader, err := conn.Query("SELECT 1 AS _1, 'two' AS _two, true AS _true, null AS _null, 4.5 AS _4_5;")
	if err != nil {
		t.Error("conn.Query: err != nil")
		return
	}
	if reader == nil {
		t.Error("conn.Query: reader == nil")
		return
	}
	defer reader.Close()

	if have, want, name := test(reader); have != want {
		t.Errorf("%s failed - have: '%v', but want '%v'", name, have, want)
	}
}

func Test_DoSimpleQueryReaderTests(t *testing.T) {
	tests := []func(reader *Reader) (have, want interface{}, name string){
		// Basic reader tests
		func(reader *Reader) (have, want interface{}, name string) {
			hasRow, _ := reader.ReadNext()
			return hasRow, true, "ReadNext"
		},
		func(reader *Reader) (have, want interface{}, name string) {
			hasRow, _ := reader.ReadNext()
			hasRow, _ = reader.ReadNext()
			return hasRow, false, "ReadNext_RetValSecondCall"
		},
		func(reader *Reader) (have, want interface{}, name string) {
			_, err := reader.ReadNext()
			return err == nil, true, "ReadNext_ErrNil"
		},

		// Get value tests
		func(reader *Reader) (have, want interface{}, name string) {
			reader.ReadNext()
			val, _, _ := reader.Int32(0)
			return val, int32(1), "field #0"
		},
		func(reader *Reader) (have, want interface{}, name string) {
			reader.ReadNext()
			val, _, _ := reader.String(1)
			return val, "two", "field #1"
		},
		func(reader *Reader) (have, want interface{}, name string) {
			reader.ReadNext()
			val, _, _ := reader.Bool(2)
			return val, true, "field #2"
		},
		func(reader *Reader) (have, want interface{}, name string) {
			reader.ReadNext()
			val, _ := reader.IsNull(3)
			return val, true, "field #3 is null"
		},
		func(reader *Reader) (have, want interface{}, name string) {
			reader.ReadNext()
			val, _, _ := reader.Float64(4)
			return val, float64(4.5), "field #4"
		},
	}

	for _, test := range tests {
		doSimpleQueryReaderTest(t, test)
	}
}
