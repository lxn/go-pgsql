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

func withConn(t *testing.T, f func(conn *Conn)) {
	conn, err := openConn()
	if err != nil {
		t.Error("withConn: openConn:", err)
		return
	}
	if conn == nil {
		t.Error("withConn: openConn: conn == nil")
		return
	}
	defer conn.Close()

	f(conn)
}

func withSimpleQueryReader(t *testing.T, command string, f func(reader *Reader)) {
	withConn(t, func(conn *Conn) {
		reader, err := conn.Query(command)
		if err != nil {
			t.Error("withSimpleQueryReader: conn.Query:", err)
			return
		}
		if reader == nil {
			t.Error("withSimpleQueryReader: conn.Query: reader == nil")
			return
		}
		defer reader.Close()

		f(reader)
	})
}

func withStatement(t *testing.T, command string, params []*Parameter, f func(stmt *Statement)) {
	withConn(t, func(conn *Conn) {
		stmt, err := conn.Prepare(command, params)
		if err != nil {
			t.Error("withStatement: conn.Prepare:", err)
			return
		}
		if stmt == nil {
			t.Error("withStatement: conn.Prepare: stmt == nil")
			return
		}
		defer stmt.Close()

		f(stmt)
	})
}

func withStatementReader(t *testing.T, command string, params []*Parameter, f func(reader *Reader)) {
	withStatement(t, command, params, func(stmt *Statement) {
		reader, err := stmt.Query()
		if err != nil {
			t.Error("withStatementReader: stmt.Query:", err)
			return
		}
		if reader == nil {
			t.Error("withStatementReader: stmt.Query: reader == nil")
			return
		}
		defer reader.Close()

		f(reader)
	})
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
	withSimpleQueryReader(t, "SELECT 1 AS _1, 'two' AS _two, true AS _true, null AS _null, 4.5 AS _4_5;", func(reader *Reader) {
		if have, want, name := test(reader); have != want {
			t.Errorf("%s failed - have: '%v', but want '%v'", name, have, want)
		}
	})
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

func Test_SimpleQuery_MultipleSelects(t *testing.T) {
	tests := []func(reader *Reader) (have, want interface{}, name string){
		// First result
		func(reader *Reader) (have, want interface{}, name string) {
			hasRead, _ := reader.ReadNext()
			return hasRead, true, "hasRead on first ReadNext (first result)"
		},
		func(reader *Reader) (have, want interface{}, name string) {
			_, err := reader.ReadNext()
			return err, nil, "err on first ReadNext (first result)"
		},
		func(reader *Reader) (have, want interface{}, name string) {
			reader.ReadNext()
			hasRead, _ := reader.ReadNext()
			return hasRead, false, "hasRead on second ReadNext (first result)"
		},
		func(reader *Reader) (have, want interface{}, name string) {
			reader.ReadNext()
			_, err := reader.ReadNext()
			return err, nil, "err on second ReadNext (first result)"
		},
		func(reader *Reader) (have, want interface{}, name string) {
			reader.ReadNext()
			val, _, _ := reader.Int(0)
			return val, 1, "value Int(0) (first result)"
		},
		func(reader *Reader) (have, want interface{}, name string) {
			reader.ReadNext()
			_, isNull, _ := reader.Int(0)
			return isNull, false, "isNull Int(0) (first result)"
		},
		func(reader *Reader) (have, want interface{}, name string) {
			reader.ReadNext()
			_, _, err := reader.Int(0)
			return err, nil, "err Int(0) (first result)"
		},
		// NextResult
		func(reader *Reader) (have, want interface{}, name string) {
			hasResult, _ := reader.NextResult()
			return hasResult, true, "hasResult on NextResult (on first result)"
		},
		func(reader *Reader) (have, want interface{}, name string) {
			_, err := reader.NextResult()
			return err, nil, "err on NextResult (on first result)"
		},
		func(reader *Reader) (have, want interface{}, name string) {
			reader.NextResult()
			hasResult, _ := reader.NextResult()
			return hasResult, false, "hasResult on NextResult (on second result)"
		},
		func(reader *Reader) (have, want interface{}, name string) {
			reader.NextResult()
			_, err := reader.NextResult()
			return err, nil, "err on NextResult (on second result)"
		},
		// Second result
		func(reader *Reader) (have, want interface{}, name string) {
			reader.NextResult()
			hasRead, _ := reader.ReadNext()
			return hasRead, true, "hasRead on first ReadNext (second result)"
		},
		func(reader *Reader) (have, want interface{}, name string) {
			reader.NextResult()
			_, err := reader.ReadNext()
			return err, nil, "err on first ReadNext (second result)"
		},
		func(reader *Reader) (have, want interface{}, name string) {
			reader.NextResult()
			reader.ReadNext()
			hasRead, _ := reader.ReadNext()
			return hasRead, false, "hasRead on second ReadNext (second result)"
		},
		func(reader *Reader) (have, want interface{}, name string) {
			reader.NextResult()
			reader.ReadNext()
			_, err := reader.ReadNext()
			return err, nil, "err on second ReadNext (second result)"
		},
		func(reader *Reader) (have, want interface{}, name string) {
			reader.NextResult()
			reader.ReadNext()
			val, _, _ := reader.String(0)
			return val, "two", "value String(0) (second result)"
		},
		func(reader *Reader) (have, want interface{}, name string) {
			reader.NextResult()
			reader.ReadNext()
			_, isNull, _ := reader.String(0)
			return isNull, false, "isNull String(0) (second result)"
		},
		func(reader *Reader) (have, want interface{}, name string) {
			reader.NextResult()
			reader.ReadNext()
			_, _, err := reader.String(0)
			return err, nil, "err String(0) (second result)"
		},
	}

	for _, test := range tests {
		withSimpleQueryReader(t, "SELECT 1 AS _1; SELECT 'two' AS _two;", func(reader *Reader) {
			if have, want, name := test(reader); have != want {
				t.Errorf("%s failed - have: '%v', but want '%v'", name, have, want)
			}
		})
	}
}

func idParameter(value int) *Parameter {
	idParam := NewParameter("@id", Integer)
	idParam.SetValue(value)

	return idParam
}

func Test_Statement_ActualCommand(t *testing.T) {
	withStatement(t, "SELECT id FROM table1 WHERE strreq = '@id' OR id = @id;", []*Parameter{idParameter(3)}, func(stmt *Statement) {
		if stmt.ActualCommand() != "SELECT id FROM table1 WHERE strreq = '@id' OR id = $1;" {
			t.Fail()
		}
	})
}

type statementReaderTest struct {
	command string
	params  []*Parameter
	fun     func(reader *Reader) (have, want interface{}, name string)
}

func doStatementReaderTest(t *testing.T, test *statementReaderTest) {
	withStatementReader(t, test.command, test.params, func(reader *Reader) {
		if have, want, name := test.fun(reader); have != want {
			t.Errorf("%s failed - have: '%v', but want '%v'", name, have, want)
		}
	})
}

func whereIdEquals2StatementReaderTest(fun func(reader *Reader) (have, want interface{}, name string)) *statementReaderTest {
	return &statementReaderTest{
		command: "SELECT id FROM table1 WHERE id = @id;",
		params:  []*Parameter{idParameter(2)},
		fun:     fun,
	}
}

func Test_DoStatementReaderTests(t *testing.T) {
	tests := []*statementReaderTest{
		whereIdEquals2StatementReaderTest(func(reader *Reader) (have, want interface{}, name string) {
			hasRead, _ := reader.ReadNext()
			return hasRead, true, "WHERE id = 2 - 'hasRead, _ := reader.ReadNext()'"
		}),
		whereIdEquals2StatementReaderTest(func(reader *Reader) (have, want interface{}, name string) {
			_, err := reader.ReadNext()
			return err, nil, "WHERE id = 2 - '_, err := reader.ReadNext()'"
		}),
		whereIdEquals2StatementReaderTest(func(reader *Reader) (have, want interface{}, name string) {
			reader.ReadNext()
			val, _, _ := reader.Int32(0)
			return val, int32(2), "WHERE id = 2 - 'val, _, _ := reader.Int32(0)'"
		}),
		whereIdEquals2StatementReaderTest(func(reader *Reader) (have, want interface{}, name string) {
			reader.ReadNext()
			_, isNull, _ := reader.Int32(0)
			return isNull, false, "WHERE id = 2 - '_, isNull, _ := reader.Int32(0)'"
		}),
		whereIdEquals2StatementReaderTest(func(reader *Reader) (have, want interface{}, name string) {
			reader.ReadNext()
			_, _, err := reader.Int32(0)
			return err, nil, "WHERE id = 2 - '_, _, err := reader.Int32(0)'"
		}),
	}

	for _, test := range tests {
		doStatementReaderTest(t, test)
	}
}
