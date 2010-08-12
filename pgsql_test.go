// Copyright 2010 Alexander Neumann. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgsql

import (
    "math"
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

func withConn(t *testing.T, f func(conn *Conn)) {
	conn, err := Connect(validParams())
	if err != nil {
		t.Error("withConn: Connect:", err)
		return
	}
	if conn == nil {
		t.Error("withConn: Connect: conn == nil")
		return
	}
	defer conn.Close()

	f(conn)
}

func withSimpleQueryResultSet(t *testing.T, command string, f func(res *ResultSet)) {
	withConn(t, func(conn *Conn) {
		res, err := conn.Query(command)
		if err != nil {
			t.Error("withSimpleQueryResultSet: conn.Query:", err)
			return
		}
		if res == nil {
			t.Error("withSimpleQueryResultSet: conn.Query: res == nil")
			return
		}
		defer res.Close()

		f(res)
	})
}

func withStatement(t *testing.T, command string, params []*Parameter, f func(stmt *Statement)) {
	withConn(t, func(conn *Conn) {
		stmt, err := conn.PrepareSlice(command, params)
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

func withStatementResultSet(t *testing.T, command string, params []*Parameter, f func(res *ResultSet)) {
	withStatement(t, command, params, func(stmt *Statement) {
		res, err := stmt.Query()
		if err != nil {
			t.Error("withStatementResultSet: stmt.Query:", err)
			return
		}
		if res == nil {
			t.Error("withStatementResultSet: stmt.Query: res == nil")
			return
		}
		defer res.Close()

		f(res)
	})
}

func Test_Connect_NilParams_ExpectErrNotNil(t *testing.T) {
	_, err := Connect(nil)
	if err == nil {
		t.Fail()
	}
}

func Test_Connect_NilParams_ExpectConnNil(t *testing.T) {
	conn, _ := Connect(nil)
	if conn != nil {
		t.Fail()
	}
}

func Test_Connect_ValidParams_ExpectErrNil(t *testing.T) {
	conn, err := Connect(validParams())
	if err != nil {
		t.Fail()
	}
	if conn != nil {
		conn.Close()
	}
}

func Test_Connect_InvalidPassword_ExpectConnNil(t *testing.T) {
	params := validParams()
	params.Password = "wrongpassword"

	conn, _ := Connect(params)
	if conn != nil {
		t.Fail()
		conn.Close()
	}
}

func Test_Connect_InvalidPassword_ExpectErrNotNil(t *testing.T) {
	params := validParams()
	params.Password = "wrongpassword"

	conn, err := Connect(params)
	if err == nil {
		t.Fail()
	}
	if conn != nil {
		conn.Close()
	}
}

func Test_DoSimpleQueryResultSetTests(t *testing.T) {
	tests := []func(res *ResultSet) (have, want interface{}, name string){
		// Basic res tests
		func(res *ResultSet) (have, want interface{}, name string) {
			hasRow, _ := res.FetchNext()
			return hasRow, true, "FetchNext"
		},
		func(res *ResultSet) (have, want interface{}, name string) {
			hasRow, _ := res.FetchNext()
			hasRow, _ = res.FetchNext()
			return hasRow, false, "FetchNext_RetValSecondCall"
		},
		func(res *ResultSet) (have, want interface{}, name string) {
			_, err := res.FetchNext()
			return err == nil, true, "FetchNext_ErrNil"
		},

		// Get value tests
		func(res *ResultSet) (have, want interface{}, name string) {
			res.FetchNext()
			val, _, _ := res.Int32(0)
			return val, int32(1), "field #0"
		},
		func(res *ResultSet) (have, want interface{}, name string) {
			res.FetchNext()
			val, _, _ := res.String(1)
			return val, "two", "field #1"
		},
		func(res *ResultSet) (have, want interface{}, name string) {
			res.FetchNext()
			val, _, _ := res.Bool(2)
			return val, true, "field #2"
		},
		func(res *ResultSet) (have, want interface{}, name string) {
			res.FetchNext()
			val, _ := res.IsNull(3)
			return val, true, "field #3 is null"
		},
		func(res *ResultSet) (have, want interface{}, name string) {
			res.FetchNext()
			val, _, _ := res.Float64(4)
			return val, float64(4.5), "field #4"
		},
	}

	for _, test := range tests {
		withSimpleQueryResultSet(t, "SELECT 1 AS _1, 'two' AS _two, true AS _true, null AS _null, 4.5 AS _4_5;", func(res *ResultSet) {
			if have, want, name := test(res); have != want {
				t.Errorf("%s failed - have: '%v', but want '%v'", name, have, want)
			}
		})
	}
}

func Test_SimpleQuery_MultipleSelects(t *testing.T) {
	tests := []func(res *ResultSet) (have, want interface{}, name string){
		// First result
		func(res *ResultSet) (have, want interface{}, name string) {
			hasRead, _ := res.FetchNext()
			return hasRead, true, "hasRead on first FetchNext (first result)"
		},
		func(res *ResultSet) (have, want interface{}, name string) {
			_, err := res.FetchNext()
			return err, nil, "err on first FetchNext (first result)"
		},
		func(res *ResultSet) (have, want interface{}, name string) {
			res.FetchNext()
			hasRead, _ := res.FetchNext()
			return hasRead, false, "hasRead on second FetchNext (first result)"
		},
		func(res *ResultSet) (have, want interface{}, name string) {
			res.FetchNext()
			_, err := res.FetchNext()
			return err, nil, "err on second FetchNext (first result)"
		},
		func(res *ResultSet) (have, want interface{}, name string) {
			res.FetchNext()
			val, _, _ := res.Int(0)
			return val, 1, "value Int(0) (first result)"
		},
		func(res *ResultSet) (have, want interface{}, name string) {
			res.FetchNext()
			_, isNull, _ := res.Int(0)
			return isNull, false, "isNull Int(0) (first result)"
		},
		func(res *ResultSet) (have, want interface{}, name string) {
			res.FetchNext()
			_, _, err := res.Int(0)
			return err, nil, "err Int(0) (first result)"
		},
		func(res *ResultSet) (have, want interface{}, name string) {
			hasResult, _ := res.NextResult()
			return hasResult, true, "hasResult on NextResult (first result)"
		},
		func(res *ResultSet) (have, want interface{}, name string) {
			_, err := res.NextResult()
			return err, nil, "err on NextResult (first result)"
		},
		// Second result
		func(res *ResultSet) (have, want interface{}, name string) {
			res.NextResult()
			hasRead, _ := res.FetchNext()
			return hasRead, true, "hasRead on first FetchNext (second result)"
		},
		func(res *ResultSet) (have, want interface{}, name string) {
			res.NextResult()
			_, err := res.FetchNext()
			return err, nil, "err on first FetchNext (second result)"
		},
		func(res *ResultSet) (have, want interface{}, name string) {
			res.NextResult()
			res.FetchNext()
			hasRead, _ := res.FetchNext()
			return hasRead, false, "hasRead on second FetchNext (second result)"
		},
		func(res *ResultSet) (have, want interface{}, name string) {
			res.NextResult()
			res.FetchNext()
			_, err := res.FetchNext()
			return err, nil, "err on second FetchNext (second result)"
		},
		func(res *ResultSet) (have, want interface{}, name string) {
			res.NextResult()
			res.FetchNext()
			val, _, _ := res.String(0)
			return val, "two", "value String(0) (second result)"
		},
		func(res *ResultSet) (have, want interface{}, name string) {
			res.NextResult()
			res.FetchNext()
			_, isNull, _ := res.String(0)
			return isNull, false, "isNull String(0) (second result)"
		},
		func(res *ResultSet) (have, want interface{}, name string) {
			res.NextResult()
			res.FetchNext()
			_, _, err := res.String(0)
			return err, nil, "err String(0) (second result)"
		},
		func(res *ResultSet) (have, want interface{}, name string) {
			res.NextResult()
			hasResult, _ := res.NextResult()
			return hasResult, false, "hasResult on NextResult (second result)"
		},
		func(res *ResultSet) (have, want interface{}, name string) {
			res.NextResult()
			_, err := res.NextResult()
			return err, nil, "err on NextResult (second result)"
		},
	}

	for _, test := range tests {
		withSimpleQueryResultSet(t, "SELECT 1 AS _1; SELECT 'two' AS _two;", func(res *ResultSet) {
			if have, want, name := test(res); have != want {
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

type statementResultSetTest struct {
	command string
	params  []*Parameter
	fun     func(res *ResultSet) (have, want interface{}, name string)
}

func whereIdEquals2StatementResultSetTest(fun func(res *ResultSet) (have, want interface{}, name string)) *statementResultSetTest {
	return &statementResultSetTest{
		command: "SELECT id FROM table1 WHERE id = @id;",
		params:  []*Parameter{idParameter(2)},
		fun:     fun,
	}
}

func Test_DoStatementResultSetTests(t *testing.T) {
	tests := []*statementResultSetTest{
		whereIdEquals2StatementResultSetTest(func(res *ResultSet) (have, want interface{}, name string) {
			hasRead, _ := res.FetchNext()
			return hasRead, true, "WHERE id = 2 - 'hasRead, _ := res.FetchNext()'"
		}),
		whereIdEquals2StatementResultSetTest(func(res *ResultSet) (have, want interface{}, name string) {
			_, err := res.FetchNext()
			return err, nil, "WHERE id = 2 - '_, err := res.FetchNext()'"
		}),
		whereIdEquals2StatementResultSetTest(func(res *ResultSet) (have, want interface{}, name string) {
			res.FetchNext()
			val, _, _ := res.Int32(0)
			return val, int32(2), "WHERE id = 2 - 'val, _, _ := res.Int32(0)'"
		}),
		whereIdEquals2StatementResultSetTest(func(res *ResultSet) (have, want interface{}, name string) {
			res.FetchNext()
			_, isNull, _ := res.Int32(0)
			return isNull, false, "WHERE id = 2 - '_, isNull, _ := res.Int32(0)'"
		}),
		whereIdEquals2StatementResultSetTest(func(res *ResultSet) (have, want interface{}, name string) {
			res.FetchNext()
			_, _, err := res.Int32(0)
			return err, nil, "WHERE id = 2 - '_, _, err := res.Int32(0)'"
		}),
	}

	for _, test := range tests {
		withStatementResultSet(t, test.command, test.params, func(res *ResultSet) {
			if have, want, name := test.fun(res); have != want {
				t.Errorf("%s failed - have: '%v', but want '%v'", name, have, want)
			}
		})
	}
}

type item struct {
    id int
    name string
    price float
    onSale bool
}

func Test_Conn_Scan(t *testing.T) {
    withConn(t, func(conn *Conn) {
        var x item
        command := "SELECT 123, 'abc', 14.99, true;"
        fetched, err := conn.Scan(command, &x.id, &x.name, &x.price, &x.onSale)
        if err != nil {
            t.Error(err)
            return
        }
        if !fetched {
            t.Error("fetched == false")
        }
        if x.id != 123 {
            t.Errorf("id - have: %d, but want: 123", x.id)
        }
        if x.name != "abc" {
            t.Errorf("name - have: '%s', but want: 'abc'", x.name)
        }
        if math.Fabs(float64(x.price) - 14.99) > 0.000001 {
            t.Errorf("price - have: %f, but want: 14.99", x.price)
        }
        if !x.onSale {
            t.Error("onSale - have: true, but want: false")
        }
    })
}
