// Copyright 2010 Alexander Neumann. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgsql

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"testing"
	"time"
)

func withConnLog(t *testing.T, logLevel LogLevel, f func(conn *Conn)) {
	conn, err := Connect("dbname=testdatabase user=testuser password=testpassword", logLevel)
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

func withConn(t *testing.T, f func(conn *Conn)) {
	withConnLog(t, LogNothing, f)
}

func withSimpleQueryResultSet(t *testing.T, command string, f func(rs *ResultSet)) {
	withConn(t, func(conn *Conn) {
		rs, err := conn.Query(command)
		if err != nil {
			t.Error("withSimpleQueryResultSet: conn.Query:", err)
			return
		}
		if rs == nil {
			t.Error("withSimpleQueryResultSet: conn.Query: rs == nil")
			return
		}
		defer rs.Close()

		f(rs)
	})
}

func withStatement(t *testing.T, command string, params []*Parameter, f func(stmt *Statement)) {
	withConn(t, func(conn *Conn) {
		stmt, err := conn.Prepare(command, params...)
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

func withStatementResultSet(t *testing.T, command string, params []*Parameter, f func(rs *ResultSet)) {
	withStatement(t, command, params, func(stmt *Statement) {
		rs, err := stmt.Query()
		if err != nil {
			t.Error("withStatementResultSet: stmt.Query:", err)
			return
		}
		if rs == nil {
			t.Error("withStatementResultSet: stmt.Query: rs == nil")
			return
		}
		defer rs.Close()

		f(rs)
	})
}

func param(name string, typ Type, value interface{}) *Parameter {
	p := NewParameter(name, typ)
	err := p.SetValue(value)
	if err != nil {
		panic(err)
	}

	return p
}

func Test_Connect_UglyButValidParamsStyle_ExpectErrNil(t *testing.T) {
	conn, err := Connect(
		`dbname=testdatabase

		user  ='testuser'    password      =  'testpassword'  `,
		LogNothing)
	if err != nil {
		t.Fail()
	}
	if conn != nil {
		conn.Close()
	}
}

func Test_Connect_InvalidPassword_ExpectConnNil(t *testing.T) {
	conn, _ := Connect("dbname=testdatabase user=testuser password=wrongpassword", LogNothing)
	if conn != nil {
		t.Fail()
		conn.Close()
	}
}

func Test_Connect_InvalidPassword_ExpectError28000(t *testing.T) {
	conn, err := Connect("dbname=testdatabase user=testuser password=wrongpassword", LogNothing)
	if err == nil {
		t.Error("expected err != nil")
	}
	// Code 28000 == invalid authorization specification
	if pgerr, ok := err.(*Error); !ok || pgerr.Code() != "28000" {
		t.Error("expected *pgsql.Error with code 28000")
	}
	if conn != nil {
		conn.Close()
	}
}

func Test_DoSimpleQueryResultSetTests(t *testing.T) {
	tests := []func(rs *ResultSet) (have, want interface{}, name string){
		// Basic rs tests
		func(rs *ResultSet) (have, want interface{}, name string) {
			hasRow, _ := rs.FetchNext()
			return hasRow, true, "FetchNext"
		},
		func(rs *ResultSet) (have, want interface{}, name string) {
			hasRow, _ := rs.FetchNext()
			hasRow, _ = rs.FetchNext()
			return hasRow, false, "FetchNext_RetValSecondCall"
		},
		func(rs *ResultSet) (have, want interface{}, name string) {
			_, err := rs.FetchNext()
			return err == nil, true, "FetchNext_ErrNil"
		},

		// Field info tests
		func(rs *ResultSet) (have, want interface{}, name string) {
			fieldCount := rs.FieldCount()
			return fieldCount, 5, "field count"
		},
		func(rs *ResultSet) (have, want interface{}, name string) {
			fieldName, _ := rs.Name(1)
			return fieldName, "_two", "field #1 name"
		},
		func(rs *ResultSet) (have, want interface{}, name string) {
			typ, _ := rs.Type(2)
			return typ, Boolean, "field #2 type"
		},

		// Get value tests
		func(rs *ResultSet) (have, want interface{}, name string) {
			rs.FetchNext()
			val, _, _ := rs.Int32(0)
			return val, int32(1), "field #0"
		},
		func(rs *ResultSet) (have, want interface{}, name string) {
			rs.FetchNext()
			val, _, _ := rs.String(1)
			return val, "two", "field #1"
		},
		func(rs *ResultSet) (have, want interface{}, name string) {
			rs.FetchNext()
			val, _, _ := rs.Bool(2)
			return val, true, "field #2"
		},
		func(rs *ResultSet) (have, want interface{}, name string) {
			rs.FetchNext()
			val, _ := rs.IsNull(3)
			return val, true, "field #3 is null"
		},
		func(rs *ResultSet) (have, want interface{}, name string) {
			rs.FetchNext()
			val, _, _ := rs.Float64(4)
			return val, float64(4.5), "field #4"
		},
	}

	for _, test := range tests {
		withSimpleQueryResultSet(t, "SELECT 1 AS _1, 'two' AS _two, true AS _true, null AS _null, 4.5 AS _4_5;", func(rs *ResultSet) {
			if have, want, name := test(rs); have != want {
				t.Errorf("%s failed - have: '%v', but want '%v'", name, have, want)
			}
		})
	}
}

func Test_SimpleQuery_MultipleSelects(t *testing.T) {
	tests := []func(rs *ResultSet) (have, want interface{}, name string){
		// First result
		func(rs *ResultSet) (have, want interface{}, name string) {
			hasRead, _ := rs.FetchNext()
			return hasRead, true, "hasRead on first FetchNext (first result)"
		},
		func(rs *ResultSet) (have, want interface{}, name string) {
			_, err := rs.FetchNext()
			return err, nil, "err on first FetchNext (first result)"
		},
		func(rs *ResultSet) (have, want interface{}, name string) {
			rs.FetchNext()
			hasRead, _ := rs.FetchNext()
			return hasRead, false, "hasRead on second FetchNext (first result)"
		},
		func(rs *ResultSet) (have, want interface{}, name string) {
			rs.FetchNext()
			_, err := rs.FetchNext()
			return err, nil, "err on second FetchNext (first result)"
		},
		func(rs *ResultSet) (have, want interface{}, name string) {
			rs.FetchNext()
			val, _, _ := rs.Int(0)
			return val, 1, "value Int(0) (first result)"
		},
		func(rs *ResultSet) (have, want interface{}, name string) {
			rs.FetchNext()
			_, isNull, _ := rs.Int(0)
			return isNull, false, "isNull Int(0) (first result)"
		},
		func(rs *ResultSet) (have, want interface{}, name string) {
			rs.FetchNext()
			_, _, err := rs.Int(0)
			return err, nil, "err Int(0) (first result)"
		},
		func(rs *ResultSet) (have, want interface{}, name string) {
			hasResult, _ := rs.NextResult()
			return hasResult, true, "hasResult on NextResult (first result)"
		},
		func(rs *ResultSet) (have, want interface{}, name string) {
			_, err := rs.NextResult()
			return err, nil, "err on NextResult (first result)"
		},
		// Second result
		func(rs *ResultSet) (have, want interface{}, name string) {
			rs.NextResult()
			hasRead, _ := rs.FetchNext()
			return hasRead, true, "hasRead on first FetchNext (second result)"
		},
		func(rs *ResultSet) (have, want interface{}, name string) {
			rs.NextResult()
			_, err := rs.FetchNext()
			return err, nil, "err on first FetchNext (second result)"
		},
		func(rs *ResultSet) (have, want interface{}, name string) {
			rs.NextResult()
			rs.FetchNext()
			hasRead, _ := rs.FetchNext()
			return hasRead, false, "hasRead on second FetchNext (second result)"
		},
		func(rs *ResultSet) (have, want interface{}, name string) {
			rs.NextResult()
			rs.FetchNext()
			_, err := rs.FetchNext()
			return err, nil, "err on second FetchNext (second result)"
		},
		func(rs *ResultSet) (have, want interface{}, name string) {
			rs.NextResult()
			rs.FetchNext()
			val, _, _ := rs.String(0)
			return val, "two", "value String(0) (second result)"
		},
		func(rs *ResultSet) (have, want interface{}, name string) {
			rs.NextResult()
			rs.FetchNext()
			_, isNull, _ := rs.String(0)
			return isNull, false, "isNull String(0) (second result)"
		},
		func(rs *ResultSet) (have, want interface{}, name string) {
			rs.NextResult()
			rs.FetchNext()
			_, _, err := rs.String(0)
			return err, nil, "err String(0) (second result)"
		},
		func(rs *ResultSet) (have, want interface{}, name string) {
			rs.NextResult()
			hasResult, _ := rs.NextResult()
			return hasResult, false, "hasResult on NextResult (second result)"
		},
		func(rs *ResultSet) (have, want interface{}, name string) {
			rs.NextResult()
			_, err := rs.NextResult()
			return err, nil, "err on NextResult (second result)"
		},
	}

	for _, test := range tests {
		withSimpleQueryResultSet(t, "SELECT 1 AS _1; SELECT 'two' AS _two;", func(rs *ResultSet) {
			if have, want, name := test(rs); have != want {
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
	fun     func(rs *ResultSet) (have, want interface{}, name string)
}

func whereIdEquals2StatementResultSetTest(fun func(rs *ResultSet) (have, want interface{}, name string)) *statementResultSetTest {
	return &statementResultSetTest{
		command: "SELECT id FROM table1 WHERE id = @id;",
		params:  []*Parameter{idParameter(2)},
		fun:     fun,
	}
}

func Test_DoStatementResultSetTests(t *testing.T) {
	tests := []*statementResultSetTest{
		whereIdEquals2StatementResultSetTest(func(rs *ResultSet) (have, want interface{}, name string) {
			hasRead, _ := rs.FetchNext()
			return hasRead, true, "WHERE id = 2 - 'hasRead, _ := rs.FetchNext()'"
		}),
		whereIdEquals2StatementResultSetTest(func(rs *ResultSet) (have, want interface{}, name string) {
			_, err := rs.FetchNext()
			return err, nil, "WHERE id = 2 - '_, err := rs.FetchNext()'"
		}),
		whereIdEquals2StatementResultSetTest(func(rs *ResultSet) (have, want interface{}, name string) {
			rs.FetchNext()
			val, _, _ := rs.Int32(0)
			return val, int32(2), "WHERE id = 2 - 'val, _, _ := rs.Int32(0)'"
		}),
		whereIdEquals2StatementResultSetTest(func(rs *ResultSet) (have, want interface{}, name string) {
			rs.FetchNext()
			_, isNull, _ := rs.Int32(0)
			return isNull, false, "WHERE id = 2 - '_, isNull, _ := rs.Int32(0)'"
		}),
		whereIdEquals2StatementResultSetTest(func(rs *ResultSet) (have, want interface{}, name string) {
			rs.FetchNext()
			_, _, err := rs.Int32(0)
			return err, nil, "WHERE id = 2 - '_, _, err := rs.Int32(0)'"
		}),
	}

	for _, test := range tests {
		withStatementResultSet(t, test.command, test.params, func(rs *ResultSet) {
			if have, want, name := test.fun(rs); have != want {
				t.Errorf("%s failed - have: '%v', but want '%v'", name, have, want)
			}
		})
	}
}

type item struct {
	id        int
	name      string
	price     float64
	packUnit  uint
	onSale    bool
	something interface{}
}

func Test_Conn_Scan(t *testing.T) {
	withConn(t, func(conn *Conn) {
		var x item
		command := "SELECT 123, 'abc', 14.99, 4, true, '2010-08-20'::DATE;"
		fetched, err := conn.Scan(command, &x.id, &x.name, &x.price, &x.packUnit, &x.onSale, &x.something)
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
		if math.Abs(float64(x.price)-14.99) > 0.000001 {
			t.Errorf("price - have: %f, but want: 14.99", x.price)
		}
		if x.packUnit != 4 {
			t.Errorf("packUnit - have: %d, but want: 4", x.packUnit)
		}
		if !x.onSale {
			t.Error("onSale - have: true, but want: false")
		}
		seconds, ok := x.something.(int64)
		if !ok {
			t.Error("something should have type int64")
		} else {
			dateStr := time.Unix(seconds, 0).UTC().Format(dateFormat)
			if dateStr != "2010-08-20" {
				t.Errorf("something - have: '%s', but want: '2010-08-20'", dateStr)
			}
		}
	})
}

type dateStyleTest struct {
	typ, format, want string
}

func Test_DateStyle(t *testing.T) {
	dateStyles := []string{
		"ISO", "ISO, DMY", "ISO, MDY",
		"SQL", "SQL, DMY", "SQL, MDY",
		"Postgres", "Postgres, DMY", "Postgres, MDY",
		"German", "German, DMY", "German, MDY",
	}

	tests := []*dateStyleTest{
		&dateStyleTest{
			typ:    "DATE",
			format: dateFormat,
			want:   "2010-08-16",
		},
		&dateStyleTest{
			typ:    "TIME",
			format: timeFormat,
			want:   "01:23:45",
		},
		&dateStyleTest{
			typ:    "TIME WITH TIME ZONE",
			format: timeFormat,
			want:   "01:23:45",
		},
		&dateStyleTest{
			typ:    "TIMESTAMP",
			format: timestampFormat,
			want:   "2010-08-16 01:23:45",
		},
		&dateStyleTest{
			typ:    "TIMESTAMP WITH TIME ZONE",
			format: timestampFormat,
			want:   "2010-08-16 01:23:45",
		},
	}

	for _, style := range dateStyles {
		withConn(t, func(conn *Conn) {
			_, err := conn.Execute("SET TimeZone = UTC;")
			if err != nil {
				t.Errorf("failed to set time zone = UTC: %s", err)
				return
			}

			_, err = conn.Execute(fmt.Sprintf("SET DateStyle = %s;", style))
			if err != nil {
				t.Errorf("failed to set DateStyle = %s: %s", style, err)
				return
			}

			var ts time.Time

			for _, test := range tests {
				_, err = conn.Scan(fmt.Sprintf("SELECT %s '%s';", test.typ, test.want), &ts)
				if err != nil {
					t.Errorf("failed to scan with DateStyle = %s: %s", style, err)
					return
				}

				have := ts.Format(test.format)

				if have != test.want {
					t.Errorf("DateStyle = %s, typ = %s: want: '%s', but have: '%s'", style, test.typ, test.want, have)
				}
			}
		})
	}
}

type timeTest struct {
	command, timeString string
	seconds             int64
}

func newTimeTest(commandTemplate, format, value string) *timeTest {
	test := &timeTest{}

	t, err := time.Parse(format, value)
	if err != nil {
		panic(err)
	}
	t = time.Unix(t.Unix(), 0).UTC()

	if strings.Index(commandTemplate, "%s") > -1 {
		test.command = fmt.Sprintf(commandTemplate, value)
	} else {
		test.command = commandTemplate
	}
	test.seconds = t.Unix()
	test.timeString = t.String()

	return test
}

const (
	dateFormat      = "2006-01-02"
	timeFormat      = "15:04:05"
	timestampFormat = "2006-01-02 15:04:05"
)

func Test_Conn_Scan_Time(t *testing.T) {
	tests := []*timeTest{
		newTimeTest(
			"SELECT DATE '%s';",
			dateFormat,
			"2010-08-14"),
		newTimeTest(
			"SELECT TIME '%s';",
			timeFormat,
			"18:43:32"),
		newTimeTest(
			"SELECT TIME WITH TIME ZONE '%s';",
			timeFormat+"-07",
			"18:43:32+02"),
		newTimeTest(
			"SELECT TIMESTAMP '%s';",
			timestampFormat,
			"2010-08-14 18:43:32"),
		newTimeTest(
			"SELECT TIMESTAMP WITH TIME ZONE '%s';",
			timestampFormat+"-07",
			"2010-08-14 18:43:32+02"),
	}

	for _, test := range tests {
		withConn(t, func(conn *Conn) {
			_, err := conn.Execute("SET TimeZone = 02; SET DateStyle = ISO")
			if err != nil {
				t.Error("failed to set time zone or date style:", err)
				return
			}

			var seconds int64
			_, err = conn.Scan(test.command, &seconds)
			if err != nil {
				t.Error(err)
				return
			}
			if seconds != test.seconds {
				t.Errorf("'%s' failed - have: '%d', but want '%d'", test.command, seconds, test.seconds)
			}

			var tm time.Time
			_, err = conn.Scan(test.command, &tm)
			if err != nil {
				t.Error(err)
				return
			}
			timeString := tm.String()
			if timeString != test.timeString {
				t.Errorf("'%s' failed - have: '%s', but want '%s'", test.command, timeString, test.timeString)
			}
		})
	}
}

func Test_Insert_Time(t *testing.T) {
	tests := []*timeTest{
		newTimeTest(
			"SELECT _d FROM _gopgsql_test_time;",
			dateFormat,
			"2010-08-14"),
		newTimeTest(
			"SELECT _t FROM _gopgsql_test_time;",
			timeFormat,
			"20:03:38"),
		newTimeTest(
			"SELECT _ttz FROM _gopgsql_test_time;",
			timeFormat+"-07",
			"20:03:38+02"),
		newTimeTest(
			"SELECT _ts FROM _gopgsql_test_time;",
			timestampFormat,
			"2010-08-14 20:03:38"),
		newTimeTest(
			"SELECT _tstz FROM _gopgsql_test_time;",
			timestampFormat+"-07",
			"2010-08-14 20:03:38+02"),
	}

	for _, test := range tests {
		withConn(t, func(conn *Conn) {
			conn.Execute("DROP TABLE _gopgsql_test_time;")

			_, err := conn.Execute(
				`CREATE TABLE _gopgsql_test_time
				(
				_d DATE,
				_t TIME,
				_ttz TIME WITH TIME ZONE,
				_ts TIMESTAMP,
				_tstz TIMESTAMP WITH TIME ZONE
				);`)
			if err != nil {
				t.Error("failed to create table:", err)
				return
			}
			defer func() {
				conn.Execute("DROP TABLE _gopgsql_test_time;")
			}()

			_, err = conn.Execute("SET TimeZone = 02; SET DateStyle = ISO")
			if err != nil {
				t.Error("failed to set time zone or date style:", err)
				return
			}

			_d, _ := time.Parse(dateFormat, "2010-08-14")
			_t, _ := time.Parse(timeFormat, "20:03:38")
			_ttz, _ := time.Parse(timeFormat, "20:03:38")
			_ts, _ := time.Parse(timestampFormat, "2010-08-14 20:03:38")
			_tstz, _ := time.Parse(timestampFormat, "2010-08-14 20:03:38")

			stmt, err := conn.Prepare(
				`INSERT INTO _gopgsql_test_time
				(_d, _t, _ttz, _ts, _tstz)
				VALUES
				(@d, @t, @ttz, @ts, @tstz);`,
				param("@d", Date, _d),
				param("@t", Time, _t.Unix()),
				param("@ttz", TimeTZ, _ttz),
				param("@ts", Timestamp, _ts),
				param("@tstz", TimestampTZ, uint64(_tstz.Unix())))
			if err != nil {
				t.Error("failed to prepare insert statement:", err)
				return
			}
			defer stmt.Close()

			_, err = stmt.Execute()
			if err != nil {
				t.Error("failed to execute insert statement:", err)
			}

			var seconds uint64
			_, err = conn.Scan(test.command, &seconds)
			if err != nil {
				t.Error(err)
				return
			}
			if seconds != uint64(test.seconds) {
				t.Errorf("'%s' failed - have: '%d', but want '%d'", test.command, seconds, test.seconds)
			}

			var tm time.Time
			_, err = conn.Scan(test.command, &tm)
			if err != nil {
				t.Error(err)
				return
			}
			timeString := tm.String()
			if timeString != test.timeString {
				t.Errorf("'%s' failed - have: '%s', but want '%s'", test.command, timeString, test.timeString)
			}
		})
	}
}

func Test_Conn_WithSavepoint(t *testing.T) {
	withConn(t, func(conn *Conn) {
		conn.Execute("DROP TABLE _gopgsql_test_account;")

		_, err := conn.Execute(`
			CREATE TABLE _gopgsql_test_account
			(
				name VARCHAR(20) PRIMARY KEY,
				balance REAL NOT NULL
			);
			INSERT INTO _gopgsql_test_account (name, balance) VALUES ('Alice', 100.0);
			INSERT INTO _gopgsql_test_account (name, balance) VALUES ('Bob', 0.0);
			INSERT INTO _gopgsql_test_account (name, balance) VALUES ('Wally', 0.0);
			`)
		if err != nil {
			t.Error("failed to create table:", err)
			return
		}
		defer func() {
			conn.Execute("DROP TABLE _gopgsql_test_account;")
		}()

		err = conn.WithTransaction(ReadCommittedIsolation, func() (err error) {
			_, err = conn.Execute(`
				UPDATE _gopgsql_test_account
				SET balance = balance - 100.0
				WHERE name = 'Alice';`)
			if err != nil {
				t.Error("failed to execute update:", err)
				return
			}

			err = conn.WithSavepoint(ReadCommittedIsolation, func() (err error) {
				_, err = conn.Execute(`
					UPDATE _gopgsql_test_account
					SET balance = balance + 100.0
					WHERE name = 'Bob';`)
				if err != nil {
					t.Error("failed to execute update:", err)
					return
				}

				err = errors.New("wrong credit account")

				return
			})

			_, err = conn.Execute(`
				UPDATE _gopgsql_test_account
				SET balance = balance + 100.0
				WHERE name = 'Wally';`)
			if err != nil {
				t.Error("failed to execute update:", err)
				return
			}

			return
		})

		var rs *ResultSet
		rs, err = conn.Query("SELECT name, balance FROM _gopgsql_test_account;")
		if err != nil {
			t.Error("failed to query:", err)
			return
		}
		defer rs.Close()

		have := make(map[string]float64)
		want := map[string]float64{
			"Alice": 0,
			"Bob":   0,
			"Wally": 100,
		}
		var name string
		var balance float64
		var fetched bool

		for {
			fetched, err = rs.ScanNext(&name, &balance)
			if err != nil {
				t.Error("failed to scan next:", err)
				return
			}
			if !fetched {
				break
			}

			have[name] = balance
		}

		for name, haveBalance := range have {
			wantBalance := want[name]

			if math.Abs(haveBalance-wantBalance) > 0.000001 {
				t.Errorf("name: %s have: %f, but want: %f", name, haveBalance, wantBalance)
			}
		}
	})
}

func Test_Numeric(t *testing.T) {
	strWant := "0." + strings.Repeat("0123456789", 100)[1:]
	numWant, _ := big.NewRat(1, 1).SetString(strWant)
	numParam := param("@num", Numeric, numWant)

	withStatementResultSet(t, "SELECT @num;", []*Parameter{numParam}, func(rs *ResultSet) {
		// Use interface{}, so *resultSet.Any will be tested as well.
		var numHaveInterface interface{}

		_, err := rs.ScanNext(&numHaveInterface)
		if err != nil {
			t.Error("failed to scan next:", err)
		}

		numHave, ok := numHaveInterface.(*big.Rat)
		if !ok {
			t.Errorf("unexpected type: %T", numHaveInterface)
			return
		}

		strHave := numHave.FloatString(999)
		if strHave != strWant {
			t.Errorf("have: %s, but want: %s", strHave, strWant)
		}
	})
}

func Test_FloatInf(t *testing.T) {
	numParam := param("@num", Real, float32(math.Inf(-1)))

	withStatementResultSet(t, "SELECT @num;", []*Parameter{numParam}, func(rs *ResultSet) {
		var numHave float32

		_, err := rs.ScanNext(&numHave)
		if err != nil {
			t.Error("failed to scan next:", err)
		}

		if !math.IsInf(float64(numHave), -1) {
			t.Fail()
		}
	})
}

func Test_FloatNaN(t *testing.T) {
	numParam := param("@num", Double, math.NaN())

	withStatementResultSet(t, "SELECT @num;", []*Parameter{numParam}, func(rs *ResultSet) {
		var numHave float64

		_, err := rs.ScanNext(&numHave)
		if err != nil {
			t.Error("failed to scan next:", err)
		}

		if !math.IsNaN(numHave) {
			t.Fail()
		}
	})
}

func Test_Parameter_SetValue_NilPtr_ValueReturnsNil(t *testing.T) {
	initialValue, _ := time.Parse(timestampFormat, "2010-09-28 16:09:32")
	p := param("@startDateTime", TimestampTZ, initialValue)

	var nilValue time.Time

	p.SetValue(nilValue)

	if p.Value() != nil {
		t.Fail()
	}
}

// This test hung when using no timeout before *ResultSet.FetchNext was fixed.
func Test_Query_Exception(t *testing.T) {
	withConn(t, func(conn *Conn) {
		conn.Execute("CREATE LANGUAGE plpgsql;")

		_, err := conn.Execute(`
			CREATE OR REPLACE FUNCTION one_or_fail(num int) RETURNS int AS $$
			BEGIN
				IF num != 1 THEN
					RAISE EXCEPTION 'FAIL!';
				END IF;
				
				RETURN 1;
			END;
			$$ LANGUAGE plpgsql;
			`)
		if err != nil {
			t.Error("create function failed:", err)
			return
		}
		defer func() {
			conn.Execute("DROP FUNCTION one_or_fail(int);")
		}()

		rs, err := conn.Query("SELECT one_or_fail(2);")
		if err != nil {
			t.Error("query failed")
			return
		}
		defer rs.Close()

		_, err = rs.FetchNext()
		if err == nil {
			t.Error("error expected")
			return
		}
		if _, ok := err.(*Error); !ok {
			t.Error("*pgsql.Error expected")
			return
		}
		rs.Close()

		rs, err = conn.Query("SELECT one_or_fail(2);")
		if err != nil {
			t.Error("query failed")
			return
		}
		defer rs.Close()

		var one int
		_, err = rs.ScanNext(&one)
		if err == nil {
			t.Error("error expected")
			return
		}
		if _, ok := err.(*Error); !ok {
			t.Error("*pgsql.Error expected")
			return
		}
		rs.Close()

		stmt, err := conn.Prepare("SELECT one_or_fail(2);")
		if err != nil {
			t.Error("prepare failed")
			return
		}
		defer stmt.Close()

		_, err = stmt.Execute()
		if err == nil {
			t.Error("error expected")
			return
		}
		if _, ok := err.(*Error); !ok {
			t.Error("*pgsql.Error expected")
			return
		}

		_, err = stmt.Scan(&one)
		if err == nil {
			t.Error("error expected")
			return
		}
		if _, ok := err.(*Error); !ok {
			t.Error("*pgsql.Error expected")
			return
		}

		_, err = conn.Execute("SELECT one_or_fail(2);")
		if err == nil {
			t.Error("error expected")
			return
		}
		if _, ok := err.(*Error); !ok {
			t.Error("*pgsql.Error expected")
			return
		}

		_, err = conn.Scan("SELECT one_or_fail(2);", &one)
		if err == nil {
			t.Error("error expected")
			return
		}
		if _, ok := err.(*Error); !ok {
			t.Error("*pgsql.Error expected")
			return
		}

		var abc string
		_, err = conn.Scan("SELECT 'abc';", &abc)
		if err != nil {
			t.Error("*Conn.Scan failed after previous expected *Conn.Scan error")
			return
		}
	})
}

func Test_bufio_Reader_Read_release_2010_12_08(t *testing.T) {
	withConn(t, func(conn *Conn) {
		conn.Execute("DROP TABLE _gopgsql_test;")

		_, err := conn.Execute(`
			CREATE TABLE _gopgsql_test
			(
				str text
			);
			`)
		if err != nil {
			t.Error("failed to create table:", err)
			return
		}
		defer func() {
			conn.Execute("DROP TABLE _gopgsql_test;")
		}()

		in := strings.Repeat("x", 10000)

		stmt, err := conn.Prepare("INSERT INTO _gopgsql_test (str) VALUES (@str);", param("@str", Text, in))
		if err != nil {
			t.Error("failed to prepare statement:", err)
			return
		}
		defer stmt.Close()

		_, err = stmt.Execute()
		if err != nil {
			t.Error("failed to execute statement:", err)
			return
		}

		var out string

		_, err = conn.Scan("SELECT str FROM _gopgsql_test;", &out)
		if err != nil {
			t.Error("failed to read str:", err)
			return
		}

		if out != in {
			t.Error("out != in")
		}
	})
}

func Test_Issue2_Uint64_OutOfRange(t *testing.T) {
	withConn(t, func(conn *Conn) {
		want := uint64(9989608743)
		query := fmt.Sprintf("SELECT %d::bigint;", want)

		var have uint64
		if _, err := conn.Scan(query, &have); err != nil {
			t.Error("failed to read uint64:", err)
			return
		}

		if have != want {
			t.Errorf("have: %d, but want: %d", have, want)
		}
	})
}
