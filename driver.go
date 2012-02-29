// Copyright 2012 The go-pgsql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgsql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"time"
)

func init() {
	sql.Register("postgres", sqlDriver{})
}

type sqlDriver struct {
}

func (sqlDriver) Open(name string) (driver.Conn, error) {
	conn, err := Connect(name, LogNothing)
	if err != nil {
		return nil, err
	}

	return &sqlConn{conn}, nil
}

type sqlConn struct {
	conn *Conn
}

func (c *sqlConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	n, err := c.conn.Execute(query, paramsFromValues(nil, args)...)
	if err != nil {
		return nil, err
	}

	return driver.RowsAffected(n), nil
}

func (c *sqlConn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.conn.Prepare(query)
	if err != nil {
		return nil, err
	}

	return &sqlStmt{stmt}, nil
}

func (c *sqlConn) Close() error {
	return c.conn.Close()
}

func (c *sqlConn) Begin() (driver.Tx, error) {
	if _, err := c.conn.Execute("BEGIN;"); err != nil {
		return nil, err
	}

	return &sqlTx{c.conn}, nil
}

type sqlStmt struct {
	stmt *Statement
}

func (s *sqlStmt) Close() error {
	return s.stmt.Close()
}

func (s *sqlStmt) NumInput() int {
	return -1
}

func (s *sqlStmt) Exec(args []driver.Value) (driver.Result, error) {
	s.stmt.params = paramsFromValues(s.stmt.params, args)

	n, err := s.stmt.Execute()
	if err != nil {
		return nil, err
	}

	return driver.RowsAffected(n), nil
}

func (s *sqlStmt) Query(args []driver.Value) (driver.Rows, error) {
	s.stmt.params = paramsFromValues(s.stmt.params, args)

	rs, err := s.stmt.Query()
	if err != nil {
		return nil, err
	}

	return &sqlRows{rs}, nil
}

type sqlTx struct {
	conn *Conn
}

func (t *sqlTx) Commit() error {
	_, err := t.conn.Execute("COMMIT;")

	return err
}

func (t *sqlTx) Rollback() error {
	_, err := t.conn.Execute("ROLLBACK;")

	return err
}

type sqlRows struct {
	rs *ResultSet
}

func (r *sqlRows) Columns() []string {
	names := make([]string, len(r.rs.fields))

	for i, f := range r.rs.fields {
		names[i] = f.name
	}

	return names
}

func (r *sqlRows) Close() error {
	return r.rs.Close()
}

func (r *sqlRows) Next(dest []driver.Value) error {
	fetched, err := r.rs.FetchNext()
	if err != nil {
		return err
	}

	if !fetched {
		return io.EOF
	}

	for i := range dest {
		val, isNull, err := r.rs.Any(i)
		if err != nil {
			return err
		}

		if isNull {
			val = nil
		}

		dest[i] = val
	}

	return nil
}

func paramsFromValues(params []*Parameter, vals []driver.Value) []*Parameter {
	if params == nil {
		params = make([]*Parameter, len(vals))
	}

	for i, val := range vals {
		p := params[i]

		if p == nil {
			var typ Type

			switch val.(type) {
			case nil:
				typ = Integer

			case bool:
				typ = Boolean

			case []byte, string:
				typ = Varchar

			case float64:
				typ = Double

			case int64:
				typ = Bigint

			case time.Time:
				typ = TimestampTZ

			default:
				panic("unexpected value type")
			}

			p = NewParameter(fmt.Sprintf("$%d", i), typ)
			params[i] = p
		}

		p.SetValue(val)
	}

	return params
}
