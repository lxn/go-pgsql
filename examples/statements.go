// Copyright 2010 The go-pgsql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"os"
)

import (
	"github.com/lxn/go-pgsql"
)

func queryAndPrintResults(stmt *pgsql.Statement) {
	rs, err := stmt.Query()
	if err != nil {
		os.Exit(1)
	}
	defer rs.Close()

	stroptOrd := rs.Ordinal("stropt")

	for {
		hasRow, err := rs.FetchNext()
		if err != nil {
			os.Exit(1)
		}
		if !hasRow {
			break
		}

		stropt, isNull, err := rs.String(stroptOrd)
		if err != nil {
			os.Exit(1)
		}
		if isNull {
			stropt = "(null)"
		}
		fmt.Println("stropt:", stropt)
	}
}

func main() {
	conn, err := pgsql.Connect("dbname=postgres user=cbbrowne port=7099", pgsql.LogError)

	if err != nil {
		os.Exit(1)
	}
	defer conn.Close()

	command := "SELECT * FROM table1 WHERE id = @id;"
	idParam := pgsql.NewParameter("@id", pgsql.Integer)

	stmt, err := conn.Prepare(command, idParam)
	if err != nil {
		os.Exit(1)
	}
	defer stmt.Close()

	for id := 1; id <= 3; id++ {
		err = idParam.SetValue(id)
		if err != nil {
			os.Exit(1)
		}
		queryAndPrintResults(stmt)
	}
}
