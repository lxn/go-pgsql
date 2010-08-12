// Copyright 2010 Alexander Neumann. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"os"
	"pgsql"
)

func queryAndPrintResults(stmt *pgsql.Statement) {
	res, err := stmt.Query()
	if err != nil {
		os.Exit(1)
	}
	defer res.Close()

	stroptOrd := res.Ordinal("stropt")

	for {
		hasRow, err := res.FetchNext()
		if err != nil {
			os.Exit(1)
		}
		if !hasRow {
			break
		}

		stropt, isNull, err := res.String(stroptOrd)
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
	pgsql.DefaultLogLevel = pgsql.LogError

	params := &pgsql.ConnParams{
		Host:     "127.0.0.1",
		Database: "testdatabase",
		User:     "testuser",
		Password: "testpassword",
	}

	conn, err := pgsql.Connect(params)
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
