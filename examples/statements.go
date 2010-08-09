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
		fmt.Printf("stropt: '%s'\n", stropt)
	}
}

func main() {
	params := &pgsql.ConnParams{
		Host:     "127.0.0.1",
		Database: "testdatabase",
		User:     "testuser",
		Password: "testpassword",
	}

	conn, err := pgsql.NewConn(params)
	if err != nil {
		os.Exit(1)
	}

	conn.LogLevel = pgsql.LogError

	err = conn.Open()
	if err != nil {
		os.Exit(1)
	}
	defer conn.Close()

	command := "SELECT * FROM table1 WHERE id = @id;"
	idParam := pgsql.NewParameter("@id", pgsql.Integer)
	parameters := []*pgsql.Parameter{idParam}

	stmt, err := conn.Prepare(command, parameters)
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
