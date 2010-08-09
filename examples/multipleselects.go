package main

import (
	"fmt"
	"os"
	"pgsql"
)

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

	res, err := conn.Query("SELECT 1 AS num; SELECT 2 AS num; SELECT 3 AS num;")
	if err != nil {
		os.Exit(1)
	}
	defer res.Close()

	for {
		hasRow, err := res.FetchNext()
		if err != nil {
			os.Exit(1)
		}
		if hasRow {
			num, _, _ := res.Int(0)
			fmt.Println("num:", num)
		} else {
			hasResult, err := res.NextResult()
			if err != nil {
				os.Exit(1)
			}
			if !hasResult {
				break
			}
			fmt.Println("next result")
		}
	}
}
