// Copyright 2010 Alexander Neumann. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"os"
	"pgsql"
)

func main() {
	pgsql.DefaultLogLevel = pgsql.LogError

	params := &pgsql.ConnParams{
		Database: "testdatabase",
		User:     "testuser",
		Password: "testpassword",
	}

	conn, err := pgsql.Connect(params)
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
