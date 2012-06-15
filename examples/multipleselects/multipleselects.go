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

func main() {
	conn, err := pgsql.Connect("dbname=postgres user=cbbrowne port=7099", pgsql.LogError)

	if err != nil {
		os.Exit(1)
	}
	defer conn.Close()

	rs, err := conn.Query("SELECT 1 AS num; SELECT 2 AS num; SELECT 3 AS num;")
	if err != nil {
		os.Exit(1)
	}
	defer rs.Close()

	for {
		hasRow, err := rs.FetchNext()
		if err != nil {
			os.Exit(1)
		}
		if hasRow {
			num, _, _ := rs.Int(0)
			fmt.Println("num:", num)
		} else {
			hasResult, err := rs.NextResult()
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
