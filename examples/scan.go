// Copyright 2010 Alexander Neumann. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"os"
	"pgsql"
)

type item struct {
	id    int
	name  string
	price float
}

func main() {
	// conn, err := pgsql.Connect("dbname=postgres user=cbbrowne port=7099", pgsql.LogError)
	// Can have a long connection string, if needed, but if values are set in environment, it's all optional
	conn, err := pgsql.Connect("", pgsql.LogError)
	if err != nil {
		os.Exit(1)
	}
	defer conn.Close()

	var x item

	_, err = conn.Scan("SELECT 123, 'abc', 14.99;", &x.id, &x.name, &x.price)
	if err != nil {
		os.Exit(1)
	}

	fmt.Printf("item x: '%+v'\n", x)
}
