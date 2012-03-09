// Copyright 2012 The go-pgsql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"database/sql"
	"fmt"
	"log"
)

import (
	_ "github.com/lxn/go-pgsql"
)

func main() {
	db, err := sql.Open("postgres", "user=testuser password=testpassword")
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	var msg string

	err = db.QueryRow("SELECT $1 || ' ' || $2;", "Hello", "SQL").Scan(&msg)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(msg)
}
