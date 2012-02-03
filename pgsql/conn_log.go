// Copyright 2010 Alexander Neumann. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgsql

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"runtime"
)

func (conn *Conn) log(level LogLevel, v ...interface{}) {
	log.Print(v...)
}

func (conn *Conn) logf(level LogLevel, format string, v ...interface{}) {
	log.Printf(format, v...)
}

func (conn *Conn) logError(level LogLevel, err error) {
	if conn.LogLevel >= level {
		conn.log(level, err)
	}
}

func (conn *Conn) logEnter(funcName string) string {
	conn.log(LogDebug, "entering: ", "pgsql."+funcName)
	return funcName
}

func (conn *Conn) logExit(funcName string) {
	conn.log(LogDebug, "exiting: ", "pgsql."+funcName)
}

func (conn *Conn) logAndConvertPanic(x interface{}) (err error) {
	buf := bytes.NewBuffer(nil)

	buf.WriteString(fmt.Sprintf("Error: %v\nStack Trace:\n", x))
	buf.WriteString("=======================================================\n")

	i := 0
	for {
		pc, file, line, ok := runtime.Caller(i + 3)
		if !ok {
			break
		}
		if i > 0 {
			buf.WriteString("-------------------------------------------------------\n")
		}

		fun := runtime.FuncForPC(pc)
		name := fun.Name()

		buf.WriteString(fmt.Sprintf("%s (%s, Line %d)\n", name, file, line))

		i++
	}
	buf.WriteString("=======================================================\n")

	if conn.LogLevel >= LogError {
		conn.log(LogError, buf)
	}

	err, ok := x.(error)
	if !ok {
		err = errors.New(buf.String())
	}

	return
}
