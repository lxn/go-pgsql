// Copyright 2010 Alexander Neumann. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgsql

import (
	"os"
)

func panicIfErr(err os.Error) {
	if err != nil {
		panic(err)
	}
}

func panicNotImplemented() {
	panic("not implemented")
}
