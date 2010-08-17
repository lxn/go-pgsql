// Copyright 2010 Alexander Neumann. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgsql

const (
	_BOOLOID             = 16
	_BYTEAOID            = 17
	_CHAROID             = 18
	_NAMEOID             = 19
	_INT8OID             = 20
	_INT2OID             = 21
	_INT2VECTOROID       = 22
	_INT4OID             = 23
	_REGPROCOID          = 24
	_TEXTOID             = 25
	_OIDOID              = 26
	_TIDOID              = 27
	_XIDOID              = 28
	_CIDOID              = 29
	_OIDVECTOROID        = 30
	_XMLOID              = 142
	_POINTOID            = 600
	_LSEGOID             = 601
	_PATHOID             = 602
	_BOXOID              = 603
	_POLYGONOID          = 604
	_LINEOID             = 628
	_FLOAT4OID           = 700
	_FLOAT8OID           = 701
	_ABSTIMEOID          = 702
	_RELTIMEOID          = 703
	_TINTERVALOID        = 704
	_UNKNOWNOID          = 705
	_CIRCLEOID           = 718
	_CASHOID             = 790
	_MACADDROID          = 829
	_INETOID             = 869
	_CIDROID             = 650
	_INT4ARRAYOID        = 1007
	_TEXTARRAYOID        = 1009
	_FLOAT4ARRAYOID      = 1021
	_ACLITEMOID          = 1033
	_CSTRINGARRAYOID     = 1263
	_BPCHAROID           = 1042
	_VARCHAROID          = 1043
	_DATEOID             = 1082
	_TIMEOID             = 1083
	_TIMESTAMPOID        = 1114
	_TIMESTAMPTZOID      = 1184
	_INTERVALOID         = 1186
	_TIMETZOID           = 1266
	_BITOID              = 1560
	_VARBITOID           = 1562
	_NUMERICOID          = 1700
	_REFCURSOROID        = 1790
	_REGPROCEDUREOID     = 2202
	_REGOPEROID          = 2203
	_REGOPERATOROID      = 2204
	_REGCLASSOID         = 2205
	_REGTYPEOID          = 2206
	_REGTYPEARRAYOID     = 2211
	_TSVECTOROID         = 3614
	_GTSVECTOROID        = 3642
	_TSQUERYOID          = 3615
	_REGCONFIGOID        = 3734
	_REGDICTIONARYOID    = 3769
	_RECORDOID           = 2249
	_RECORDARRAYOID      = 2287
	_CSTRINGOID          = 2275
	_ANYOID              = 2276
	_ANYARRAYOID         = 2277
	_VOIDOID             = 2278
	_TRIGGEROID          = 2279
	_LANGUAGE_HANDLEROID = 2280
	_INTERNALOID         = 2281
	_OPAQUEOID           = 2282
	_ANYELEMENTOID       = 2283
	_ANYNONARRAYOID      = 2776
	_ANYENUMOID          = 3500
)

// Type represents the PostgreSQL data type of fields and parameters.
type Type int32

const (
	Custom      Type = 0
	Boolean     Type = _BOOLOID
	Char        Type = _CHAROID
	Date        Type = _DATEOID
	Real        Type = _FLOAT4OID
	Double      Type = _FLOAT8OID
	Smallint    Type = _INT2OID
	Integer     Type = _INT4OID
	Bigint      Type = _INT8OID
	Text        Type = _TEXTOID
	Time        Type = _TIMEOID
	TimeTZ      Type = _TIMETZOID
	Timestamp   Type = _TIMESTAMPOID
	TimestampTZ Type = _TIMESTAMPTZOID
	Varchar     Type = _VARCHAROID
)

func (t Type) String() string {
	switch t {
	case Boolean:
		return "Boolean"

	case Char:
		return "Char"

	case Custom:
		return "Custom"

	case Date:
		return "Date"

	case Real:
		return "Real"

	case Double:
		return "Double"

	case Smallint:
		return "Smallint"

	case Integer:
		return "Integer"

	case Bigint:
		return "Bigint"

	case Text:
		return "Text"

	case Time:
		return "Time"

	case TimeTZ:
		return "TimeTZ"

	case Timestamp:
		return "Timestamp"

	case TimestampTZ:
		return "TimestampTZ"

	case Varchar:
		return "Varchar"
	}

	return "Unknown"
}
