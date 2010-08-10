include $(GOROOT)/src/Make.$(GOARCH)

TARG=pgsql
GOFILES=\
	conn.go\
	disconnectedstate.go\
	error.go\
	messagecodes.go\
	parameter.go\
	processingquerystate.go\
	readystate.go\
	resultset.go\
	state.go\
	statement.go\
	types.go

include $(GOROOT)/src/Make.pkg
