include $(GOROOT)/src/Make.$(GOARCH)

TARG=pgsql
GOFILES=\
	conn.go\
	connectedstate.go\
	disconnectedstate.go\
	error.go\
	messagecodes.go\
	processingquerystate.go\
	reader.go\
	readystate.go\
	startupstate.go\
	state.go

include $(GOROOT)/src/Make.pkg
