include Make.$(GOARCH)
ALL=pool multipleselects scan statements

all: $(ALL)

scan: scan.$(O)
	$(LD)  -o scan scan.$(O)

scan.$(O): scan.go
	$(GC) scan.go

multipleselects: multipleselects.$(O)
	$(LD)  -o multipleselects multipleselects.$(O)

multipleselects.$(O): multipleselects.go
	$(GC) multipleselects.go

statements: statements.$(O)
	$(LD)  -o statements statements.$(O)

statements.$(O): statements.go
	$(GC) statements.go

pool: pool.$(O)
	$(LD)  -o pool pool.$(O)

pool.$(O): pool.go
	$(GC) pool.go

clean:
	rm -f *.6 *.8 $(ALL)
