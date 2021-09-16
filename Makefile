TMP_DOC_DIR:=/tmp/minimal_ocaml
scratch:=/tmp/l/github/scratch

default: all

all::
	dune build bin/direct_example.exe
	cp _build/default/bin/direct_example.exe .

#	dune build bin/kyoto_example.exe
#	dune build bin/rocksdb_example.exe
#	dune build bin/example.exe

-include Makefile.ocaml

run:
	time ./direct_example.exe hash
#	time $(DUNE) exec bin/rocksdb_example.exe
#	time $(DUNE) exec bin/kyoto_example.exe
#	time $(DUNE) exec bin/example.exe

# for auto-completion of Makefile target
clean::
