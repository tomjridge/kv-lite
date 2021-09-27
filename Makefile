TMP_DOC_DIR:=/tmp/minimal_ocaml
scratch:=/tmp/l/github/scratch

default: all

all::
	dune build bin/lwt_example.exe bin/direct_example.exe

-include Makefile.ocaml

run:
	time dune exec bin/direct_example.exe btree

# for auto-completion of Makefile target
clean::

