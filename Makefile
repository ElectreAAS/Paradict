.PHONY: test
test:
	dune runtest --force

.PHONY: bench
bench:
	dune exec bench/bench.exe
