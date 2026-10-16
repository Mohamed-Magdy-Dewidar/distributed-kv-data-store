#!/bin/sh
# Builds the write benchmarks at three commits and runs them interleaved in
# one container, so every comparison is within one run.
set -eu
git config --global --add safe.directory /src
COMMITS="before:442ccaa stripes:494cd1f group:8433b98"
mkdir -p /work/tmp /out
for pair in $COMMITS; do
	name=${pair%%:*}; c=${pair#*:}
	mkdir -p /work/$name
	git -C /src archive "$c" | tar -x -C /work/$name
	(cd /work/$name && go test -c -o /work/bin-$name-engine ./internal/storage/engine/ && go test -c -o /work/bin-$name-store ./internal/store/)
done
for i in 1 2 3 4 5 6; do
	for pair in $COMMITS; do
		name=${pair%%:*}
		for pkg in engine store; do
			dir=/work/$name/internal/storage/engine; [ $pkg = store ] && dir=/work/$name/internal/store
			(cd $dir && TMPDIR=/work/tmp /work/bin-$name-$pkg -test.run '^$' -test.bench . -test.benchtime 1000x -test.count 1) >> /out/$name-$pkg.txt 2>&1
		done
	done
	echo "round $i done $(date -u +%H:%M:%S)"
done
uname -a > /out/env.txt; nproc >> /out/env.txt; go version >> /out/env.txt
