#!/usr/bin/env bash
# Runs a repeatable matrix of macro-benchmarks and prints one RESULT line per run.
#
#   DIR=/mnt/nvme/bench ./run_suite.sh | tee results.txt
#
# Override the matrix via env:  WORKLOADS="c a w" THREADS="1 8" SYNCS="never always" REPS=5
# Report the MEDIAN of the repetitions and the spread (min..max), never a single run.
set -euo pipefail

DIR=${DIR:?set DIR to a directory on the disk under test (not tmpfs)}
WORKLOADS=${WORKLOADS:-"c b a w"}
THREADS=${THREADS:-"1 8"}
SYNCS=${SYNCS:-"never interval:100 always"}
REPS=${REPS:-3}
KEYS=${KEYS:-1000000}
VALUE=${VALUE:-256}
DURATION=${DURATION:-30}
WARMUP=${WARMUP:-5}

cargo build --release --example ironbarrel_bench >&2
BIN=target/release/examples/ironbarrel_bench

for sync in $SYNCS; do
  for wl in $WORKLOADS; do
    # read-only workload does not care about sync strategy; run it once
    if [[ "$wl" == "c" && "$sync" != "never" ]]; then continue; fi
    for t in $THREADS; do
      for rep in $(seq 1 "$REPS"); do
        echo "# sync=$sync workload=$wl threads=$t rep=$rep" >&2
        "$BIN" --dir "$DIR" --workload "$wl" --threads "$t" --sync "$sync" \
               --keys "$KEYS" --value-size "$VALUE" \
               --duration "$DURATION" --warmup "$WARMUP" | grep '^RESULT'
      done
    done
  done
done
