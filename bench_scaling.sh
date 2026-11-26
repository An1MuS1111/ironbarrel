#!/usr/bin/env bash
# Write-scaling, durability and compaction benchmarks for ironbarrel.
# Works on macOS (bash 3.2) and Linux. Run from the repo root:
#
#   bash bench_scaling.sh            # "before" numbers
#   bash bench_scaling.sh after      # same runs after a change, saved separately
#
# Results go to ~/bench-results/<label>_*.txt and a summary table is printed
# at the end. Each run takes about 40 s; the whole script about 10 minutes.
# Close heavy apps (browser, IDE indexing) while it runs.

set -uo pipefail

LABEL="${1:-before}"
DIR="${BENCH_DIR:-$HOME/bench}"     # must be on your real disk, not a RAM disk
OUT="$HOME/bench-results"
BIN=./target/release/examples/ironbarrel_bench

if [ ! -f Cargo.toml ] || ! grep -q '^name = "ironbarrel"' Cargo.toml; then
  echo "Run this from the ironbarrel repo root." >&2
  exit 1
fi

mkdir -p "$DIR" "$OUT"
echo "Building..." >&2
cargo build --release --example ironbarrel_bench >&2 || exit 1

CORES=$(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu)
echo "Machine: $(uname -sm), $CORES cores. Label: $LABEL. Data dir: $DIR" >&2

run() { # name, then benchmark flags
  local name="$1"; shift
  echo "== $name: $*" >&2
  "$BIN" --dir "$DIR" "$@" > "$OUT/${LABEL}_${name}.txt" 2>&1
  grep -E '^(read|update|rmw) |^RESULT|space amplification' "$OUT/${LABEL}_${name}.txt" >&2
}

# 1. Write scaling: pure writes, then 50/50 mix, at increasing thread counts.
for t in 1 2 4 6; do
  [ "$t" -le "$CORES" ] && run "w_t$t" --workload w --threads "$t"
done
for t in 1 2 4; do
  [ "$t" -le "$CORES" ] && run "a_t$t" --workload a --threads "$t"
done

# 2. Durability: fsync on every write vs every 100 ms (never = w_t1 above).
run "sync_always" --workload w --threads 1 --sync always
run "sync_100ms"  --workload w --threads 1 --sync interval:100

# 3. Compaction: same write load with auto-merge on (off = w_t1 above).
run "merge_on" --workload w --threads 1 --auto-merge-mb 256

echo
echo "Summary ($LABEL):"
for f in "$OUT/${LABEL}"_*.txt; do
  name=$(basename "$f" .txt)
  line=$(grep '^RESULT' "$f") || { echo "$name: no RESULT (see $f)"; continue; }
  ops=$(echo "$line" | sed -n 's/.*ops_s=\([0-9]*\).*/\1/p')
  up99=$(echo "$line" | sed -n 's/.*update_p99_us=\([0-9.]*\).*/\1/p')
  rd99=$(echo "$line" | sed -n 's/.*read_p99_us=\([0-9.]*\).*/\1/p')
  sa=$(sed -n 's/.*space amplification \([0-9.]*x\).*/\1/p' "$f")
  printf "%-22s ops/s=%-9s read_p99=%-7s update_p99=%-8s space_amp=%s\n" "$name" "$ops" "${rd99}us" "${up99}us" "$sa"
done
