#!/bin/sh
# Time a run the way shopping-sessions-4 scores it.
#
# The score is the wall clock from container start to container exit on the
# hidden practice set (about 20 million events). If that first run is under
# 20 seconds, the scorer repeats it three times and keeps the middle time.
# The 1 million-event dev set is only a correctness gate. The 36 fixtures are
# only a correctness gate. Neither one is the score.
#
# Usage:
#   scripts/bench_score.sh DATA_DIR [ANSWER_DIR]
#
# DATA_DIR is the dataset directory (manifest.json and the CSV trees).
# ANSWER_DIR, when given, is checked with scripts/compare.py after the run.
# A practice-sized directory has no public answer, so omit ANSWER_DIR.
#
# When Docker is available this uses solutions/rust/scripts/docker_run.sh,
# which is the scorer's docker run: linux/amd64, no network, read-only root,
# uid 1000, 8 CPUs, 24 GB, DATA_DIR=/data, OUTPUT_DIR=/output,
# SCRATCH_DIR=/scratch, HOME=/scratch, NCPU=8, MEM_GB=24.
# The clock includes starting that container.
#
# Without Docker the host times the same command run.sh execs: the shopping
# binary with DATA_DIR, SCRATCH_DIR, and OUTPUT_DIR. That is not the scored
# clock: this host has fewer CPUs and less memory, and the local binary is
# musl rather than the glibc binary the image builds. The 36 fixtures are
# not this benchmark.
set -eu

ROOT=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
DATA=$(CDPATH= cd -- "$1" && pwd)
ANSWER=${2:-}

IMAGE="${IMAGE:-shopping-sessions:latest}"
CPUS="${CPUS:-8}"
MEM="${MEM:-24g}"
SOL="$ROOT/solutions/rust"
BIN="$SOL/target/x86_64-unknown-linux-musl/release/shopping"

host_cpus=$(nproc)
if [ "$host_cpus" -ge 8 ]; then
  HOST_NCPU=8
else
  HOST_NCPU=$host_cpus
fi

if docker info >/dev/null 2>&1; then
  MODE=container
else
  MODE=host
fi

echo "score protocol: container start to exit, median of 3 when the first run is under 20s"
echo "mode: $MODE"
if [ "$MODE" = container ]; then
  echo "limits: cpus=$CPUS mem=$MEM image=$IMAGE platform=linux/amd64"
else
  echo "limits: host nproc=$host_cpus NCPU=$HOST_NCPU (scorer pins 8 vCPUs and 24 GB)"
  echo "binary: $BIN (musl; the image links glibc)"
fi
echo "data: $DATA"

run_once() {
  out=$(mktemp -d /tmp/shopping-score-out-XXXX)
  scratch=$(mktemp -d /tmp/shopping-score-scratch-XXXX)
  chmod 777 "$out" "$scratch"
  timing=$(mktemp /tmp/shopping-score-time-XXXX)
  if [ "$MODE" = container ]; then
    IMAGE="$IMAGE" CPUS="$CPUS" MEM="$MEM" \
      /usr/bin/time -o "$timing" -f %e \
      "$SOL/scripts/docker_run.sh" "$DATA" "$out"
  else
    (
      cd "$SOL"
      export DATA_DIR="$DATA" OUTPUT_DIR="$out" SCRATCH_DIR="$scratch"
      export HOME="$scratch" TMPDIR="$scratch"
      export NCPU="$HOST_NCPU" MEM_GB="${MEM_GB:-15}"
      /usr/bin/time -o "$timing" -f %e "$BIN" "$DATA" "$scratch" "$out"
    )
  fi
  if [ -n "$ANSWER" ]; then
    (
      cd "$SOL"
      export PYTHONPATH="/home/a/.local/lib/python3.14/site-packages${PYTHONPATH:+:$PYTHONPATH}"
      python3 scripts/compare.py "$out" "$ANSWER"
    )
  fi
  rm -rf "$out" "$scratch"
  cat "$timing"
  rm -f "$timing"
}

elapsed=$(run_once)
echo "run 1: ${elapsed}s"
score=$elapsed
if awk "BEGIN { exit !($elapsed < 20) }"; then
  elapsed2=$(run_once)
  echo "run 2: ${elapsed2}s"
  elapsed3=$(run_once)
  echo "run 3: ${elapsed3}s"
  score=$(printf '%s\n' "$elapsed" "$elapsed2" "$elapsed3" | sort -n | awk 'NR==2 { print; exit }')
  echo "median of 3: ${score}s"
else
  echo "first run is not under 20s, so the scorer would keep this single time: ${score}s"
fi
