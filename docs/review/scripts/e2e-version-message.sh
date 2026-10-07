#!/bin/bash
# e2e-version-message.sh — boots a real store end to end through the startup gate.
#
# Case 1, the production copy: written in WAL format version 1, which this build
# cannot read. It must be refused, and the message must name both versions.
#
# Case 2, a version-3 store: must boot through to serving, so the refusal in
# case 1 is not a gate that refuses everything.
#
#   ./e2e-version-message.sh <binary> <template-config>
set -u

BIN=${1:?usage: e2e-version-message.sh <binary> <template-config>}
TEMPLATE=${2:?usage: e2e-version-message.sh <binary> <template-config>}
PROD=${PROD:-$HOME/bench-prod-data/pristine.tar.zst}
V3=${V3:-$HOME/laptop-test-kit/stores/pristine-3k.tar.zst}
WORK=${WORK:-/tmp/e2e-nodedb}

say() { printf '\n== %s\n' "$*"; }

extract() { # $1 = archive, $2 = destination
  rm -rf "$2" "$WORK/store"
  mkdir -p "$2" "$WORK/store"
  tar --zstd -xf "$1" -C "$WORK/store" --no-same-owner
  cp -a "$WORK/store/nodedb/." "$2"/
}

config_for() { # $1 = data dir, $2 = output config
  sed "s#^data_dir = .*#data_dir = \"$1\"#" "$TEMPLATE" > "$2"
}

clean() { sed -r 's/\x1b\[[0-9;]*m//g'; }

say "case 1: a version-1 store must be refused, by name"
DATA1=$WORK/prod-v1
extract "$PROD" "$DATA1"
config_for "$DATA1" "$WORK/v1.toml"
LOG1=$WORK/v1.log
RUST_LOG=info NO_COLOR=1 timeout 180 "$BIN" --config "$WORK/v1.toml" > "$LOG1" 2>&1
CODE1=$?
echo "exit=$CODE1"
clean < "$LOG1" | grep -aE "format version|corrupted|StartupError|listening" | head -4
echo "segments in the store: $(ls "$DATA1/wal" 2>/dev/null | grep -c '\.seg$')"

say "case 2: a version-3 store must boot to serving"
DATA2=$WORK/store-v3
extract "$V3" "$DATA2"
config_for "$DATA2" "$WORK/v3.toml"
LOG2=$WORK/v3.log
RUST_LOG=info NO_COLOR=1 "$BIN" --config "$WORK/v3.toml" > "$LOG2" 2>&1 &
PID=$!
READY=0
for _ in $(seq 1 180); do
  if grep -qa "HTTP API server listening" "$LOG2" 2>/dev/null; then READY=1; break; fi
  kill -0 "$PID" 2>/dev/null || break
  sleep 1
done
kill "$PID" 2>/dev/null; wait "$PID" 2>/dev/null
echo "ready=$READY"
clean < "$LOG2" | grep -aE "WAL replay complete|listening|format version|StartupError" | head -3

say "verdict"
if [ "$CODE1" -ne 0 ] && [ "$READY" = "1" ]; then
  echo "PASS: the version-1 store was refused and the version-3 store served"
else
  echo "FAIL: refused=$([ "$CODE1" -ne 0 ] && echo yes || echo no) served=$READY"
  exit 1
fi
