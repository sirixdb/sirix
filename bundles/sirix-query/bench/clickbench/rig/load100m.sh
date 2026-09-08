#!/bin/bash
# Load the 100M corpus into a fresh database with the segment lane. ~45-60 min, ~48 GB on disk.
# Writes the pointer file the query scripts read, so the next suite100m.sh/diag100m.sh use this DB.
#
# Preconditions it enforces: MemAvailable >= 26 GB (the envelope needs the box to itself), >= 100 GB
# free disk, the source present, no other leg. Only ONE 100M database fits on the box: delete the old
# one deliberately (ask first if you did not create it) before loading a new one.
set -u
. "$(dirname "$0")/rig.env"
D=${1:-$ROOT/bundles/sirix-query/build/diagnostics/clickbench-seg100m-$(date +%Y%m%d-%H%M)}
mkdir -p "$D"
AVAIL_KB=$(grep MemAvailable /proc/meminfo | awk '{print $2}')
[ "$AVAIL_KB" -ge 26000000 ] || { echo "ABORT: MemAvailable $((AVAIL_KB/1024)) MB < 26 GB"; exit 1; }
FREE_GB=$(df --output=avail -BG "$ROOT" | tail -1 | tr -dc '0-9')
[ "$FREE_GB" -ge 100 ] || { echo "ABORT: only ${FREE_GB} GB free"; exit 1; }
[ -f "$SRC100M" ] || { echo "ABORT: source $SRC100M missing"; exit 1; }
take_lock 120
refuse_live_jvm
echo "$D" > "$WORK/current-100m-dir.txt"
export CB100M_DIR="$D"
echo "start $(date +%H:%M:%S) memAvail=$((AVAIL_KB/1024))MB freeDisk=${FREE_GB}GB dir=$D" | tee "$D/watch.txt"
# Liveness = file growth. A watcher line every minute; a load whose db stops growing is dead.
( while [ ! -f "$D/LOAD100M_DONE" ]; do
    SZ=$(find "$D/db" -name 'sirix.data' -printf '%s' 2>/dev/null | head -1)
    printf '%s db=%sGB free=%sGB\n' "$(date +%H:%M:%S)" \
      "$(awk -v b="${SZ:-0}" 'BEGIN{printf "%.1f", b/1e9}')" \
      "$(df --output=avail -BG "$ROOT" | tail -1 | tr -dc '0-9')" >> "$D/watch.txt"
    sleep 60
  done ) &
T0=$(date +%s)
cd "$ROOT" && ./gradlew --no-daemon --console=plain :sirix-query:clickBenchLoad -Pclickbench.args="$D/db $SRC100M" \
  -Pclickbench.jvmArgs="-Xms6g -Xmx16g -XX:+ExitOnOutOfMemoryError -Dsirix.offheap.bytes=8589934592 \
    -Dclickbench.expectedRows=99997497 $LOADFLAGS -Dsirix.storage.profile=true ${EXTRA:-}" > "$D/load.log" 2>&1
LE=$?
touch "$D/LOAD100M_DONE"
echo "LOAD_EXIT=$LE elapsed=$(( $(date +%s) - T0 ))s" | tee -a "$D/watch.txt"
find "$D/db" -name 'sirix.data' -printf '%s %p\n' 2>/dev/null | tee -a "$D/watch.txt"
grep -E 'Load time|Data size|segment lane:|OutOfMemory|Exception' "$D/load.log" | head -8
