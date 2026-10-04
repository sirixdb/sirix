#!/usr/bin/env bash
set -euo pipefail

# Keep heap first-touch faults outside warm HOT history measurements.
# Invoke through the campaign's shared JVM limiter and CPU-affinity wrapper.
exec java -Xms2g -Xmx2g -XX:+AlwaysPreTouch "$@"
