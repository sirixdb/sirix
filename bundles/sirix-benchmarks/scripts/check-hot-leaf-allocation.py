#!/usr/bin/env python3
"""Compare GC-profiled encoded owned results with unchanged-main RAW JMH results."""
import argparse
import json
import math
from pathlib import Path

# Less than the smallest object; tolerates profiler background rate, never a per-page object.
PROFILER_NOISE_BYTES = 1.0
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("baseline", type=Path)
parser.add_argument("encoded", type=Path)
parser.add_argument("--minimum-forks", type=int, default=5)
args = parser.parse_args()
if args.minimum_forks < 1:
    parser.error("minimum forks must be positive")


def metrics(path, encoding):
    found = {}
    for row in json.loads(path.read_text()):
        if row["params"]["encoding"] != encoding:
            raise ValueError(f"Unexpected encoding in {path}")
        metric = row["secondaryMetrics"]["gc.alloc.rate.norm"]
        if metric["scoreUnit"] != "B/op" or not math.isfinite(metric["score"]):
            raise ValueError(f"Invalid allocation metric in {path}")
        forks = metric["rawData"]
        if len(forks) < args.minimum_forks or any(len(fork) < 3 for fork in forks):
            raise ValueError(f"Insufficient forks/measurements in {path}")
        key = (row["benchmark"], row["params"]["content"])
        if key in found:
            raise ValueError(f"Duplicate fixture: {key}")
        means = [sum(fork) / len(fork) for fork in forks]
        if any(not math.isfinite(value) for fork in forks for value in fork):
            raise ValueError(f"Non-finite allocation sample in {path}")
        found[key] = means
    return found


baseline = metrics(args.baseline, "RAW")
encoded = metrics(args.encoded, "ENCODED")
if not baseline or baseline.keys() != encoded.keys():
    raise ValueError("Baseline and encoded fixtures must match and be nonempty")
required = {(method, content) for method in ("fullMaterialize", "uncachedSelection")
            for content in ("ZERO", "REPEATED")}
observed = {(key[0].rsplit(".", 1)[1], key[1]) for key in baseline}
if not required <= observed:
    raise ValueError("Full materialization and uncached selection require both content patterns")
failed = []
for key in sorted(baseline):
    main_max = max(baseline[key])
    encoded_max = max(encoded[key])
    delta = encoded_max - main_max
    passed = all(value <= main_max + PROFILER_NOISE_BYTES for value in encoded[key])
    print(f"{'PASS' if passed else 'FAIL'} {key[0]} {key[1]} "
          f"main_max={main_max:.5f} encoded_max={encoded_max:.5f} "
          f"delta={delta:+.5f} B/op encoded_forks={encoded[key]}")
    if not passed:
        failed.append(key)
raise SystemExit(1 if failed else 0)
