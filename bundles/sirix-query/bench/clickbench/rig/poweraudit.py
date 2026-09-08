#!/usr/bin/env python3
"""Per-leg power-limit audit over retained telemetry: python3 poweraudit.py TELEMETRY.jsonl...

A leg's attestation names the MSR long-term cap the rig verifies, but the package is also bounded by
the MMIO domain, which the rig never sets and which the platform moves on its own. The effective
envelope is the LOWER of the two, so a leg can run under a tighter cap than its attestation states.
This reads the sysfs limit readings already retained in telemetry and reports, per file, the distinct
values observed and how many samples had an MMIO long-term limit below the MSR long-term limit.

These are sampled readings of configured limits. They are not measured package power, they do not
prove uninterrupted enforcement between samples, and they do not identify what changed a limit.
Rows that never recorded a limit are counted as unsampled, not as agreement.
"""
import json
from pathlib import Path
import sys

MSR_LONG = '/sys/class/powercap/intel-rapl:0/constraint_0_power_limit_uw'
MSR_SHORT = '/sys/class/powercap/intel-rapl:0/constraint_1_power_limit_uw'
MMIO_LONG = '/sys/class/powercap/intel-rapl-mmio:0/constraint_0_power_limit_uw'


def limits(row):
    """Merge the mappings a telemetry row may carry its sysfs limit readings in."""
    merged = {}
    for key in ('values', 'power_domains_uw'):
        section = row.get(key)
        if isinstance(section, dict):
            merged.update(section)
    return merged


def audit_file(path):
    samples = unsampled = below = 0
    observed = {}
    for line in Path(path).read_text().splitlines():
        if not line.strip():
            continue
        reading = limits(json.loads(line))
        samples += 1
        msr, mmio = reading.get(MSR_LONG), reading.get(MMIO_LONG)
        if msr is None or mmio is None:
            unsampled += 1
            continue
        key = (str(msr), str(reading.get(MSR_SHORT)), str(mmio))
        observed[key] = observed.get(key, 0)+1
        if int(mmio) < int(msr):
            below += 1
    return dict(file=str(path), samples=samples, unsampled=unsampled, mmio_below_msr=below,
                observed=[dict(msr_long_uw=k[0], msr_short_uw=k[1], mmio_long_uw=k[2], count=n)
                          for k, n in sorted(observed.items())])


def audit(paths):
    files = [audit_file(path) for path in paths]
    scored = [row for row in files if row['samples'] > row['unsampled']]
    return dict(files=files,
                totals=dict(files=len(files), files_with_readings=len(scored),
                            files_with_mmio_below_msr=sum(1 for row in scored if row['mmio_below_msr']),
                            samples=sum(row['samples'] for row in files),
                            unsampled=sum(row['unsampled'] for row in files),
                            mmio_below_msr=sum(row['mmio_below_msr'] for row in files)))


def main():
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    print(json.dumps(audit(sys.argv[1:]), indent=2))


if __name__ == '__main__':
    sys.exit(main())
