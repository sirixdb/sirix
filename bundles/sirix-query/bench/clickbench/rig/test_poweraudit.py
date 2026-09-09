"""The effective envelope is the lower of the MSR and MMIO long-term limits, so the audit must see it."""
import json
from pathlib import Path
import tempfile
import unittest

from poweraudit import audit
from poweraudit import audit_file
from poweraudit import MMIO_LONG
from poweraudit import MSR_LONG
from poweraudit import MSR_SHORT


def row(msr_long, mmio_long, *, section='power_domains_uw'):
    if msr_long is None:
        return {'time': 1, 'values': {'/sys/class/thermal/thermal_zone12/temp': '61000'}}
    return {'time': 1, section: {MSR_LONG: str(msr_long), MSR_SHORT: str(msr_long),
                                 MMIO_LONG: str(mmio_long)}}


class PowerAuditTest(unittest.TestCase):
    def write(self, directory, name, rows):
        path = Path(directory)/name
        path.write_text(''.join(json.dumps(item)+'\n' for item in rows))
        return path

    def test_a_tighter_mmio_limit_is_counted_under_an_unchanged_msr_cap(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            path = self.write(directory, 'telemetry.jsonl',
                              [row(50_000_000, 45_000_000)]*3+[row(50_000_000, 76_000_000)])
            report = audit_file(path)
            self.assertEqual(report['samples'], 4)
            self.assertEqual(report['mmio_below_msr'], 3)
            self.assertEqual(report['unsampled'], 0)
            self.assertEqual(sorted(entry['mmio_long_uw'] for entry in report['observed']),
                             ['45000000', '76000000'])

    def test_an_equal_limit_is_not_below_it(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            path = self.write(directory, 'telemetry.jsonl', [row(50_000_000, 50_000_000)]*5)
            self.assertEqual(audit_file(path)['mmio_below_msr'], 0)

    def test_rows_that_never_sampled_a_limit_are_unsampled_not_agreement(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            path = self.write(directory, 'telemetry.jsonl', [row(None, None)]*7)
            report = audit_file(path)
            self.assertEqual((report['samples'], report['unsampled'], report['mmio_below_msr']), (7, 7, 0))
            self.assertEqual(report['observed'], [])
            self.assertEqual(audit([path])['totals']['files_with_readings'], 0)

    def test_both_retained_telemetry_layouts_are_read(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            historical = self.write(directory, 'historical.jsonl',
                                    [row(50_000_000, 45_000_000, section='values')]*2)
            current = self.write(directory, 'current.jsonl', [row(50_000_000, 45_000_000)]*2)
            self.assertEqual(audit_file(historical)['mmio_below_msr'], 2)
            self.assertEqual(audit_file(current)['mmio_below_msr'], 2)
            totals = audit([historical, current])['totals']
            self.assertEqual(totals['files_with_mmio_below_msr'], 2)
            self.assertEqual(totals['mmio_below_msr'], 4)


if __name__ == '__main__':
    unittest.main()
