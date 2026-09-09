"""junit.py <started-epoch> <fully.qualified.TestClass>... : print JUnit results, refusing stale XML.

A Gradle test run that fails to COMPILE leaves the previous run's TEST-*.xml untouched, so reading the
XML reports the OLD verdict as if it were new -- a green report over a red build. This refuses any
report older than the run's start (`date +%s` taken before ./gradlew) and prints STALE OR MISSING;
grep the gradle log for `error:` when it does. Short class names always report STALE.
"""
import xml.etree.ElementTree as ET, sys, os
ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), *[".."] * 5))
start = float(sys.argv[1]); bad = 0
for cls in sys.argv[2:]:
    mod = 'sirix-query' if cls.startswith('io.sirix.query.') else 'sirix-core'
    f = f'{ROOT}/bundles/{mod}/build/test-results/test/TEST-{cls}.xml'
    if not os.path.exists(f) or os.path.getmtime(f) < start:
        print(f'STALE OR MISSING RESULT for {cls} (the build did not produce it: read the gradle log)'); bad += 1; continue
    r = ET.parse(f).getroot()
    fails = int(r.get('failures')) + int(r.get('errors')); bad += fails
    print(f"{cls.split('.')[-1]}: {r.get('tests')} tests, {fails} failed  ({r.get('timestamp')})")
    for tc in r.findall('testcase'):
        x = tc.find('failure') if tc.find('failure') is not None else tc.find('error')
        if x is not None: print('   FAIL', tc.get('name'), '::', (x.get('message') or '')[:300].replace('\n', ' '))
sys.exit(1 if bad else 0)
