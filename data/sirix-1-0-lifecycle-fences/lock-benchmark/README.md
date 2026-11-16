# R15 bounded lock comparison

This is one focused JMH comparison of the timed mutation and nested commit/mutation lock path.
The baseline is independently compiled from a574857c, with only the class-name tokens renamed.
The same fixture runs both implementations on the same machine. It does not measure I/O,
contention, tail latency or whole-database throughput.

From the repository root, prepare the baseline inside the worktree:

```python
from pathlib import Path
import subprocess
source = subprocess.check_output([
    "git", "show",
    "a574857c716a640c6d92e1a46262518dee598200:bundles/sirix-core/src/main/java/io/sirix/access/trx/node/TransactionExecutionLock.java"
], text=True)
target = Path("build/r15-benchmark-src/io/sirix/access/trx/node/BeforeTransactionExecutionLock.java")
target.parent.mkdir(parents=True, exist_ok=True)
target.write_text(source.replace("TransactionExecutionLock", "BeforeTransactionExecutionLock"))
```

Run the following Gradle command through the operator's heavy.sh wrapper, with `df` and the
current disk-floor check immediately before Gradle. This review used the authorized 60 GB floor.
Do not run it alongside a benchmark campaign.

```sh
./gradlew -I data/sirix-1-0-lifecycle-fences/lock-benchmark.gradle \
  :sirix-benchmarks:r15LockBenchmark \
  --no-daemon --max-workers=2 -Dorg.gradle.jvmargs=-Xmx2g
```

The init file restricts benchmark sources to this fixture and the frozen baseline. It runs
one thread, two forks, three 500 ms warmups and five 500 ms measurements per fork/configuration.
Results are written under `build/r15-lock-benchmark.json`; the captured results and limitations
are in `../revision-view-lock-jmh.json` and `../revision-view-verification.json`.

Observed ns/op (reported JMH error): timed mutation before 27.103 ± 0.971, after 26.993 ± 0.419;
nested locking before 102.931 ± 8.256, after 101.001 ± 2.374. Confidence intervals overlap:
no measurable regression and no established speedup. These are results from this machine/JVM,
not a portable latency guarantee.
