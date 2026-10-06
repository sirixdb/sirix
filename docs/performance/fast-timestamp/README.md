# Stored timestamp fast path

`StoredDateTimeParser` admits only 20-byte `YYYY-MM-DDTHH:MM:SSZ` values with a valid
Gregorian date, years 0001–9999 and hours 00–23. It reads the storage cursor's UTF-8
bytes and produces epoch milliseconds without a string, substring, calendar or
boxed number. `AtomicStrJsonDBItem` retains that primitive alongside the original
string and lazily constructs a Brackit `DateTime` with its component constructor.
Every other spelling goes to `DateTime(String)`, including offsets, fractions,
absent zones, expanded/negative years, whitespace, end-of-day spellings and errors.
Successful casts are memoized on that immutable field value; errors are not.

The Sirix translator uses the memo for the `xs:dateTime` constructor and explicit
`cast as xs:dateTime`. Constructor dispatch retains Brackit's function argument
conversion and signature. Other types and values keep Brackit's ordinary cast.
Read-only `JsonDBObject` field values belong to a fixed revision. Cursor mutability is classified once in the constructor and encoded in the existing
first-child cache slot, keeping interface checks off repeated hits without growing
every object wrapper. Write cursors bypass the field memo because another wrapper or the cursor itself can update a
field without notifying this wrapper. Existing mutation methods also clear their memo and avoid allocating an unused map
for writers. A previously returned string remains an immutable value snapshot.

`ValidTimeIndexScan` reuses fixed UTC epochs and compares seconds and nanoseconds:
it never truncates the probe instant to milliseconds. Non-fixed bounds keep the
original `Instant.parse` path, including its nanosecond precision, rejection of
zone-less strings and treatment of unparseable bounds. Missing/unparseable bounds
remain open-ended, and a record with no bounds remains invalid.

The projection timestamp codec accepts both 19-byte zone-less text and the same
text suffixed with `Z`. Both parse to the same epoch second, and its suffix-aware
formatter can round-trip either spelling when the caller retains that suffix.
The persisted epoch lane contains no spelling bit. Extraction therefore marks a
`Z` cell with the existing unrepresentable flag, so text-sensitive query routes
fall back to its original record. This preserves emission, comparison, grouping
and ordering of mixed spellings without changing the persisted format. Zone-less
columns retain their existing projection routes. Fractions and offsets still fail
the declared timestamp-column build.

## Verification plan

All JVM commands run through the dispatch brief's `heavy` limiter: at least 6 GiB
of available memory, and one of `/var/tmp/fm-heavy-jvm.{1,2}.lock`. Builds use an
initially empty private Maven repository at `build/fast-timestamp/m2-private`;
they never install into or modify `~/.m2`. The resolved Brackit publication is
`1.0-alpha10-20261006.152144-93`. Test forks use `-PtestHeapMin=512m
-PtestHeapMax=2g`, with one test class per fork and one parallel fork, bounding
retained transaction/cache state across the full suites.

Correctness covers general-parser differential values and error codes, all four
versioning strategies, other-wrapper and direct-cursor writes, historical reads,
shared constructor/cast identity, valid-time nanosecond boundaries and fallback,
and projection text differentials after build, reopen and update. The allocation
budget measures thread allocation units, never wall-clock time: the fixed byte
parser and repeated memoized field casts allocate zero bytes after warmup. The
ordinary parser is an executable allocation witness.

The before SH1 runtime was copied before production changes from base
`842f48e080ef3897c4fc421d05650dc2a7c35e33`. Both variants use the same published
Brackit snapshot, input stream, database, query catalog and independent Python
oracle. The unmodified kit's generation/loading logic runs with only its campaign
root constant relocated into this worktree by the private harness. Diff sidecars
remain off as required by the kit; publication batching, storage and indexes are
unchanged. Input and store live in `build/fast-timestamp/campaign`.

The timing probe is the existing `docs/performance/cheap-first` `LatencyProbe`.
Each variant runs all twelve SH1 queries seven times in one resident JVM, validates
every repetition byte-for-byte against the independent TSV oracle, discards the
first three repetitions and reports the median of the remaining four. Total time
includes compile, execute/materialize and serialize; canonicalization is measured
separately and excluded, matching the existing probe. JVM flags:
`--enable-preview --add-modules jdk.incubator.vector
--enable-native-access=ALL-UNNAMED -Xms512m -Xmx2g -XX:MaxDirectMemorySize=1g`.
The laptop is shared; no power/energy settings or other workers' processes change.
Timings are evidence, not test assertions.

Results and completed checks are recorded alongside this file. Work budgets run with the bounds in `docs/VERIFICATION.md` unchanged. The valid-time
budget's timestamp-read capture now observes both `getValue` and `getValueBytes`;
its zero, exact and minimum bounds are unchanged, and its executable positive
control proves that each accessor is counted. The first run exposed nine stale
positive controls that only watched `getValue`, while the new zero checks also
cover byte reads. The private budget init script narrows candidate class files to
exactly the existing budget package/classes, retaining the Gradle test filters;
this avoids starting empty forks for unrelated classes. Full suites have no such
candidate narrowing.

## Measured results

T100k, same immutable runtime dependencies, store, query text and oracle. Every
repetition in the before and delivered all-query runs is byte-exact (84 each).
Medians below use repetitions 3–6; milliseconds include compile, materialize and
serialize. Raw repetitions and source hashes are in `measurements.json`.

| Query | Before (ms) | Delivered (ms) | Change |
| --- | ---: | ---: | ---: |
| Q1 | 145.293 | 121.090 | -16.7% |
| Q2 | 82.615 | 66.485 | -19.5% |
| Q3 | 420.965 | 272.583 | -35.2% |
| Q4 | 606.825 | 421.379 | -30.6% |
| Q5 | 1520.293 | 1510.246 | -0.7% |
| Q6 | 4422.922 | 4566.495 | +3.2% |
| Q7 | 217.794 | 232.450 | +6.7% |
| Q8 | 291.375 | 318.505 | +9.3% |
| Q9 | 291.039 | 294.433 | +1.2% |
| Q10 | 1179.829 | 907.177 | -23.1% |
| Q11 | 14469.764 | 13346.767 | -7.8% |
| Q12 | 178.613 | 169.452 | -5.1% |

Q10 is the remaining timestamp-heavy join: 1,179.829 → 907.177 ms in the full
run (23.1% lower). A separate nine-repetition timestamp/control run, discarding
three warmups, measured 1,072.227 → 963.811 ms (10.1% lower). The host was shared
and unaffected control queries also varied, so these are observed medians rather
than an isolated causal estimate for every query. Q5 was effectively unchanged.
Allocation and work-budget tests supply the deterministic regression evidence.

The initial writer-interface check on every cache hit produced a repeatable Q12
control penalty (154.567 → 224.450 ms). Cursor classification now uses the
existing first-child cache state, and the guard runs only on a cache miss. The
delivered full-run Q12 median is 169.452 ms versus 178.613 ms before; the separate
control repeat was 164.033 ms versus 154.567 ms. No extra object field was added.
Prototype measurements are labelled separately in the JSON evidence.

## Completed checks

Full `sirix-core` (13,157 tests; 78 existing skips) and `sirix-query` (3,002 tests;
12 existing skips) suites passed with zero failures or errors. Both Spotless
checks passed. Every test-class result is in `verification.json`.
All work-budget bounds stayed unchanged. No wall-clock assertions were added.
The query budget meter now covers both string and byte value reads.

The branch is committed for Firstmate handoff; no-mistakes publication/CI follows
that handoff, and merging remains subject to explicit approval.
