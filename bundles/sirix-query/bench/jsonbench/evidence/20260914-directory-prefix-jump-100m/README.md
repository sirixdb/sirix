# Directory prefix skipping on 100M JSONBench

The sorted cursor now uses existing directory fences to jump to the last leaf whose first key
shares the current group prefix. It still reads that boundary leaf to locate the next group and,
when requested, capture the exact maximum. Intermediate data leaves and directory subtrees can
be skipped. The persisted format, bounded copy-on-write mutation unit, and validation of loaded
leaves are unchanged.

On the retained 99,999,968-row revision, one Q4 plus one Q5 read 59,796 leaves instead of 65,454
(8.64% fewer). The decoded leaf bytes fell from 780,441,394 to 750,287,710 (3.86% fewer). These
counts came from a diagnostic-only overlay and its query outputs matched the baseline exactly.

The separate JVM timing screen used baseline/candidate/candidate/baseline order, 16 tries per
query, identical database, heap, thread count, and frozen dependencies. Only the directory class
and its nested classes differed. Average best hot times improved by roughly 4%, but Q4's
later-run medians did not improve consistently. This is evidence of reduced storage work, not a
firm latency improvement or a new native/ClickHouse score.

Validation passed: 14 sorted-core tests, 12 query tests, exact Q4/Q5 results against the retained
ClickHouse reference in all four timing runs, and all 43 ClickBench queries against the retained
interpreter outputs on 200K rows. The retained DuckDB reference gave 33 exact matches and 10
strongly verified legal tie windows, with no mismatch or missing query. ClickBench performance
was not remeasured.

The new boundary test exercises prefixes ending within a leaf, crossings between directory
levels, empty and full-key prefixes, and old/new revisions after deleting boundary extrema.
