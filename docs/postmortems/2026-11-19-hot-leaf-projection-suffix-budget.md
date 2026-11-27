# Grouped prefixes exceeded the projection word-probe work budget

- `HOTHistoricalBlobReadWorkBudgetTest.historicalLastAndSpanReadsUseBoundedSuffixLanes`
  captured 25 suffix reads for a last-key probe against a budget of eight.
  Every versioning policy hit the same failure.
- The original native lookup could read the suffix of an eight-byte key
  with one masked word load per search step. Grouping those keys moved
  part of each suffix into another region. The fallback read separate
  ranges and used short byte loops for each probe.
- The writer now selects contiguous suffixes for fixed-word projection
  leaves. Its last-word path can also end the load at the end of the
  suffix when a forward load would exceed the slot region.
- The existing work budget and its counters remain intact.
  `HOTLeafBodyEncodingTest` pins the writer decision, the last-key lookup
  and the upper insertion point with empty values. The CAS and VALIDTIME
  fixtures keep the grouped-prefix oracle.
- See [the word-probe lesson](../LESSONS.md#preserve-contiguous-keys-for-fixed-word-probes).
