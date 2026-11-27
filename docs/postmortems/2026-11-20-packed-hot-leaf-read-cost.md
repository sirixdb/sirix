# Packed HOT leaf reads repeated structural work

- **Symptom.** The paired leaf gate against main `ec746e786158` rejected ten
  of twelve endpoints. Full materialization rose from 1.01 µs to 11.19 µs
  in the repeated-value fixture. Warm eight-byte leaf lookup and value
  access regressed by 24.2% and 38.5%. Verified frame copying improved by
  about 87%.
- **Trigger.** Read a packed kind-12 leaf with narrow lengths and shared
  values. The full fixture has 128 eight-byte keys and one inline owner
  for 127 aliases. The compact and projection-select fixtures have 32
  one-byte keys with repeated or zero values.
- **Root cause.** A value alias addressed the owning slot's payload
  offset. Finding its directory entry required a second binary search.
  JFR sampled that search in 60% of full cold stacks and 42–45% of
  borrowed selection stacks. The full validator read the same slot bounds
  through both its key and value checks. Cached short-key selects still
  used the checked wire reader. Fixed-word native search still made
  per-slot width and group decisions.
- **Why tests missed it.** Golden, differential and work-budget cases
  checked logical answers and bounded key probes. The suffix probe counter
  does not count alias-owner searches or repeated header reads.
- **Correction and regression.** The two-byte alias field now addresses
  the earlier directory entry. Validate in directory order and reuse the
  most recently checked inline owner. Full fixed-word leaves retain a
  short directory and use the validated width for each bounded word load.
  Prepared projection images validate once and use precomputed key and
  range lanes for uniform one- to eight-byte keys. The packed-wire suite
  checks a nonzero owner, odd and out-of-range pointers, chained aliases,
  and a repeated owner's declared length. The full-image suite checks
  exact bounds under both the retained directory and the generic lookup.
  The second paired run passed eight of twelve endpoints. Full cold
  materialization fell from 11.19 µs to 1.69 µs against 1.04 µs on main.
  The cold profile attributed 67.2% of candidate stacks to packed-body
  validation. The remaining repeated-owner check read and compared the
  target and one-byte length separately for each alias.
  The validator now proves the alias span before loading its length and
  target together. A repeated exact pair reuses the earlier validation.
  The native conformance fixture covers a nonzero frame base, an exact
  tail and malformed target and length changes after a valid alias.
  The next paired run measured full cold materialization at +51.92%,
  and both borrowed selectors regressed by 4.74–5.49%. C2 inlined the
  alias load, while the borrowed search grew after switching from typed
  short loads to separate byte loads. Writers now keep fixed-eight-byte
  projection values inline, with the shared forms still readable. The
  borrowed selector uses one little-endian 32-bit load for adjacent
  directory entries and a separate bounded 16-bit load at the last slot.
  Regression cases cover native shared frames with both value widths,
  the writer's inline eligibility, and adjacent unsigned offsets above
  32 KiB. The first conformance run found that the sparse alias fixture
  still used eight-byte projection keys. Its encoder correctly wrote
  both values inline, so the ownership test could no longer reach its
  intended alias path. The fixture now uses eligible seven-byte keys
  and still asserts the sharing bit before checking fragment ownership.
  The next paired run passed all warm and borrowed endpoints.
  Full cold materialization was still 39.06% slower after the writer
  switched to inline values. The remaining fixture paid for packed
  validation with narrow slot lengths alone. The writer now uses mode
  zero with a narrow directory for this cohort. The writer regression
  pins the flag byte and distinct inline value locations. Older packed
  frames still have native reader coverage. The streaming projection
  fixture failed in all four versioning modes because it required
  packed lengths at the on-disk root. The test now asserts mode zero
  and keeps its stored-size, checksum and historical-revision checks.
  This eligibility change needs a new paired measurement.
- **Lesson.** Count metadata work separately from key and value decoding.
  See [direct directory references](../LESSONS.md#let-pointers-name-the-directory-entry).
