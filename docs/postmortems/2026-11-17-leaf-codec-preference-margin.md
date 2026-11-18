# Bug: negligible savings selected the stronger leaf decoder

- Fix / issue: review R2, S1 HOT leaf payload codec.
- Symptom: LZ4 replaced zero-run even for a one-byte improvement, paying a stronger decoder's cost without a clear space gain.
- Trigger: a compressible payload with LZ4 beating zero-run by at most the existing five-byte payload margin.
- Root cause: the shared HOT writer implemented a strict size minimum but omitted the required cheap-decoder preference. Overflow's existing five-byte margin was an inline expression rather than a shared constant.
- Why tests missed it: the codec-choice boundary test covered equality only; the raw golden covered framing equality and the first compression win but not margin-minus-one explicitly.
- Regression test: HOTLeafPayloadCodecTest.strongerCodecMustBeatZeroRunByMoreThanFiveBytes uses real independently measured native/zero-run payload lengths to exercise savings of four, five and six bytes and asserts the wire codec and full round trip. It rejects the pre-fix LZ4 choice at four and five bytes. rawFallbackAccountsForFramingAndZeroRunGoldenPinsTheBoundary pins raw/encoded bytes for savings of four, five and six bytes against raw. The native-only comparison is explicitly skipped without native LZ4. Run the focused codec tests; execution results are recorded separately.
- Lesson: test both boundaries of a codec policy, including framing and the preference between decoders. See [Codec preference is a separate boundary](../LESSONS.md#codec-preference-is-a-separate-boundary).

## Focused review verification

All 20 codec tests passed with native LZ4 available, including the four-, five-
and six-byte witnesses. PIT killed all six boundary, arithmetic and conditional
mutants on the two margin comparisons. The unchanged writer rows retained by
this method-wide scope have the following dispositions:

| Method, line/index | Status / mutation | Disposition |
| --- | --- | --- |
| writePage, 68/65 | survived / conditional boundary | At six raw bytes neither codec plus its five-byte framing can win; either arm emits raw. |
| writePage, 85/161 | survived / conditional boundary | A positive valid input and compressBound-sized native destination produce positive LZ4 output; zero needs a native failure or undersized output, neither injected here. |
| writePage, 112/218 | uncovered / removed close | Compiler-duplicated cleanup arm reached only if closing valid borrowed codec scratch throws; ordinary cleanup and a throwing sink are covered, no throwing-close injection claimed. |
| writePage, 112/327 | uncovered / removed close | Same throwing-close-only compiler duplication, distinct bytecode site. |
| writePage, 112/357 | uncovered / removed close | Same throwing-close-only compiler duplication, distinct bytecode site. |

The complete local mutation ledger and execution log are retained alongside the
[ownership fix's verification record](2026-11-17-leaf-codec-retained-frames.md#focused-review-verification).
The outer executor must regenerate the census and cold probe with this policy;
no updated space or timing result is claimed by this review phase.

## Follow-up 2026-11-18: the premise of this fix was wrong

The fix above made LZ4 beat zero-run by five bytes because zero-run was taken to be the cheap
decoder. That was the plan's wording, never a measurement. When the margin was to be replaced by a
measured one, the measurement inverted it.

- Method: 8,192 real kind-12 leaves of the t100k comparison store (58.74 MB of slot payload), each
  re-framed under a forced codec, decoded through the ordinary reader in memory; ten fresh JVMs per
  decoder in exclusive timing windows, one core.
- Result, per leaf, median of the ten forks: raw copy 1,433 ns, native LZ4 4,814 ns, zero-run
  16,003 ns, Java LZ4 as it was 24,058 ns and rewritten 10,632 ns. Zero-run is 3.3 times slower
  than native LZ4 in every size class, and no fork of one overlaps a fork of the other.
- Sizes on the same leaves: zero-run alone saves 37 % of the payload, LZ4 61 %. Where LZ4 can be
  written, also offering zero-run adds less than 0.01 MB.
- Consequence for a cold read on the store's device (1.56 ns per byte not read): a zero-run leaf
  reads slower than its raw form, and an LZ4 leaf breaks even when it saves about a third of the
  payload.

Correction: the leaf writer emits LZ4 or raw. LZ4 is kept when it saves at least a third of the
payload, framing included (`HOTLeafPayloadCodec.LZ4_MIN_SAVING_DIVISOR`, with its derivation). A
host without native LZ4 writes raw leaves. Readers keep decoding zero-run frames. The Java LZ4
fallback decoder was rewritten in the same round, because a store written with native LZ4 is also
read where only the fallback exists, and the old fallback was slower than zero-run.

Tests: `strongerCodecMustBeatZeroRunByMoreThanFiveBytes`, `codecSizeTiesPreferZeroRun` and the
zero-run raw-boundary golden are replaced by `lz4IsKeptExactlyWhenItSavesAThirdOfThePayloadFramingIncluded`,
`theWriterAppliesTheRuleToTheBlockNativeLz4Produces`, `theWriterNeverEmitsZeroRun`,
`aHostWithoutNativeLz4WritesRawPages` and `tinyAndEmptyPayloadsStayRaw`. Reader tests build zero-run
and LZ4 frames themselves, so they run the same on hosts that cannot write LZ4.

Why the first fix was not caught: its review and tests checked that the code matched the stated
preference, and nothing checked the preference. The lesson above is extended accordingly.
