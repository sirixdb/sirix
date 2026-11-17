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
