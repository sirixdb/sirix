# Bug: HOT leaf read paths accepted different V0 frames

- Fix and issue. The [kind-12 framing contract](../../spec/FMT-disk-format.md#fmt-211--hot-leaf-directory-and-slot-framing)
  specifies one directory width, bounded stop-bit fields, a sorted side
  map and exact frame consumption.
- Symptom. Whole-page decoding accepted a beta directory, extra bytes
  after an ordinary leaf, repeated side keys and stop-bit integers
  beyond their field widths. A cached projection image already rejected
  some of those inputs. Read behavior depended on the cache path.
- Trigger. Supply an ordinary leaf using a beta directory, a frame
  longer than its sections, repeated trailer keys, or a stop-bit page
  key or count longer than its field permits.
- Root cause. The materializer still used two widths and an unbounded
  generic stop-bit reader. It also stopped reading once it could
  construct a leaf. The selective projection reader and immutable
  projection image had separate trailer validation. The materializer's
  map insertion altered the meaning of duplicate trailer keys.
- Why tests missed it. Earlier tests treated the beta and mode-zero
  formats as alternate encodings and checked either logical values on
  valid frames or isolated corruption conditions on a single path.
- Regression test. Run `HOTLeafDirectoryEncodingTest` through the core
  test task. The four new framing cases failed on the previous reader.
  They now exercise the whole-page reader, borrowed projection lookup
  and cached projection image with heap and segment inputs. The suite
  also pins empty and nonempty images and the legal case of relocated
  slots with gaps.
  All framing cases passed in the 463-case focused core run.
- Lesson. [Use the same envelope and trailer bounds at each page read
  entry point](../LESSONS.md#keep-the-hot-leaf-frame-contract-across-read-paths).

The first paired fixture smoke also found that the candidate benchmark
constructed and opened both images. Its unused beta control failed
before the candidate could run. Each benchmark arm now initializes
only the image it measures. The shared benchmark smoke runs both
arms against their own production classes before a scored run.
