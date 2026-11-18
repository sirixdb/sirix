# Bug: leaf compression was on by default and slowed fresh processes

- Fix / issue: PR #1454, the commit that makes leaf compression opt-in. Found before merge by the
  rehearsal of the native gate; no released version wrote encoded leaves.
- Symptom: with the leaf writer on, a fresh process on a warm page cache ran SH1 Q7 in 3.3 ms
  instead of 1.9 ms and Q8 in 377 ms instead of 298 ms (t250k, one native image, 13 and 5 processes
  per arm, ranges not overlapping). Loads were 2.6 % and commits 4.3 % slower in total (two loads per
  arm). Warm queries were unaffected. The goal for this change was that it must never cost latency.
- Trigger: any process that starts and loads index leaves that the operating system still has in its
  page cache. The cost has two parts: about 1.4 ms once per process for the first use of the LZ4
  decoder, and the decode of every loaded leaf (4.8 µs against 1.4 µs for a copy).
- Root cause: a wrong premise in the design, not a coding error. The plan credited the 40 % fewer
  leaf bytes to cold reads ("a decode replaces a copy"). That holds when the bytes come from the
  device, where an encoded store read 13 % faster per leaf. A fresh process usually gets them from
  the page cache, where fewer bytes save nothing and the decode is pure extra work. In the
  leaf-heavy query three quarters of the extra cycles are inside the LZ4 decoder itself.
- Why tests missed it: the latency evidence gathered first answered other questions. The warm
  lookup benchmark does not decode. The decode-cost comparison ranked codecs against each other,
  not against the gate. The store probe dropped the page cache, which is the regime that favours
  compression. The fresh-process endpoints of the native gate, which decide, were scheduled last.
- Regression test: `PageKindTest.thePageWriterStoresLeavesRawUnlessCompressionIsSwitchedOn` pins the
  default (`./gradlew :sirix-core:test --tests io.sirix.page.PageKindTest`); the same test asserts
  the encoded form in `:sirix-core:hotLeafCompressionEnabledTest`, where the setting is on. Before the
  change the default writer set the flag on that page. The native gate judges the default against
  main.
- Lesson: [Fewer bytes only pay where the bytes come from the device](../LESSONS.md#fewer-bytes-only-pay-where-the-bytes-come-from-the-device).
