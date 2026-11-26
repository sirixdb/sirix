---
section: Fixed
---
- **A corrupt compressed overflow payload is refused instead of decoded from reusable scratch.**
  Every payload codec now bounds each literal, length-extension and repeat span by the frame's
  declared stored length rather than by the array holding it, requires the run codecs' frames to be
  consumed exactly, and rejects byte-run counts below two, zero repeat bytes in nonzero repeat runs
  and the unassigned opcode, as
  [FMT-216](spec/FMT-disk-format.md#fmt-216--projection-overflow-envelope-and-codecs) requires.
  The reader also corroborates the page's decoded length against the size the codec frame itself
  declares *before* reserving either the decoded array or the native landing, so a garbled length
  can no longer drive a large reservation from a tiny record. Previously a projection or ordinary
  overflow page whose frame asked for more bytes than it carried decoded successfully and published
  bytes left in the decode scratch by an earlier payload. LZ77 overflow payloads decode through the
  bounds-checked core; the native decoder, whose C hot loop has no input bound, remains in use for
  frames this writer produced in the same process.
