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
  declares, and against what the bytes after that frame header could actually produce, *before*
  reserving either the decoded array or the native landing — so neither a garbled length nor two
  agreeing lengths with an empty token stream can drive a large reservation from a tiny record.
  Previously a projection or ordinary overflow page whose frame asked for more bytes than it carried
  decoded successfully and published bytes left in the decode scratch by an earlier payload, and a
  frame declaring a gibibyte in both headers reserved it before the decoder could refuse.
  Decoding speed is unchanged: the native LZ77 decoder gained a second entry point that carries
  these input bounds inside the same vectorised loop, selected at compile time, so a valid payload
  still decodes natively and a corrupt one is refused rather than decoded. Platforms without the
  bundled library give the identical refusals from the Java decoder.
