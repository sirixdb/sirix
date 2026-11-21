# HOT projection corruption fixture used the wrong directory width

- The initial targeted sweep stopped in
  `HOTProjectionImageTest.malformedWordSlotsCannotPublishUncheckedRanges`
  while constructing a malformed fixture. The other 1,934 tests passed.
- The fixture opened a two-entry projection frame, then attempted to exceed
  the slot region by changing one value length.
- The slot-region start was based on a two-byte directory stride, but an
  int load was used to read the slot offset. That load combined two valid
  directory entries into an invalid address. The test threw while locating
  the target length field, before it had exercised the reader.
- The previous writer emitted four-byte offsets, so running the fixture
  against its own writer output could keep the hard-coded load width hidden.
- The focused projection test now writes an ordinary narrow control and
  uses an unsigned short to locate the target. The packed body suite uses
  independent golden frames for malformed directory and alias pointers.
- See [the lesson](../LESSONS.md#corrupt-wire-fixtures-must-pin-their-layout).
