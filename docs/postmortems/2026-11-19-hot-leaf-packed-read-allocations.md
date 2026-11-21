# Packed HOT readers allocated transient wire views

- The normal-width clean fork of `HOTLeafCodecAllocationTest.encodedPathsAddNoAllocation`
  recorded 168 extra bytes per packed borrowed projection lookup. Compact packed fragments
  allocated 44 to 48 extra bytes per read across the two fixtures.
- The borrowed selector constructed a body descriptor, a heap segment wrapper and a
  segment slice even though only the selected value escaped. The compact reader created
  a descriptor and slice for the input, then a second descriptor for the copied frame.
  Eliminating the redundant int directory did not cover those wrappers.
- A synchronous borrowed selector now uses confined scratch and clears its backing
  before returning the independent result. Array-backed frames use the array directly.
  A materializing reader adopts native input when it owns the frame, or rebases the
  existing unpublished descriptor to its independent copy.
- The existing clean-fork gate compares the same raw and packed operations and reports
  all eight allocation deltas. The documented 128-bit fork runs the same assertion.
- See [the ownership lesson](../LESSONS.md#reuse-unpublished-packed-wire-views).
