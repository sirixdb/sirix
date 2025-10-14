# Diagnostic Approach - Remove Clearing to Find Real Bug

## Decision: No fill(), No madvise

Changed `resetSegment()` to complete NO-OP:

```java
@Override
public void resetSegment(MemorySegment segment) {
    // NO-OP: Trust offset tracking
}
```

## Rationale

### If Offset Tracking is Correct

**Theory:** slotOffsets array ensures we only read valid slots:
```java
Arrays.fill(slotOffsets, -1);  // Mark all invalid
// ... only set offsets for written slots ...
if (slotOffsets[slot] < 0) return null;  // Don't read invalid
```

**If this works:** Stale data never accessed → no corruption → fill() was unnecessary overhead!

### If Corruption Occurs

Added defensive checks to diagnose:

```java
// Check 1: Offset bounds
if (slotOffset + INT_SIZE > slotMemory.byteSize()) {
    throw "CORRUPT OFFSET";  // Offset itself is wrong
}

// Check 2: Length sanity
if (length < 0 || length > slotMemory.byteSize()) {
    throw "CORRUPT LENGTH";  // Reading garbage data
}
```

**Error will show us:**
- Offset corruption → fix offset tracking
- Length corruption → investigate why we're reading there
- Race condition → add synchronization

## Current Error Analysis

Previous error: `offset=180, length=4888621, memory_size=131072`

**Offset 180:** Valid (< 131072) ✓
**Length 4888621:** WAY too large (> 131072) ❌

**This means:**
- Offset tracking put a valid offset (180) in slotOffsets
- But the data at offset 180 has garbage length value
- Either:
  1. Data at 180 was never written (offset set prematurely)
  2. Data at 180 was overwritten/corrupted
  3. Reading wrong memory segment (reuse bug)

## Expected Test Results

### Scenario 1: Test Passes

**Meaning:** Offset tracking IS correct, fill() was unnecessary
**Result:** HUGE performance win! Zero overhead on reuse
**Action:** Done! Ship it!

### Scenario 2: "CORRUPT OFFSET" Error

**Meaning:** slotOffsets has out-of-bounds values
**Result:** Fix offset tracking logic
**Action:** Investigate how offsets get set incorrectly

### Scenario 3: "CORRUPT LENGTH" Error

**Meaning:** Valid offset but garbage data
**Result:** Either:
- Data not written before offset set
- Parallel modification
- Wrong segment assigned
**Action:** Add synchronization or fix write ordering

### Scenario 4: Same Error (extends beyond)

**Meaning:** Length check passed but still invalid
**Result:** Race condition between read and segment reassignment
**Action:** Add synchronization to page lifecycle

## Performance

**NO-OP resetSegment():**
- Zero overhead ✓
- Best possible performance ✓
- If it works: MemorySegments >> byte arrays ✓

**vs fill():**
- ~200μs overhead per page reuse
- Acceptable but not optimal
- Would hide the real bug

## Next Steps

1. Run test with NO-OP resetSegment()
2. Observe error (if any)
3. Error message will guide us to real fix
4. Fix root cause properly
5. Keep NO-OP if test passes!

## Files Modified

- `LinuxMemorySegmentAllocator.java` - resetSegment() → NO-OP
- `KeyValueLeafPage.java` - Enhanced defensive checks

## Commits

`[current]` - Remove fill() entirely, diagnostic approach

