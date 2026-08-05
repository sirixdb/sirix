/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Staging for region payloads in tests, laid out the way {@link RegionTable} lays them out.
 *
 * <h2>Why one place</h2>
 *
 * <p>Two details here are load-bearing and neither is obvious. Payloads are <em>native</em>: a
 * vector load takes a different intrinsic against a heap segment than a native one, so a test that
 * staged its fixture with {@code MemorySegment.ofArray} would assert against a code path the engine
 * never runs. And the allocation carries tail slack past the visible length, because
 * {@link BitUnpackSimd}'s two-load window and the native LZ77 decoder both read past the last byte
 * they decode.
 *
 * <p>Both constants belong to {@code RegionTable.allocate}. Copied into each test file — as they
 * were, nine times — they become nine places to miss when the layout changes, and a fixture that
 * silently dropped the slack would still pass while exercising something else.
 */
final class PaxTestSegments {

  /**
   * Tail slack past the visible payload, matching what {@code RegionTable} reserves.
   *
   * <p>Deliberately at least as large as the decoder's requirement rather than exactly equal to it:
   * a test fixture that under-reserves does not fail, it just stops reproducing production layout.
   */
  private static final int TAIL_SLACK = 64;

  private PaxTestSegments() {
    throw new AssertionError("no instances");
  }

  /**
   * A native payload segment holding {@code bytes}, sized to {@code bytes.length} with slack past
   * the end.
   *
   * @return the segment, or {@code null} when {@code bytes} is {@code null}, so a test can pass an
   *         absent region straight through
   */
  static MemorySegment of(final byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    final MemorySegment target =
        Arena.ofAuto().allocate((long) bytes.length + TAIL_SLACK, Long.BYTES)
             .asSlice(0, bytes.length);
    MemorySegment.copy(bytes, 0, target, ValueLayout.JAVA_BYTE, 0, bytes.length);
    return target;
  }

  /** The segment's bytes, so assertions can keep comparing arrays. */
  static byte[] bytes(final MemorySegment segment) {
    return segment == null ? null : segment.toArray(ValueLayout.JAVA_BYTE);
  }
}
