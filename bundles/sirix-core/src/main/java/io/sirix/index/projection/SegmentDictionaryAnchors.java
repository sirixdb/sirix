/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Where a segment's dictionary actually lives: {@code (segment, column)} to the header key it was
 * committed under, and to the entry count it held when it was sealed.
 *
 * <p>
 * A page written under {@link SegmentScopedDictionaries} records its SEGMENT as its anchor, because
 * at page-encode time the segment's dictionary has not been written and has no storage key yet.
 * This is the table that closes the gap: the writer fills it as each segment is persisted, and the
 * read side translates a page's segment anchor into the header key {@code GlobalValueDictionary}
 * resolves against.
 * </p>
 *
 * <h2>Why the sealed count is stored beside the key</h2>
 *
 * A page also records the entry count it saw. The reader's validity rule is that the dictionary
 * must hold at least that many entries — a live count BELOW a recorded one means the key was reused
 * by something else, and ids resolved against it would be plausible and wrong. A segment dictionary
 * is sealed once and never appended to afterwards, so its sealed count is the whole answer and no
 * dictionary read is needed to perform the check.
 *
 * <p>
 * Segments are sealed by the flush pipeline in no particular order (a segment is done when its last
 * page has been ENCODED, not when the writer passes its last row), so this is written concurrently
 * and read concurrently, and a segment absent from it is simply one whose pages must keep their
 * bytes.
 * </p>
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class SegmentDictionaryAnchors {

  /** Answer for a segment/column that was never sealed. */
  public static final long NO_HEADER_KEY = 0L;

  private record Anchor(long headerKey, int sealedEntryCount) {
  }

  private final ConcurrentHashMap<Long, Anchor> anchors = new ConcurrentHashMap<>();

  private static Long key(final long segment, final int column) {
    if (segment < 0) {
      throw new IllegalArgumentException("segment must not be negative: " + segment);
    }
    if (column < 0 || column > 0xfffff) {
      throw new IllegalArgumentException("column out of range: " + column);
    }
    return (segment << 20) | column;
  }

  /**
   * Record where a sealed segment dictionary lives. Sealing happens once per {@code (segment,
   * column)}; a second seal with a different key is a pipeline defect, not a legitimate update, and
   * is refused rather than silently repointing pages that already carry the first key's ids.
   *
   * @param headerKey the committed dictionary's header key, never {@link #NO_HEADER_KEY}
   * @param sealedEntryCount entries the dictionary held when sealed
   */
  public void seal(final long segment, final int column, final long headerKey, final int sealedEntryCount) {
    if (headerKey == NO_HEADER_KEY) {
      throw new IllegalArgumentException("a sealed segment dictionary needs a header key");
    }
    if (sealedEntryCount < 0) {
      throw new IllegalArgumentException("sealedEntryCount must not be negative: " + sealedEntryCount);
    }
    final Anchor sealed = new Anchor(headerKey, sealedEntryCount);
    final Anchor previous = anchors.putIfAbsent(key(segment, column), sealed);
    if (previous != null && !previous.equals(sealed)) {
      throw new IllegalStateException("segment " + segment + " column " + column + " was already sealed at header key "
          + previous.headerKey() + " with " + previous.sealedEntryCount() + " entries; refusing to reseal at "
          + headerKey + " with " + sealedEntryCount);
    }
  }

  /**
   * The sealed dictionary's header key, or {@link #NO_HEADER_KEY} when the segment was never sealed.
   */
  public long headerKeyOf(final long segment, final int column) {
    final Anchor anchor = anchors.get(key(segment, column));
    return anchor == null
        ? NO_HEADER_KEY
        : anchor.headerKey();
  }

  /** Entries the sealed dictionary held, or {@code 0} when the segment was never sealed. */
  public int sealedEntryCountOf(final long segment, final int column) {
    final Anchor anchor = anchors.get(key(segment, column));
    return anchor == null
        ? 0
        : anchor.sealedEntryCount();
  }

  /**
   * Whether a page recording {@code recordedEntryCount} against this segment may be resolved: the
   * segment must be sealed, and it must hold at least what the page saw.
   */
  public boolean accepts(final long segment, final int column, final int recordedEntryCount) {
    final Anchor anchor = anchors.get(key(segment, column));
    return anchor != null && anchor.sealedEntryCount() >= recordedEntryCount;
  }

  /** Sealed {@code (segment, column)} pairs (test observability). */
  public int sealedCount() {
    return anchors.size();
  }
}
