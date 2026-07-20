/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.api.StorageEngineWriter;
import io.sirix.page.interfaces.Page;

import java.lang.foreign.MemorySegment;
import java.util.List;

/**
 * Dedicated copy-on-write page holding one encoded projection-index
 * <em>segment</em> (record-key column, one column body, or one string
 * dictionary of a single logical projection leaf — see
 * {@code docs/PROJECTION_INDEX_STORAGE_REDESIGN.md} §2.3).
 *
 * <p>Deliberately {@link OverflowPage}-shaped (the working template for
 * reference-bearing values hung off a leaf's side map, #1076):
 *
 * <ul>
 *   <li><b>Leaf of the commit recursion</b> — {@link #getReferences()},
 *       {@link #commit(StorageEngineWriter)} and the structural reference
 *       accessors throw {@link UnsupportedOperationException}; the page has
 *       no children and is written directly by the storage-engine writer's
 *       commit branch.</li>
 *   <li><b>Offset identity</b> — the durable identity is the file offset
 *       assigned by the storage writer at commit time; the page consumes no
 *       logical page key and participates in no fragment chain (whole-page
 *       last-writer-wins; unchanged segments are shared across revisions by
 *       carrying the resolved {@link PageReference} forward).</li>
 *   <li><b>Never written before commit</b> — instances are created in-memory
 *       during a write transaction and only reach disk inside the commit
 *       descent; on rollback they were simply never written (the
 *       OverflowPage isolation discipline).</li>
 *   <li><b>Immutable heap payload</b> — the byte array never changes after
 *       construction and the page has no close()/off-heap lifecycle, so the
 *       stale-swizzle failure family (recycled frame memory behind a closed
 *       page) cannot occur for this kind; racy swizzles by concurrent
 *       readers are benign, exactly as for {@link OverflowPage}.</li>
 * </ul>
 *
 * <p>Integrity: unlike reference-page children, the parent reference
 * persists a bare 8-byte offset with no hash — corruption detection is the
 * owning leaf descriptor's per-segment {@code byteLen} + {@code contentHash}
 * (XXH3-64), which double as the maintenance no-op comparator. Do not
 * "optimize" the descriptor hash away; it is the only checksum this page
 * has.
 */
public final class ProjectionSegmentPage implements Page {

  /** Encoded segment bytes. Immutable after construction. */
  private final byte[] data;

  /**
   * Constructor.
   *
   * @param data encoded segment bytes; the caller must not mutate the array
   *             after handing it over
   */
  public ProjectionSegmentPage(final byte[] data) {
    if (data == null) {
      throw new IllegalArgumentException("segment data must not be null");
    }
    this.data = data;
  }

  @Override
  public List<PageReference> getReferences() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void commit(final StorageEngineWriter storageEngineWriter) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PageReference getOrCreateReference(final int offset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean setOrCreateReference(final int offset, final PageReference pageReference) {
    throw new UnsupportedOperationException();
  }

  /** @return the segment bytes as a heap-backed {@link MemorySegment} view */
  public MemorySegment getData() {
    return MemorySegment.ofArray(data);
  }

  /** @return the raw segment bytes (do not mutate) */
  public byte[] getDataBytes() {
    return data;
  }

  /** @return the segment length in bytes */
  public int length() {
    return data.length;
  }
}
