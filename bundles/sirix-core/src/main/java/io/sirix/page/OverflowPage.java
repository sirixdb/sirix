package io.sirix.page;


import io.sirix.api.StorageEngineWriter;
import io.sirix.page.interfaces.Page;

import java.lang.foreign.MemorySegment;
import java.util.List;

/**
 * OverflowPage: an opaque, immutable heap byte[] hung off a leaf's side map by a bare durable
 * offset key — the working template for reference-bearing values (#1076). Two producers:
 *
 * <ul>
 *   <li>{@link KeyValueLeafPage} spills a record that does not fit the slotted page heap here;</li>
 *   <li>the projection index stores a <em>referenced</em> column segment here (the segments a
 *       {@link io.sirix.index.projection.RowGroupDescriptor} does not inline — see
 *       {@code docs/PROJECTION_INDEX_HYBRID_INLINE_SEGMENTS.md} §3.1a). It replaced the
 *       near-identical bespoke {@code ProjectionSegmentPage}: same single immutable byte[], same
 *       throwing structural accessors, same {@code [id][ver+flags][int len][data]} wire form.</li>
 * </ul>
 *
 * <p>Leaf of the commit recursion: the structural accessors throw; the storage-engine writer's
 * commit branch writes it directly and assigns its offset key. Offset identity, no fragment chain
 * (whole-page last-writer-wins); an unchanged page is shared across revisions by carrying its
 * resolved {@link PageReference} forward. Integrity for projection segments is the owning
 * descriptor's per-segment {@code byteLen} + XXH3-64 hash (these pages carry no checksum).
 *
 * @author Johannes Lichtenberger
 */
public final class OverflowPage implements Page {

  /**
   * Defensive sanity bound on a single overflow/segment page, enforced at deserialization so a
   * corrupted stored length fails as a clean {@link IllegalStateException} rather than a
   * negative-array-size error or a multi-GB allocation. Node-record overflow and projection
   * segments are both far below this.
   */
  public static final int MAX_PAGE_BYTES = 16 * 1024 * 1024;

  /**
   * Data to be stored.
   */
  private final byte[] data;

  /**
   * Constructor.
   *
   * @param data data to be stored as byte array
   * @throws IllegalArgumentException if {@code data} is null or exceeds {@link #MAX_PAGE_BYTES}
   */
  public OverflowPage(final byte[] data) {
    if (data == null) {
      throw new IllegalArgumentException("overflow page data must not be null");
    }
    // Guard at construction (write path), symmetric with the deserialization bound: a pathological
    // oversized value must fail loudly BEFORE it is committed, never persist as a committed page
    // that then fails every read. (The retired ProjectionSegmentPage enforced this at construction.)
    if (data.length > MAX_PAGE_BYTES) {
      throw new IllegalArgumentException("overflow page of " + data.length + " bytes exceeds MAX_PAGE_BYTES="
          + MAX_PAGE_BYTES);
    }
    this.data = data;
  }


  @Override
  public List<PageReference> getReferences() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void commit(StorageEngineWriter storageEngineWriter) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PageReference getOrCreateReference(int offset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean setOrCreateReference(int offset, PageReference pageReference) {
    throw new UnsupportedOperationException();
  }

  /**
   * Get the data as a MemorySegment (for compatibility with existing code). Returns a heap segment
   * backed by the byte array.
   */
  public MemorySegment getData() {
    return MemorySegment.ofArray(data);
  }

  /**
   * Get the raw byte array data.
   */
  public byte[] getDataBytes() {
    return data;
  }
}
