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
   * Data to be stored.
   */
  private final byte[] data;

  /**
   * Constructor.
   *
   * <p>Deliberately imposes NO upper bound on {@code data}. A node record spills here whenever it
   * exceeds {@link io.sirix.settings.Constants}' slot threshold, and that threshold is a spill
   * trigger, not a ceiling — a single large string or binary value legitimately produces an
   * arbitrarily large overflow page. A size cap here would reject valid user data at commit time
   * and, worse, make already-committed pages of that size unreadable. Producers with a genuine
   * domain limit enforce it themselves (see {@code RowGroupDescriptor.MAX_SEGMENT_BYTES} for the
   * projection index); the reader guards against a <em>corrupt</em> stored length by bounding it
   * against the bytes actually remaining in the source, which can never reject an intact page.</p>
   *
   * @param data data to be stored as byte array
   * @throws IllegalArgumentException if {@code data} is null
   */
  public OverflowPage(final byte[] data) {
    if (data == null) {
      throw new IllegalArgumentException("overflow page data must not be null");
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
