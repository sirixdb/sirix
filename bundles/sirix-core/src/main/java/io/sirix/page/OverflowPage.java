package io.sirix.page;


import io.sirix.api.StorageEngineWriter;
import io.sirix.node.BytesOut;
import io.sirix.page.interfaces.Page;
import org.jspecify.annotations.Nullable;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.List;

/**
 * OverflowPage: opaque immutable bytes hung off a leaf's side map by a bare durable offset key —
 * heap-backed for ordinary pages, or a bounded native-reservoir view while a bulk side page is
 * pending. It is the working template for reference-bearing values (#1076). Two producers:
 *
 * <ul>
 * <li>{@link KeyValueLeafPage} spills a record that does not fit the slotted page heap here;</li>
 * <li>the projection index stores a <em>referenced</em> column segment here (the segments a
 * {@link io.sirix.index.projection.RowGroupDescriptor} does not inline — see
 * {@code docs/PROJECTION_INDEX_HYBRID_INLINE_SEGMENTS.md} §3.1a). It replaced the near-identical
 * bespoke {@code ProjectionSegmentPage}: same immutable bytes, same throwing structural accessors,
 * same {@code [id][ver+flags][int len][data]} wire form.</li>
 * </ul>
 *
 * <p>
 * Leaf of the commit recursion: the structural accessors throw; the storage-engine writer's commit
 * branch writes it directly and assigns its offset key. Offset identity, no fragment chain
 * (whole-page last-writer-wins); an unchanged page is shared across revisions by carrying its
 * resolved {@link PageReference} forward. Integrity for projection segments is the owning
 * descriptor's per-segment {@code byteLen} + XXH3-64 hash (these pages carry no checksum).
 *
 * @author Johannes Lichtenberger
 */
public final class OverflowPage implements Page {

  /**
   * Heap data to be stored, or {@code null} for a view into a transaction-owned native reservoir.
   */
  private final byte @Nullable [] heapData;

  /** Shared, read-only native reservoir for a staged immutable page. */
  private final @Nullable MemorySegment nativeData;

  /** First byte of this page inside {@link #nativeData}. */
  private final long nativeOffset;

  /** Exact payload length for either representation. */
  private final int dataLength;

  /** Native views are valid only while their append batch owns them. */
  private volatile boolean closed;

  /**
   * Constructor.
   *
   * <p>
   * Deliberately imposes NO upper bound on {@code data}. A node record spills here whenever it
   * exceeds {@link io.sirix.settings.Constants}' slot threshold, and that threshold is a spill
   * trigger, not a ceiling — a single large string or binary value legitimately produces an
   * arbitrarily large overflow page. A size cap here would reject valid user data at commit time and,
   * worse, make already-committed pages of that size unreadable. Producers with a genuine domain
   * limit enforce it themselves (see {@code RowGroupDescriptor.MAX_SEGMENT_BYTES} for the projection
   * index); the reader guards against a <em>corrupt</em> stored length by bounding it against the
   * bytes actually remaining in the source, which can never reject an intact page.
   * </p>
   *
   * @param data data to be stored as byte array
   * @throws IllegalArgumentException if {@code data} is null
   */
  public OverflowPage(final byte[] data) {
    if (data == null) {
      throw new IllegalArgumentException("overflow page data must not be null");
    }
    heapData = data;
    nativeData = null;
    nativeOffset = 0L;
    dataLength = data.length;
  }

  /**
   * Construct an immutable view into a transaction-owned native payload reservoir.
   *
   * <p>
   * The segment must be read-only and remain alive until the owning page reference is either
   * published or cancelled. The side-page append pipeline uses this representation so encoded segment
   * arrays can die in eden immediately instead of surviving several young collections and becoming
   * dead old-generation garbage. The reservoir itself is fixed-size and reused.
   * </p>
   *
   * @param data shared read-only reservoir
   * @param offset first payload byte in {@code data}
   * @param length exact payload length
   */
  public OverflowPage(final MemorySegment data, final long offset, final int length) {
    if (data == null) {
      throw new IllegalArgumentException("overflow page data segment must not be null");
    }
    if (!data.isReadOnly()) {
      throw new IllegalArgumentException("native overflow page data must be read-only");
    }
    if (!data.isNative()) {
      throw new IllegalArgumentException("native overflow page data must reside outside the Java heap");
    }
    final long capacity = data.byteSize();
    if (offset < 0L || length < 0 || offset > capacity || length > capacity - offset) {
      throw new IllegalArgumentException("overflow page native range is outside its segment: offset=" + offset
          + ", length=" + length + ", capacity=" + capacity);
    }
    heapData = null;
    nativeData = data;
    nativeOffset = offset;
    dataLength = length;
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
    if (heapData != null) {
      return MemorySegment.ofArray(heapData);
    }
    // Do not leak a view into a reusable reservoir to compatibility callers. The page serializer
    // uses writeDataTo(), which retains the zero-allocation native-to-native path.
    return MemorySegment.ofArray(getDataBytes());
  }

  /**
   * Get this payload as a byte array.
   *
   * <p>
   * A heap-backed page returns its immutable producer array as before. A staged native page returns a
   * copy; same-transaction projection reads are deliberately uncommon and must not turn the fixed
   * native reservoir back into transaction-long heap retention.
   * </p>
   */
  public byte[] getDataBytes() {
    if (heapData != null) {
      return heapData;
    }
    ensureNativeViewOpen();
    final byte[] copy = new byte[dataLength];
    MemorySegment.copy(nativeData, ValueLayout.JAVA_BYTE, nativeOffset, copy, 0, dataLength);
    return copy;
  }

  /** Exact payload length without materialising a native payload on heap. */
  public int dataLength() {
    return dataLength;
  }

  /** Whether this page still owns a producer-supplied heap array. */
  public boolean isHeapBacked() {
    return heapData != null;
  }

  /** Copy this payload into a writable staging reservoir. */
  public void copyDataTo(final MemorySegment target, final long targetOffset) {
    if (target == null) {
      throw new IllegalArgumentException("overflow page copy target must not be null");
    }
    final long capacity = target.byteSize();
    if (targetOffset < 0L || targetOffset > capacity || dataLength > capacity - targetOffset) {
      throw new IllegalArgumentException("overflow page copy range is outside its target: offset=" + targetOffset
          + ", length=" + dataLength + ", capacity=" + capacity);
    }
    if (heapData != null) {
      MemorySegment.copy(heapData, 0, target, ValueLayout.JAVA_BYTE, targetOffset, dataLength);
    } else {
      ensureNativeViewOpen();
      MemorySegment.copy(nativeData, nativeOffset, target, targetOffset, dataLength);
    }
  }

  /** Serialize only the payload bytes, preserving the exact bounded native view. */
  public void writeDataTo(final BytesOut<?> sink) {
    if (sink == null) {
      throw new IllegalArgumentException("overflow page sink must not be null");
    }
    if (heapData != null) {
      sink.write(heapData);
    } else {
      ensureNativeViewOpen();
      sink.writeSegment(nativeData, nativeOffset, dataLength);
    }
  }

  /**
   * The payload as a segment the page serializer's codecs can read IN PLACE, paired with
   * {@link #payloadOffsetForSerializer()}.
   *
   * <p>
   * Package-private and named for its one caller on purpose. {@link #getData()} deliberately refuses
   * to hand a view into the reusable native reservoir to general callers, and that refusal stands —
   * it copies instead. The serializer is the exception it already makes for {@link #writeDataTo}: it
   * runs inside the append batch that owns the reservoir, so it may read the bounded view directly,
   * and compressing through a heap copy would reintroduce exactly the promoted garbage the native
   * staging path exists to remove.
   * </p>
   */
  MemorySegment payloadSegmentForSerializer() {
    if (heapData != null) {
      return MemorySegment.ofArray(heapData);
    }
    ensureNativeViewOpen();
    return nativeData;
  }

  /** First byte of this payload inside {@link #payloadSegmentForSerializer()}. */
  long payloadOffsetForSerializer() {
    return heapData != null
        ? 0L
        : nativeOffset;
  }

  private void ensureNativeViewOpen() {
    if (closed) {
      throw new IllegalStateException("staged native overflow page is no longer owned by its append batch");
    }
  }

  @Override
  public void close() {
    if (nativeData != null) {
      closed = true;
    }
  }

  @Override
  public boolean isClosed() {
    return nativeData != null && closed;
  }
}
