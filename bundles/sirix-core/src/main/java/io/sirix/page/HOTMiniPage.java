/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.cache.CacheablePage;
import io.sirix.index.IndexType;
import org.jspecify.annotations.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;

/**
 * Immutable, resolved projection slots from ONE committed HOT leaf view. This is a read cache,
 * never a persisted page or an input to a versioning merge. An absent slot and a present tombstone
 * are distinct. Side references come from the newest image, independently of the value's fragment.
 *
 * <p>
 * The packed allocation starts at 64 bytes and grows in powers of two, bounded at half a leaf. A
 * sorted two-byte offset directory belongs to each immutable view. Appends may share an unchanged
 * payload prefix, writing only previously unused tail bytes; a new directory publishes the result.
 * Readers guarding the old one can finish after replacement or invalidation. No native frame or
 * mutable PageReference is retained, and each hit returns detached value and reference copies.
 */
public final class HOTMiniPage implements CacheablePage {
  public static final int MAX_DATA_BYTES = HOTLeafPage.DEFAULT_SIZE / 2;
  private static final int HEADER_BYTES = 15;
  private static final int SIDE_BYTES = 32;
  // Conservative charge for this object, payload holder, array headers, canonical PageReference, both
  // cache-map
  // nodes, recorded charge and table slots (including uncompressed references/alignment).
  private static final int RETAINED_OVERHEAD_BYTES = 512;
  private static final VarHandle LONGS = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle INTS = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle STATE;

  static {
    try {
      STATE = MethodHandles.lookup().findVarHandle(HOTMiniPage.class, "state", int.class);
    } catch (final ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private final byte[] data;
  private final PackedBody body;
  private final short[] offsets;
  private final int capacity;
  private final int used;
  private final int count;
  private final int distinctKeys;
  private final int revision;
  private final long pageKey;
  /** Sign bit retires the page; the remaining bits count guards. */
  private volatile int state;
  private volatile boolean hot;
  /** One in-flight complete promotion; the published payload and directory remain immutable. */
  private volatile boolean pointPromotionRequested;

  /** Tail reservations cannot overwrite any published prefix, including a fork from an old view. */
  private static final class PackedBody {
    private final byte[] data;
    private int reserved;

    private PackedBody(final int size, final int reserved) {
      data = new byte[size];
      this.reserved = reserved;
    }

    private synchronized boolean reserve(final int expected, final int next) {
      if (reserved != expected || next > data.length) {
        return false;
      }
      reserved = next;
      return true;
    }
  }

  private HOTMiniPage(final PackedBody body, final short[] offsets, final int capacity, final int used,
      final int distinctKeys, final long pageKey, final int revision) {
    this.body = body;
    this.data = body.data;
    this.offsets = offsets;
    this.capacity = capacity;
    this.used = used;
    this.count = offsets.length;
    this.distinctKeys = distinctKeys;
    this.pageKey = pageKey;
    this.revision = revision;
  }

  /** Reject unusual side-reference shapes rather than dropping provenance on admission. */
  public static boolean canCache(final byte[] key, final @Nullable HOTLeafEntry entry) {
    Objects.requireNonNull(key);
    return key.length <= 0xffff
        && (entry == null || entry.sideReference() == null || entry.sideReference().isRawCanonicalHOTCacheReference());
  }

  /** Bound even a deferred admission before retaining a detached copy of the caller's record. */
  public static boolean fitsEmpty(final byte[] key, final @Nullable HOTLeafEntry entry) {
    return canCache(key, entry) && (long) HEADER_BYTES + key.length + 2 + (entry == null
        ? 0
        : entry.value().length + (entry.sideReference() == null
            ? 0L
            : SIDE_BYTES)) <= MAX_DATA_BYTES;
  }

  /**
   * Copy the directory; share the immutable payload prefix until capacity growth. Null means the
   * entry would exceed the unchanged packed cap. The caller guards previous and serializes admissions
   * for this canonical key. Existing entries are never overwritten.
   */
  public static @Nullable HOTMiniPage append(final @Nullable HOTMiniPage previous, final long pageKey,
      final int revision, final byte[] key, final long sideReferenceKey, final @Nullable HOTLeafEntry entry) {
    if (!canCache(key, entry)) {
      throw new IllegalArgumentException("Mini page requires a bounded key and a durable side reference");
    }
    final int found = previous == null
        ? -1
        : previous.find(key, sideReferenceKey);
    if (found >= 0) {
      return previous;
    }
    final int insertion = -found - 1;
    final PageReference side = entry == null
        ? null
        : entry.sideReference();
    final int valueLength = entry == null
        ? -1
        : entry.value().length;
    final int previousUsed = previous == null
        ? 0
        : previous.used;
    final int previousCount = previous == null
        ? 0
        : previous.count;
    final boolean newKey = previous == null || !((insertion > 0 && previous.keyEquals(insertion - 1, key))
        || (insertion < previousCount && previous.keyEquals(insertion, key)));
    final int distinctKeys = (previous == null
        ? 0
        : previous.distinctKeys)
        + (newKey
            ? 1
            : 0);
    final long nextUsed = (long) previousUsed + HEADER_BYTES + key.length + Math.max(0, valueLength) + (side == null
        ? 0
        : SIDE_BYTES);
    final long required = nextUsed + (previousCount + 1L) * 2;
    if (required > MAX_DATA_BYTES) {
      return null;
    }
    final int capacity = Math.max(64, Integer.highestOneBit((int) required - 1) << 1);
    final short[] offsets = new short[previousCount + 1];
    final PackedBody body;
    if (previous != null && previous.body.data.length + offsets.length * Short.BYTES <= capacity
        && previous.body.reserve(previousUsed, (int) nextUsed)) {
      body = previous.body;
    } else {
      // Reserve directory headroom using the smallest possible record (header plus offset).
      // Both arrays together stay within the same power-of-two charge and 32 KiB packed cap.
      // A large record may consume that headroom; it still fits whenever the old format did.
      final int directoryReserve = capacity / (HEADER_BYTES + Short.BYTES) * Short.BYTES;
      body = new PackedBody(Math.max((int) nextUsed, capacity - directoryReserve), (int) nextUsed);
      if (previous != null) {
        System.arraycopy(previous.data, 0, body.data, 0, previousUsed);
      }
    }
    if (previous != null) {
      System.arraycopy(previous.offsets, 0, offsets, 0, insertion);
      System.arraycopy(previous.offsets, insertion, offsets, insertion + 1, previousCount - insertion);
    }
    offsets[insertion] = (short) previousUsed;
    final byte[] packed = body.data;
    putShort(packed, previousUsed, key.length);
    INTS.set(packed, previousUsed + 2, valueLength);
    LONGS.set(packed, previousUsed + 6, sideReferenceKey);
    packed[previousUsed + 14] = (byte) (side == null
        ? 0
        : side.hasHash()
            ? 2
            : 1);
    int position = previousUsed + HEADER_BYTES;
    if (side != null) {
      LONGS.set(packed, position, side.getKey());
      LONGS.set(packed, position + 8, side.getDatabaseId());
      LONGS.set(packed, position + 16, side.getResourceId());
      LONGS.set(packed, position + 24, side.getHashAsLong());
      position += SIDE_BYTES;
    }
    System.arraycopy(key, 0, packed, position, key.length);
    if (entry != null) {
      System.arraycopy(entry.value(), 0, packed, position + key.length, valueLength);
    }
    return new HOTMiniPage(body, offsets, capacity, (int) nextUsed, distinctKeys, pageKey, revision);
  }

  /** Binary-search a resolved slot; negative result encodes the insertion position. Hold a guard. */
  public int find(final byte[] key, final long sideReferenceKey) {
    Objects.requireNonNull(key);
    int low = 0;
    int high = count - 1;
    while (low <= high) {
      final int middle = (low + high) >>> 1;
      final int offset = offset(middle);
      final int start = offset + HEADER_BYTES + (data[offset + 14] == 0
          ? 0
          : SIDE_BYTES);
      int comparison = Arrays.compareUnsigned(key, 0, key.length, data, start, start + getShort(data, offset));
      if (comparison == 0) {
        comparison = Long.compare(sideReferenceKey, (long) LONGS.get(data, offset + 6));
      }
      if (comparison < 0) {
        high = middle - 1;
      } else if (comparison > 0) {
        low = middle + 1;
      } else {
        return middle;
      }
    }
    return -low - 1;
  }

  /** Number of distinct resolved keys; callers must hold a guard. */
  public int getEntryCount() {
    return count;
  }

  /** Side-reference variants of one serialized key are one distinct point demand. */
  public int getDistinctKeyCount() {
    return distinctKeys;
  }

  public boolean containsKey(final byte[] key) {
    final int first = find(key, Long.MIN_VALUE);
    return first >= 0 || (-first - 1 < count && keyEquals(-first - 1, key));
  }

  private boolean keyEquals(final int slot, final byte[] key) {
    final int offset = offset(slot);
    final int start = offset + HEADER_BYTES + (data[offset + 14] == 0
        ? 0
        : SIDE_BYTES);
    return Arrays.equals(key, 0, key.length, data, start, start + getShort(data, offset));
  }

  /** Called under the owning cache's per-key admission lock. */
  public boolean requestPointPromotion() {
    if (pointPromotionRequested) {
      return false;
    }
    pointPromotionRequested = true;
    return true;
  }

  public boolean isPointPromotionRequested() {
    return pointPromotionRequested;
  }

  /** Null is a known absent key, not a cache miss. Hold a guard through this copy. */
  public @Nullable HOTLeafEntry copyEntry(final int slot) {
    Objects.checkIndex(slot, count);
    final int offset = offset(slot);
    final int valueLength = (int) INTS.get(data, offset + 2);
    if (valueLength < 0) {
      return null;
    }
    int position = offset + HEADER_BYTES;
    PageReference side = null;
    final int sideKind = data[offset + 14];
    if (sideKind != 0) {
      side = new PageReference().setKey((long) LONGS.get(data, position))
                                .setDatabaseId((long) LONGS.get(data, position + 8))
                                .setResourceId((long) LONGS.get(data, position + 16));
      if (sideKind == 2) {
        side.setHash((long) LONGS.get(data, position + 24));
      }
      position += SIDE_BYTES;
    }
    position += getShort(data, offset);
    return new HOTLeafEntry(Arrays.copyOfRange(data, position, position + valueLength), side);
  }

  private int offset(final int slot) {
    return Short.toUnsignedInt(offsets[slot]);
  }

  private static int getShort(final byte[] data, final int offset) {
    return (data[offset] & 0xff) | (data[offset + 1] & 0xff) << 8;
  }

  private static void putShort(final byte[] data, final int offset, final int value) {
    data[offset] = (byte) value;
    data[offset + 1] = (byte) (value >>> 8);
  }

  @Override
  public long getActualMemorySize() {
    // Charge the entire capacity, including reserved directory space and shared payload bytes.
    // This is conservative even while another guarded image shares the immutable prefix.
    return capacity + RETAINED_OVERHEAD_BYTES;
  }

  @Override
  public void markAccessed() {
    hot = true;
  }

  @Override
  public boolean isHot() {
    return hot;
  }

  @Override
  public void clearHot() {
    hot = false;
  }

  @Override
  public boolean acquireGuard() {
    int observed = state;
    while (observed >= 0) {
      if (observed == Integer.MAX_VALUE) {
        throw new IllegalStateException("Mini-page guard overflow");
      }
      final int witness = (int) STATE.compareAndExchange(this, observed, observed + 1);
      if (witness == observed) {
        return true;
      }
      observed = witness;
    }
    return false;
  }

  @Override
  public void releaseGuard() {
    int observed = state;
    for (;;) {
      if ((observed & Integer.MAX_VALUE) == 0) {
        throw new IllegalStateException("Mini-page guard underflow");
      }
      final int witness = (int) STATE.compareAndExchange(this, observed, observed - 1);
      if (witness == observed) {
        return;
      }
      observed = witness;
    }
  }

  @Override
  public int getGuardCount() {
    return state & Integer.MAX_VALUE;
  }

  @Override
  public boolean isClosed() {
    return state == Integer.MIN_VALUE;
  }

  @Override
  public void markOrphaned() {
    STATE.getAndBitwiseOr(this, Integer.MIN_VALUE);
  }

  @Override
  public void close() {
    markOrphaned();
  }

  @Override
  public void incrementVersion() {
    // Immutable published image; replacement never changes bytes under a reader.
  }

  @Override
  public long getPageKey() {
    return pageKey;
  }

  @Override
  public int getRevision() {
    return revision;
  }

  @Override
  public IndexType getIndexType() {
    return IndexType.PROJECTION;
  }
}
