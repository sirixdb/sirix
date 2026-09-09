/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.index.IndexType;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;

/**
 * Identity of one HOT point lookup: a serialized index key within a single index of a single
 * committed revision.
 *
 * <p>
 * The revision is part of the identity rather than a validity check, which is what lets
 * {@link HOTLookupCache} run WITHOUT an invalidation protocol: a committed revision's index content
 * is immutable, so an entry can never go stale — a write produces a new revision and therefore new
 * keys, and the old entries simply age out. The database and resource ids prevent collisions
 * because one buffer manager serves more than one resource.
 * </p>
 *
 * <h2>Probing without copying</h2>
 * <p>
 * The key bytes are compared and hashed BY CONTENT over a {@code (offset, length)} range, so a
 * lookup can be answered straight out of the caller's reusable serialization buffer. That matters
 * because the miss path is the common one on a cold cache and must not be made slower than the walk
 * it is trying to avoid: {@link #probe} borrows the caller's buffer and allocates nothing beyond
 * the key object itself, while {@link #owned} takes the copy that a stored entry needs. Only
 * {@code owned} keys may be handed to {@link HOTLookupCache#put(HOTLookupKey, long[], long)} — a
 * probe key's buffer is overwritten by the very next lookup.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public final class HOTLookupKey {

  private final long databaseId;
  private final long resourceId;
  private final int revisionNumber;
  private final IndexType indexType;
  private final int indexNumber;
  private final byte[] key;
  private final int offset;
  private final int length;

  /**
   * Whether {@link #key} is this key's own copy rather than a borrowed buffer.
   *
   * <p>
   * The distinction is load-bearing and was previously documented but unenforceable: a probe key
   * stored in the cache aliases a reused serialization buffer, so the next lookup on that thread
   * rewrites the stored key's bytes while its precomputed {@link #hash} stays frozen — after which
   * the entry answers a DIFFERENT logical key's probe. Carrying the flag lets
   * {@link HOTLookupCache#put} reject that outright instead of corrupting silently.
   * </p>
   */
  private final boolean owned;

  /** Precomputed because this key is hashed on every lookup and never mutated after construction. */
  private final int hash;

  private HOTLookupKey(final long databaseId, final long resourceId, final int revisionNumber,
      final IndexType indexType, final int indexNumber, final byte[] key, final int offset, final int length,
      final int hash, final boolean owned) {
    this.databaseId = databaseId;
    this.resourceId = resourceId;
    this.revisionNumber = revisionNumber;
    this.indexType = indexType;
    this.indexNumber = indexNumber;
    this.key = key;
    this.offset = offset;
    this.length = length;
    this.hash = hash;
    this.owned = owned;
  }

  /**
   * A key that BORROWS {@code key[offset, offset + length)} for the duration of a cache lookup.
   *
   * <p>
   * Never store one: the buffer is typically a reused thread-local that the next lookup overwrites,
   * which would silently corrupt the cache's key space.
   * </p>
   *
   * @param databaseId the database id
   * @param resourceId the resource id
   * @param revisionNumber the committed revision the lookup is answered from
   * @param indexType the index type (PATH, CAS, NAME, ...)
   * @param indexNumber the index number within that type
   * @param key buffer holding the serialized index key; may be oversized and reused
   * @param offset offset of the serialized key within {@code key}
   * @param length serialized length of the key
   * @return a borrowing key, valid only until {@code key} is next written
   */
  public static HOTLookupKey probe(final long databaseId, final long resourceId, final int revisionNumber,
      final IndexType indexType, final int indexNumber, final byte[] key, final int offset, final int length) {
    Objects.requireNonNull(indexType, "indexType");
    Objects.requireNonNull(key, "key");
    Objects.checkFromIndexSize(offset, length, key.length);
    return new HOTLookupKey(databaseId, resourceId, revisionNumber, indexType, indexNumber, key, offset, length,
        computeHash(databaseId, resourceId, revisionNumber, indexType, indexNumber, key, offset, length), false);
  }

  /**
   * The storable twin of this key: identical identity, over bytes this key owns.
   *
   * <p>
   * Equal to the probe it was derived from — same content, same hash — so the entry it stores is
   * found by the next probe of the same key.
   * </p>
   *
   * @return a key owning a copy of the serialized bytes
   */
  public HOTLookupKey owned() {
    // The copy holds the same bytes, so the hash carries over verbatim — recomputing it would be a
    // second full pass over the key on the one path (admission) that is already doing a copy.
    return new HOTLookupKey(databaseId, resourceId, revisionNumber, indexType, indexNumber,
        Arrays.copyOfRange(key, offset, offset + length), 0, length, hash, true);
  }

  /**
   * Mixes the identity and the key bytes into one {@code int}.
   *
   * <p>
   * HFT: the bytes are consumed EIGHT AT A TIME. This runs on every lookup, hit or miss, and a CAS
   * key is tens of bytes — the textbook {@code 31 * h + b} polynomial would be one dependent
   * multiply-add per byte, a serial chain roughly as long as the key. Four word reads and four
   * multiplies cover a 32-byte key instead, and the 64-bit Fibonacci multiplier mixes high bits down
   * so the low bits the hash table indexes on actually depend on the whole key. Native byte order:
   * the hash never leaves the process, so there is nothing to keep stable across hosts.
   * </p>
   */
  private static int computeHash(final long databaseId, final long resourceId, final int revisionNumber,
      final IndexType indexType, final int indexNumber, final byte[] key, final int offset, final int length) {
    long h = databaseId * MIX ^ resourceId;
    h = (h ^ ((long) revisionNumber << 32 | Integer.toUnsignedLong(indexNumber))) * MIX;
    h = (h ^ indexType.ordinal()) * MIX;

    int i = offset;
    for (final int wordEnd = offset + length - Long.BYTES; i <= wordEnd; i += Long.BYTES) {
      h = (h ^ (long) BYTE_ARRAY_LONG.get(key, i)) * MIX;
    }
    for (final int end = offset + length; i < end; i++) {
      h = (h ^ (key[i] & 0xFFL)) * MIX;
    }
    // Length participates so that two keys differing only by trailing bytes the tail loop consumed
    // in a different grouping cannot collide.
    h = (h ^ length) * MIX;
    return (int) (h ^ (h >>> 32));
  }

  /** 2^64 / phi — the standard Fibonacci hashing multiplier, odd so it is invertible mod 2^64. */
  private static final long MIX = 0x9E3779B97F4A7C15L;

  /** Reads eight key bytes as one {@code long} for {@link #computeHash}. */
  private static final VarHandle BYTE_ARRAY_LONG =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());


  /**
   * Whether this key owns its bytes and may therefore be stored.
   *
   * @return {@code true} for a key produced by {@link #owned()}
   */
  public boolean isOwned() {
    return owned;
  }

  /**
   * The database this lookup belongs to — the scope a cache sweep invalidates by.
   *
   * @return the database id
   */
  public long databaseId() {
    return databaseId;
  }

  /**
   * The resource this lookup belongs to — the scope a cache sweep invalidates by.
   *
   * @return the resource id
   */
  public long resourceId() {
    return resourceId;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof HOTLookupKey other)) {
      return false;
    }
    // Cheap scalar discriminators first: two keys that differ at all almost always differ here, so
    // the byte-range compare is reached only by genuine candidates.
    return hash == other.hash && length == other.length && revisionNumber == other.revisionNumber
        && indexNumber == other.indexNumber && databaseId == other.databaseId && resourceId == other.resourceId
        && indexType == other.indexType
        && Arrays.equals(key, offset, offset + length, other.key, other.offset, other.offset + other.length);
  }

  @Override
  public int hashCode() {
    return hash;
  }

  @Override
  public String toString() {
    return "HOTLookupKey[db=" + databaseId + ", resource=" + resourceId + ", revision=" + revisionNumber + ", type="
        + indexType + ", index=" + indexNumber + ", keyLength=" + length + ']';
  }
}
