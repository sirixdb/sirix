/*
 * [New BSD License]
 * Copyright (c) 2026, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.index.interval;

/**
 * Composite key into the single HOT sub-tree that backs a {@link RelationalIntervalTree}'s two
 * ordered stores.
 *
 * <p>The RI-tree drives one logical ordered map {@code (forkNode, endpoint) -> multiset(ref)} per
 * store (a {@code lower} store keyed {@code (fork, lo)} and an {@code upper} store keyed
 * {@code (fork, hi)}). We realise BOTH stores in a single HOT sub-tree by prefixing the key with a
 * one-byte {@link #store} discriminator ({@link #STORE_LOWER} / {@link #STORE_UPPER}). The
 * order-preserving wire encoding is
 * {@code [store:1][signFlippedBE(forkNode):8][signFlippedBE(endpoint):8]} (see
 * {@link ValidTimeKeySerializer}), so a fixed {@code (store, forkNode)} endpoint sub-range is one
 * contiguous HOT range scan — exactly what {@link OrderedStore#scan} needs.
 *
 * <p>The {@link Comparable} implementation matches the byte encoding's unsigned order: by store,
 * then by signed {@code forkNode}, then by signed {@code endpoint}. The fork node and endpoints are
 * always in {@code [1, 2^h-1]} (positive), so signed and the sign-flipped-unsigned orders coincide;
 * the sign-flip keeps the encoding total-order-preserving even for hypothetical negative inputs.
 *
 * @author Johannes Lichtenberger
 */
public final class ValidTimeKey implements Comparable<ValidTimeKey> {

  /** Discriminator for the {@code lower} store, keyed {@code (fork, lo)}. */
  public static final byte STORE_LOWER = 0;

  /** Discriminator for the {@code upper} store, keyed {@code (fork, hi)}. */
  public static final byte STORE_UPPER = 1;

  private final byte store;
  private final long forkNode;
  private final long endpoint;

  public ValidTimeKey(final byte store, final long forkNode, final long endpoint) {
    this.store = store;
    this.forkNode = forkNode;
    this.endpoint = endpoint;
  }

  public byte store() {
    return store;
  }

  public long forkNode() {
    return forkNode;
  }

  public long endpoint() {
    return endpoint;
  }

  @Override
  public int compareTo(final ValidTimeKey other) {
    final int byStore = Byte.compare(store, other.store);
    if (byStore != 0) {
      return byStore;
    }
    final int byFork = Long.compare(forkNode, other.forkNode);
    if (byFork != 0) {
      return byFork;
    }
    return Long.compare(endpoint, other.endpoint);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof final ValidTimeKey other)) {
      return false;
    }
    return store == other.store && forkNode == other.forkNode && endpoint == other.endpoint;
  }

  @Override
  public int hashCode() {
    int result = store;
    result = 31 * result + Long.hashCode(forkNode);
    result = 31 * result + Long.hashCode(endpoint);
    return result;
  }

  @Override
  public String toString() {
    return "ValidTimeKey[store=" + store + ", fork=" + forkNode + ", endpoint=" + endpoint + "]";
  }
}
