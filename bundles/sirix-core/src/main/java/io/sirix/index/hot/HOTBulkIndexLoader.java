/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import static java.util.Objects.requireNonNull;

/**
 * {@link AbstractHOTBulkIndexLoader} for indexes with object keys — a {@code CASValue} for CAS, a
 * {@code QNm} for NAME.
 *
 * @param <K> the logical index key type
 * @author Johannes Lichtenberger
 */
public final class HOTBulkIndexLoader<K extends Comparable<? super K>> extends AbstractHOTBulkIndexLoader {

  private final HOTKeySerializer<K> keySerializer;

  HOTBulkIndexLoader(final HOTIndexWriter<K> writer, final HOTKeySerializer<K> keySerializer) {
    super(writer);
    this.keySerializer = requireNonNull(keySerializer, "keySerializer");
  }

  /**
   * Record that {@code nodeKey} belongs to {@code key}'s posting list.
   *
   * @param key the logical index key
   * @param nodeKey the node key to add; must be in {@code [0, 2^48)}
   */
  public void add(final K key, final long nodeKey) {
    requireNonNull(key, "key");
    final byte[] block = reserveKeySpace(nodeKey);
    final int offset = blockOffset();
    final int length = keySerializer.serializeWithChunkIdx(key, (int) (nodeKey >>> 16), block, offset);
    commitKey(length, nodeKey);
  }
}
