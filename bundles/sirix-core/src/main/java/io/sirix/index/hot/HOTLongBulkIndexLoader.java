/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import static java.util.Objects.requireNonNull;

/**
 * {@link AbstractHOTBulkIndexLoader} for the PATH index's primitive {@code long} keys.
 *
 * <p>
 * Separate from {@link HOTBulkIndexLoader} purely to keep the key primitive: a PATH build adds one
 * entry per indexed node, so a {@code Long} key would be one box per node.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public final class HOTLongBulkIndexLoader extends AbstractHOTBulkIndexLoader {

  private final HOTLongKeySerializer keySerializer;

  HOTLongBulkIndexLoader(final HOTLongIndexWriter writer, final HOTLongKeySerializer keySerializer) {
    super(writer);
    this.keySerializer = requireNonNull(keySerializer, "keySerializer");
  }

  /**
   * Record that {@code nodeKey} belongs to {@code key}'s posting list.
   *
   * @param key the logical index key (a path-class record for the PATH index)
   * @param nodeKey the node key to add; must be in {@code [0, 2^48)}
   */
  public void add(final long key, final long nodeKey) {
    final byte[] block = reserveKeySpace(nodeKey, HOTLongKeySerializer.CHUNKED_SERIALIZED_SIZE);
    final int offset = blockOffset();
    final int length = keySerializer.serializeWithChunkIdx(key, (int) (nodeKey >>> 16), block, offset);
    commitKey(length, nodeKey);
  }
}
