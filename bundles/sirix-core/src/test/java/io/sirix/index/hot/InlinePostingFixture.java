package io.sirix.index.hot;

import io.sirix.index.interval.ValidTimeKey;
import io.sirix.index.interval.ValidTimeKeySerializer;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;

/**
 * Builds the inline-chunk geometry of structural regressions through the ordinary slot mutation
 * primitives. No production threshold or format switch is needed to build that geometry.
 */
final class InlinePostingFixture {
  private static final ThreadLocal<InlinePostingFixture> SCRATCH = ThreadLocal.withInitial(InlinePostingFixture::new);
  private final byte[] composite = new byte[ValidTimeKeySerializer.KEY_BYTES + HOTKeySerializer.CHUNK_IDX_BYTES];
  private final byte[] payload = new byte[2 + Long.BYTES];
  private final NodeReferences bit = new NodeReferences();

  static void insert(final HOTIndexWriter<ValidTimeKey> writer, final ValidTimeKey key, final long nodeKey) {
    final InlinePostingFixture scratch = SCRATCH.get();
    ValidTimeKeySerializer.INSTANCE.serializeWithChunkIdx(key, (int) (nodeKey >>> 16), scratch.composite, 0);
    scratch.bit.getNodeKeys().clear();
    scratch.bit.addNodeKey(nodeKey & 0xFFFFL);
    final int length = NodeReferencesSerializer.serialize(scratch.bit, scratch.payload, 0);
    writer.doIndex(scratch.composite, scratch.composite.length, scratch.payload, length);
  }

  static boolean remove(final HOTIndexWriter<ValidTimeKey> writer, final ValidTimeKey key, final long nodeKey) {
    final InlinePostingFixture scratch = SCRATCH.get();
    ValidTimeKeySerializer.INSTANCE.serializeWithChunkIdx(key, (int) (nodeKey >>> 16), scratch.composite, 0);
    return writer.doRemovePostingBit(scratch.composite, scratch.composite.length, nodeKey & 0xFFFFL);
  }
}
