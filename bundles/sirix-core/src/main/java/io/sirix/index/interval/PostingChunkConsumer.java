package io.sirix.index.interval;

import io.sirix.index.redblacktree.keyvalue.NodeReferences;

/** Receives an owned 64K posting chunk before individual keys are expanded. */
@FunctionalInterface
public interface PostingChunkConsumer {
  /** A tied exclusive end contributes only records requiring exact endpoint verification. */
  void accept(long chunkBase, NodeReferences references, boolean verifyOnly);
}
