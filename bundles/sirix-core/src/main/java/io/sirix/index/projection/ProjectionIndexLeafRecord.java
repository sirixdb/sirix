/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.DataRecord;

import java.util.Arrays;
import java.util.Objects;

/**
 * DataRecord wrapping a single serialised {@link ProjectionIndexLeafPage}.
 *
 * <p>One record per leaf chunk: the {@code nodeKey} is the sequential leaf
 * index inside the projection index's HOT sub-tree (rooted at
 * {@code RevisionRootPage#getProjectionPageReference}), and {@code payload}
 * is exactly the byte[] produced by {@link ProjectionIndexLeafPage#serialize()}.
 *
 * <p>Payload is stored opaquely at this layer — the scan kernel unpacks it
 * via {@link ProjectionIndexLeafPage#deserialize(byte[])} when it walks a
 * leaf. Wrapping keeps the index persistent across restarts without needing
 * a new PageKind.
 */
public final class ProjectionIndexLeafRecord implements DataRecord {

  private final long nodeKey;
  private final byte[] payload;

  public ProjectionIndexLeafRecord(final long nodeKey, final byte[] payload) {
    this.nodeKey = nodeKey;
    this.payload = Objects.requireNonNull(payload, "payload");
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  /**
   * Returns the raw serialised leaf bytes — aliased (not copied) on the hot
   * path. Callers must not mutate.
   */
  public byte[] getPayload() {
    return payload;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.PROJECTION_INDEX_LEAF;
  }

  @Override
  public SirixDeweyID getDeweyID() {
    return null;
  }

  @Override
  public byte[] getDeweyIDAsBytes() {
    return null;
  }

  @Override
  public int getPreviousRevisionNumber() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof final ProjectionIndexLeafRecord other)) return false;
    return nodeKey == other.nodeKey && Arrays.equals(payload, other.payload);
  }

  @Override
  public int hashCode() {
    int h = Long.hashCode(nodeKey);
    h = 31 * h + Arrays.hashCode(payload);
    return h;
  }

  @Override
  public String toString() {
    return "ProjectionIndexLeafRecord{nodeKey=" + nodeKey
        + ", payloadBytes=" + payload.length + "}";
  }
}
