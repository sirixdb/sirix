/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

import io.sirix.api.StorageEngineWriter;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.settings.Fixed;

/**
 * The transaction-backed {@link BulkRecordSink}: records mint through the write transaction's bulk
 * node factory exactly as the sequential assembler always did, index notifications ride each
 * creation, and the one-shot fixups go through the CoW-checked
 * {@code prepareRecordForModificationDocument} path.
 *
 * <p>
 * The fixups apply child counts with a single {@link StructNode#setChildCount(long)} instead of the
 * historical one-increment-per-child loop — at 100M top-level records that loop was 100M
 * decode-modify-encode round trips (each able to trigger a varint-width resize) to reach a value
 * one write expresses.
 */
final class WtxBulkRecordSink implements BulkRecordSink {

  private static final long NULL_KEY = Fixed.NULL_NODE_KEY.getStandardProperty();

  private final JsonNodeTrxImpl wtx;
  private final JsonNodeFactory factory;
  private final StorageEngineWriter storageEngineWriter;
  private final boolean storeChildCount;
  private final boolean useTextCompression;
  private final boolean notifyIndexes;

  WtxBulkRecordSink(final JsonNodeTrxImpl wtx) {
    this(wtx, wtx.bulkHasPrimitiveIndexes());
  }

  /**
   * @param notifyIndexes whether each creation fires a primitive-index notification. The parallel
   *        importer passes {@code false} even with a projection armed: its workers mint records off
   *        the transaction and never notify at all, so the coordinator attributes records to the load
   *        directly rather than through a notification stream only the spine's own handful of nodes
   *        would reach.
   */
  WtxBulkRecordSink(final JsonNodeTrxImpl wtx, final boolean notifyIndexes) {
    this.wtx = wtx;
    this.factory = wtx.bulkNodeFactory();
    this.storageEngineWriter = wtx.getStorageEngineWriter();
    this.storeChildCount = wtx.bulkStoreChildCount();
    this.useTextCompression = wtx.bulkUseTextCompression();
    this.notifyIndexes = notifyIndexes;
  }

  @Override
  public long createObjectNode(final long parentKey, final long leftSibKey, final long notifyPcr) {
    final var node = factory.createJsonObjectNode(parentKey, leftSibKey, NULL_KEY, null);
    notifyInsert(node, notifyPcr);
    return node.getNodeKey();
  }

  @Override
  public long createArrayNode(final long parentKey, final long leftSibKey, final long arrayPcr) {
    final var node = factory.createJsonArrayNode(parentKey, leftSibKey, NULL_KEY, arrayPcr, null);
    notifyInsert(node, arrayPcr);
    return node.getNodeKey();
  }

  @Override
  public long createObjectNamedObjectNode(final long parentKey, final long leftSibKey, final long pathNodeKey,
      final String name) {
    final var node = factory.createJsonObjectNamedObjectNode(parentKey, leftSibKey, NULL_KEY, pathNodeKey, name, null);
    notifyInsert(node, pathNodeKey);
    return node.getNodeKey();
  }

  @Override
  public long createObjectNamedArrayNode(final long parentKey, final long leftSibKey, final long pathNodeKey,
      final String name) {
    final var node = factory.createJsonObjectNamedArrayNode(parentKey, leftSibKey, NULL_KEY, pathNodeKey, name, null);
    notifyInsert(node, pathNodeKey);
    return node.getNodeKey();
  }

  @Override
  public long createStringNode(final long parentKey, final long leftSibKey, final long rightSibKey, final byte[] utf8,
      final int utf8Length, final long notifyPcr) {
    final var node =
        factory.createJsonStringNode(parentKey, leftSibKey, rightSibKey, utf8, 0, utf8Length, useTextCompression, null);
    notifyInsert(node, notifyPcr);
    return node.getNodeKey();
  }

  @Override
  public long createObjectNamedStringNode(final long parentKey, final long leftSibKey, final long rightSibKey,
      final long pathNodeKey, final String name, final byte[] utf8, final int utf8Length) {
    final var node = factory.createJsonObjectNamedStringNode(parentKey, leftSibKey, rightSibKey, pathNodeKey, name,
        utf8, 0, utf8Length, null);
    notifyInsert(node, pathNodeKey);
    return node.getNodeKey();
  }

  @Override
  public long createNumberNode(final long parentKey, final long leftSibKey, final long rightSibKey, final Number value,
      final long notifyPcr) {
    final var node = factory.createJsonNumberNode(parentKey, leftSibKey, rightSibKey, value, null);
    notifyInsert(node, notifyPcr);
    return node.getNodeKey();
  }

  @Override
  public long createNumberNode(final long parentKey, final long leftSibKey, final long rightSibKey, final int value,
      final long notifyPcr) {
    final var node = factory.createJsonNumberNode(parentKey, leftSibKey, rightSibKey, value, null);
    notifyInsert(node, notifyPcr, value);
    return node.getNodeKey();
  }

  @Override
  public long createNumberNode(final long parentKey, final long leftSibKey, final long rightSibKey, final long value,
      final long notifyPcr) {
    final var node = factory.createJsonNumberNode(parentKey, leftSibKey, rightSibKey, value, null);
    notifyInsert(node, notifyPcr, value);
    return node.getNodeKey();
  }

  @Override
  public long createObjectNamedNumberNode(final long parentKey, final long leftSibKey, final long rightSibKey,
      final long pathNodeKey, final String name, final Number value) {
    final var node =
        factory.createJsonObjectNamedNumberNode(parentKey, leftSibKey, rightSibKey, pathNodeKey, name, value, null);
    notifyInsert(node, pathNodeKey);
    return node.getNodeKey();
  }

  @Override
  public long createObjectNamedNumberNode(final long parentKey, final long leftSibKey, final long rightSibKey,
      final long pathNodeKey, final String name, final int value) {
    final var node =
        factory.createJsonObjectNamedNumberNode(parentKey, leftSibKey, rightSibKey, pathNodeKey, name, value, null);
    notifyInsert(node, pathNodeKey, value);
    return node.getNodeKey();
  }

  @Override
  public long createObjectNamedNumberNode(final long parentKey, final long leftSibKey, final long rightSibKey,
      final long pathNodeKey, final String name, final long value) {
    final var node =
        factory.createJsonObjectNamedNumberNode(parentKey, leftSibKey, rightSibKey, pathNodeKey, name, value, null);
    notifyInsert(node, pathNodeKey, value);
    return node.getNodeKey();
  }

  @Override
  public long createBooleanNode(final long parentKey, final long leftSibKey, final long rightSibKey,
      final boolean value, final long notifyPcr) {
    final var node = factory.createJsonBooleanNode(parentKey, leftSibKey, rightSibKey, value, null);
    notifyInsert(node, notifyPcr);
    return node.getNodeKey();
  }

  @Override
  public long createObjectNamedBooleanNode(final long parentKey, final long leftSibKey, final long rightSibKey,
      final long pathNodeKey, final String name, final boolean value) {
    final var node =
        factory.createJsonObjectNamedBooleanNode(parentKey, leftSibKey, rightSibKey, pathNodeKey, name, value, null);
    notifyInsert(node, pathNodeKey);
    return node.getNodeKey();
  }

  @Override
  public long createNullNode(final long parentKey, final long leftSibKey, final long rightSibKey,
      final long notifyPcr) {
    final var node = factory.createJsonNullNode(parentKey, leftSibKey, rightSibKey, null);
    notifyInsert(node, notifyPcr);
    return node.getNodeKey();
  }

  @Override
  public long createObjectNamedNullNode(final long parentKey, final long leftSibKey, final long rightSibKey,
      final long pathNodeKey, final String name) {
    final var node = factory.createJsonObjectNamedNullNode(parentKey, leftSibKey, rightSibKey, pathNodeKey, name, null);
    notifyInsert(node, pathNodeKey);
    return node.getNodeKey();
  }

  @Override
  public void fixupContainer(final long containerKey, final long firstChildKey, final long lastChildKey,
      final long childCount, final long rightSibKey) {
    final StructNode container = storageEngineWriter.prepareRecordForModificationDocument(containerKey);
    if (childCount > 0) {
      container.setFirstChildKey(firstChildKey);
      container.setLastChildKey(lastChildKey);
      if (storeChildCount) {
        container.setChildCount(childCount);
      }
    }
    if (rightSibKey != NULL_KEY) {
      container.setRightSiblingKey(rightSibKey);
    }
  }

  @Override
  public void fixupDocument(final long documentKey, final long firstChildKey, final long lastChildKey,
      final long childCount) {
    final StructNode document = storageEngineWriter.prepareRecordForModificationDocument(documentKey);
    document.setFirstChildKey(firstChildKey);
    document.setLastChildKey(lastChildKey);
    if (storeChildCount) {
      document.setChildCount(childCount);
    }
  }

  @Override
  public void accountRecords(final int mutations) {
    wtx.bulkAccountRecord(mutations);
  }

  private void notifyInsert(final ImmutableNode node, final long pathNodeKey) {
    if (notifyIndexes) {
      wtx.bulkNotifyInsert(node, pathNodeKey);
    }
  }

  private void notifyInsert(final ImmutableNode node, final long pathNodeKey, final int numericValue) {
    if (notifyIndexes) {
      wtx.bulkNotifyInsert(node, pathNodeKey, numericValue);
    }
  }

  private void notifyInsert(final ImmutableNode node, final long pathNodeKey, final long numericValue) {
    if (notifyIndexes) {
      wtx.bulkNotifyInsert(node, pathNodeKey, numericValue);
    }
  }
}
