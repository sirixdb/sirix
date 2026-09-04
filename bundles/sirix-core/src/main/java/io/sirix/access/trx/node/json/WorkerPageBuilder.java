/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import io.sirix.index.projection.ProjectionChunkRowBatch;
import io.sirix.node.NodeKind;
import io.sirix.node.json.ArrayNode;
import io.sirix.node.json.BooleanNode;
import io.sirix.node.json.NullNode;
import io.sirix.node.json.NumberNode;
import io.sirix.node.json.ObjectNamedArrayNode;
import io.sirix.node.json.ObjectNamedBooleanNode;
import io.sirix.node.json.ObjectNamedNullNode;
import io.sirix.node.json.ObjectNamedNumberNode;
import io.sirix.node.json.ObjectNamedObjectNode;
import io.sirix.node.json.ObjectNamedStringNode;
import io.sirix.node.json.ObjectNode;
import io.sirix.node.json.StringNode;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.StructNode;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageLayout;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;

import net.openhft.hashing.LongHashFunction;
import org.jspecify.annotations.Nullable;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

/**
 * Stage-B worker sink of the parallel bulk importer: builds records with FINAL bytes into
 * STANDALONE {@link KeyValueLeafPage}s for a pre-reserved contiguous key range, entirely off-thread
 * — no transaction, no intent log, no shared dictionaries. It replicates the transaction factory's
 * direct-write skeleton per node kind (estimate → heap reserve → {@code writeNewRecord} →
 * {@code completeDirectWrite}), with three deliberate differences:
 * <ul>
 * <li>Keys mint from the worker's own counter over the reserved range — the assembler's prediction
 * asserts every mint, and {@link #finish(long)} additionally verifies the final key and the
 * populated-slot total, so a count/build divergence refuses loudly (critic C2).</li>
 * <li>Field names arrive as PRE-RESOLVED dictionary keys through the chunk's name table — the
 * coordinator interned every name in first-occurrence order during the counting pass.</li>
 * <li>Insert-time FSST never engages: v1 imports into a FRESH resource, where the sequential path's
 * encode is a no-op too (no prior revision, no table) — the flags written are identical by
 * construction.</li>
 * </ul>
 *
 * <p>
 * Oversized string values take the same overflow route as the factory: a heap record via
 * {@link KeyValueLeafPage#setRecord}, diverted to an overflow page at commit.
 */
final class WorkerPageBuilder implements BulkRecordSink {

  private static final long NULL_KEY = Fixed.NULL_NODE_KEY.getStandardProperty();
  private static final ProjectionChunkRowBatch[] NO_PROJECTION_BATCHES = new ProjectionChunkRowBatch[0];

  private final ResourceConfiguration resourceConfig;
  private final int revisionNumber;
  private final LongHashFunction hashFunction;
  private final boolean storeChildCount;
  private final Object2IntOpenHashMap<String> nameKeys;
  private final long firstKey;
  private final long lastKey;

  /**
   * Armed projection batches this build feeds AS IT BUILDS — one per armed definition, empty when
   * none is armed. The worker holds every record's primitives in hand exactly once, here, so
   * extraction costs one hook per node instead of a per-record re-read through the transaction.
   */
  private final ProjectionChunkRowBatch[] projectionBatches;
  private final long projectionRecordSetKey;
  private final boolean projectionTrackChildValues;

  /**
   * PATH/CAS/NAME tuples this build collects AS IT BUILDS, or {@code null} when none of those
   * families is armed. Same rationale as the projection batches: the worker holds every node's
   * (pathNodeKey, nameKey, value) exactly once, here.
   */
  private final @Nullable ChunkIndexTupleBatch indexTuples;

  /**
   * Per-chunk path-statistics partials, recorded by the chunk's assembler; {@code null} when unarmed.
   */
  private final @Nullable ChunkPathStatsBatch pathStatsBatch;

  private final List<KeyValueLeafPage> pages = new ArrayList<>();
  private KeyValueLeafPage currentPage;
  private long currentPageKey = -1;
  private long nextKey;
  private long populatedSlots;

  // Worker-local flyweight scratch: estimateSerializedSize()/getHeapOffsets() plus the container
  // fixup binds. Same construction recipes as the transaction factory's singletons.
  private final ObjectNode scratchObjectNode;
  private final ArrayNode scratchArrayNode;
  private final NullNode scratchNullNode;
  private final BooleanNode scratchBooleanNode;
  private final NumberNode scratchNumberNode;
  private final StringNode scratchStringNode;
  private final ObjectNamedBooleanNode scratchNamedBooleanNode;
  private final ObjectNamedNumberNode scratchNamedNumberNode;
  private final ObjectNamedStringNode scratchNamedStringNode;
  private final ObjectNamedNullNode scratchNamedNullNode;
  private final ObjectNamedObjectNode scratchNamedObjectNode;
  private final ObjectNamedArrayNode scratchNamedArrayNode;

  WorkerPageBuilder(final ResourceConfiguration resourceConfig, final int revisionNumber,
      final LongHashFunction hashFunction, final boolean storeChildCount, final Object2IntOpenHashMap<String> nameKeys,
      final long firstKey, final long lastKey) {
    this(resourceConfig, revisionNumber, hashFunction, storeChildCount, nameKeys, firstKey, lastKey, null, NULL_KEY,
        null, null);
  }

  WorkerPageBuilder(final ResourceConfiguration resourceConfig, final int revisionNumber,
      final LongHashFunction hashFunction, final boolean storeChildCount, final Object2IntOpenHashMap<String> nameKeys,
      final long firstKey, final long lastKey, final ProjectionChunkRowBatch @Nullable [] projectionBatches,
      final long projectionRecordSetKey, final @Nullable ChunkIndexTupleBatch indexTuples,
      final @Nullable ChunkPathStatsBatch pathStatsBatch) {
    this.resourceConfig = resourceConfig;
    this.revisionNumber = revisionNumber;
    this.hashFunction = hashFunction;
    this.storeChildCount = storeChildCount;
    this.nameKeys = nameKeys;
    this.firstKey = firstKey;
    this.lastKey = lastKey;
    this.nextKey = firstKey;
    this.projectionBatches = projectionBatches == null
        ? NO_PROJECTION_BATCHES
        : projectionBatches;
    this.projectionRecordSetKey = projectionRecordSetKey;
    boolean trackChildValues = false;
    for (final ProjectionChunkRowBatch batch : this.projectionBatches) {
      trackChildValues |= batch.trackChildValues();
    }
    this.projectionTrackChildValues = trackChildValues;
    this.indexTuples = indexTuples;
    this.pathStatsBatch = pathStatsBatch;

    this.scratchObjectNode = new ObjectNode(0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber, NULL_KEY, NULL_KEY,
        NULL_KEY, NULL_KEY, 0, 0, 0, hashFunction, (SirixDeweyID) null);
    this.scratchArrayNode = new ArrayNode(0, 0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber, NULL_KEY, NULL_KEY,
        NULL_KEY, NULL_KEY, 0, 0, 0, hashFunction, (SirixDeweyID) null);
    this.scratchNullNode = new NullNode(0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber, NULL_KEY, NULL_KEY, 0,
        hashFunction, (SirixDeweyID) null);
    this.scratchBooleanNode = new BooleanNode(0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber, NULL_KEY, NULL_KEY,
        0, false, hashFunction, (SirixDeweyID) null);
    this.scratchNumberNode = new NumberNode(0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber, NULL_KEY, NULL_KEY, 0,
        0, hashFunction, (SirixDeweyID) null);
    this.scratchStringNode = new StringNode(0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber, NULL_KEY, NULL_KEY, 0,
        new byte[0], hashFunction, (SirixDeweyID) null, false, null);
    this.scratchNamedBooleanNode = new ObjectNamedBooleanNode(0, hashFunction);
    this.scratchNamedNumberNode = new ObjectNamedNumberNode(0, hashFunction);
    this.scratchNamedStringNode = new ObjectNamedStringNode(0, hashFunction);
    this.scratchNamedNullNode = new ObjectNamedNullNode(0, hashFunction);
    this.scratchNamedObjectNode = new ObjectNamedObjectNode(0, hashFunction);
    this.scratchNamedArrayNode = new ObjectNamedArrayNode(0, hashFunction);
  }

  // ==== output =================================================================================

  /**
   * Verifies the build filled the reserved range EXACTLY and returns the pages in page-key order.
   * Always on — this replaces the shared-mint safety net for off-thread builds.
   *
   * @param expectedLastKey the range's last reserved key
   */
  List<KeyValueLeafPage> finish(final long expectedLastKey) {
    if (nextKey - 1 != expectedLastKey) {
      throw new IllegalStateException(
          "chunk build minted up to " + (nextKey - 1) + " but the reserved range ends at " + expectedLastKey);
    }
    final long expectedRecords = expectedLastKey - firstKey + 1;
    if (populatedSlots != expectedRecords) {
      throw new IllegalStateException(
          "chunk build populated " + populatedSlots + " slots for " + expectedRecords + " reserved keys");
    }
    if (currentPage != null) {
      pages.add(currentPage);
      currentPage = null;
    }
    for (final ProjectionChunkRowBatch batch : projectionBatches) {
      batch.finishBuild();
    }
    return pages;
  }

  // ==== projection extraction hooks (no-ops when nothing is armed) =============================

  /**
   * A structured, unnamed node: a record root when its parent is the record set, else a plain child.
   */
  private void noteUnnamedStructured(final long nodeKey, final long parentKey) {
    if (parentKey == projectionRecordSetKey) {
      for (final ProjectionChunkRowBatch batch : projectionBatches) {
        batch.beginRecord(nodeKey);
      }
    } else if (projectionTrackChildValues) {
      for (final ProjectionChunkRowBatch batch : projectionBatches) {
        batch.onChildNonString(parentKey);
      }
    }
  }

  /** An unnamed non-string leaf: a scalar record root, or a set-poisoning child. */
  private void noteUnnamedNonString(final long nodeKey, final long parentKey) {
    noteUnnamedStructured(nodeKey, parentKey);
  }

  private void noteUnnamedString(final long nodeKey, final long parentKey, final byte[] utf8, final int utf8Length) {
    if (parentKey == projectionRecordSetKey) {
      for (final ProjectionChunkRowBatch batch : projectionBatches) {
        batch.beginRecord(nodeKey);
      }
    } else if (projectionTrackChildValues) {
      for (final ProjectionChunkRowBatch batch : projectionBatches) {
        batch.onChildValueString(parentKey, utf8, utf8Length);
      }
    }
  }

  long firstKey() {
    return firstKey;
  }

  /** The chunk's PATH/CAS/NAME tuples, for the coordinator's drain; {@code null} when unarmed. */
  @Nullable
  ChunkIndexTupleBatch indexTuples() {
    return indexTuples;
  }

  /** The chunk's path-statistics partials, for the coordinator's drain; {@code null} when unarmed. */
  @Nullable
  ChunkPathStatsBatch pathStatsBatch() {
    return pathStatsBatch;
  }

  // ==== minting + page roll ====================================================================

  private long mint() {
    final long key = nextKey++;
    if (key > lastKey) {
      throw new IllegalStateException("chunk build overran its reserved range at key " + key);
    }
    final long pageKey = key >>> 10;
    if (pageKey != currentPageKey) {
      if (currentPage != null) {
        pages.add(currentPage);
      }
      currentPage =
          new KeyValueLeafPage(pageKey, IndexType.DOCUMENT, resourceConfig, revisionNumber, null, null, false);
      currentPageKey = pageKey;
    }
    populatedSlots++;
    return key;
  }

  private static int slotOf(final long key) {
    return (int) (key & (Constants.NDP_NODE_COUNT - 1));
  }

  private int resolvedNameKey(final String name) {
    final int key = nameKeys.getInt(name);
    if (key == Integer.MIN_VALUE) {
      throw new IllegalStateException("name '" + name + "' was not pre-interned for this build chunk");
    }
    return key;
  }

  // ==== containers =============================================================================

  @Override
  public long createObjectNode(final long parentKey, final long leftSibKey, final long notifyPcr) {
    final long nodeKey = mint();
    noteUnnamedStructured(nodeKey, parentKey);
    final KeyValueLeafPage kvl = currentPage;
    final long absOffset = kvl.prepareHeapForDirectWriteOrOverflow(scratchObjectNode.estimateSerializedSize(), 0);
    if (absOffset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
      kvl.setRecord(new ObjectNode(nodeKey, parentKey, Constants.NULL_REVISION_NUMBER, revisionNumber, NULL_KEY,
          leftSibKey, NULL_KEY, NULL_KEY, 0, 0, 0, hashFunction, (SirixDeweyID) null));
      return nodeKey;
    }
    final int recordBytes = ObjectNode.writeNewRecord(kvl.getSlottedPage(), absOffset,
        scratchObjectNode.getHeapOffsets(), nodeKey, parentKey, NULL_KEY, leftSibKey, NULL_KEY, NULL_KEY,
        Constants.NULL_REVISION_NUMBER, revisionNumber, 0, 0, 0);
    kvl.completeDirectWrite(NodeKind.OBJECT.getId(), nodeKey, slotOf(nodeKey), recordBytes, null);
    return nodeKey;
  }

  @Override
  public long createArrayNode(final long parentKey, final long leftSibKey, final long arrayPcr) {
    final long nodeKey = mint();
    noteUnnamedStructured(nodeKey, parentKey);
    if (indexTuples != null) {
      indexTuples.onPathEntry(arrayPcr, nodeKey);
    }
    final KeyValueLeafPage kvl = currentPage;
    final long absOffset = kvl.prepareHeapForDirectWriteOrOverflow(scratchArrayNode.estimateSerializedSize(), 0);
    if (absOffset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
      kvl.setRecord(new ArrayNode(nodeKey, parentKey, arrayPcr, Constants.NULL_REVISION_NUMBER, revisionNumber,
          NULL_KEY, leftSibKey, NULL_KEY, NULL_KEY, 0, 0, 0, hashFunction, (SirixDeweyID) null));
      return nodeKey;
    }
    final int recordBytes = ArrayNode.writeNewRecord(kvl.getSlottedPage(), absOffset, scratchArrayNode.getHeapOffsets(),
        nodeKey, parentKey, NULL_KEY, leftSibKey, NULL_KEY, NULL_KEY, arrayPcr, Constants.NULL_REVISION_NUMBER,
        revisionNumber, 0, 0, 0);
    kvl.completeDirectWrite(NodeKind.ARRAY.getId(), nodeKey, slotOf(nodeKey), recordBytes, null);
    return nodeKey;
  }

  @Override
  public long createObjectNamedObjectNode(final long parentKey, final long leftSibKey, final long pathNodeKey,
      final String name) {
    final int nameKey = resolvedNameKey(name);
    final long nodeKey = mint();
    for (final ProjectionChunkRowBatch batch : projectionBatches) {
      batch.onNamedObject(pathNodeKey);
    }
    if (indexTuples != null) {
      indexTuples.onPathEntry(pathNodeKey, nodeKey);
      indexTuples.onNameEntry(nameKey, nodeKey);
    }
    final KeyValueLeafPage kvl = currentPage;
    final long absOffset = kvl.prepareHeapForDirectWriteOrOverflow(scratchNamedObjectNode.estimateSerializedSize(), 0);
    if (absOffset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
      kvl.setRecord(new ObjectNamedObjectNode(nodeKey, parentKey, NULL_KEY, leftSibKey, NULL_KEY, NULL_KEY, nameKey,
          pathNodeKey, Constants.NULL_REVISION_NUMBER, revisionNumber, 0, 0, 0, hashFunction, (SirixDeweyID) null));
      return nodeKey;
    }
    final int recordBytes = ObjectNamedObjectNode.writeNewRecord(kvl.getSlottedPage(), absOffset,
        scratchNamedObjectNode.getHeapOffsets(), nodeKey, parentKey, NULL_KEY, leftSibKey, NULL_KEY, NULL_KEY, nameKey,
        pathNodeKey, Constants.NULL_REVISION_NUMBER, revisionNumber, 0L, 0L, 0L);
    kvl.completeDirectWrite(NodeKind.OBJECT_NAMED_OBJECT.getId(), nodeKey, slotOf(nodeKey), recordBytes, null);
    return nodeKey;
  }

  @Override
  public long createObjectNamedArrayNode(final long parentKey, final long leftSibKey, final long pathNodeKey,
      final String name) {
    final int nameKey = resolvedNameKey(name);
    final long nodeKey = mint();
    for (final ProjectionChunkRowBatch batch : projectionBatches) {
      batch.onNamedArray(pathNodeKey, nodeKey);
    }
    if (indexTuples != null) {
      indexTuples.onPathEntry(pathNodeKey, nodeKey);
      indexTuples.onNamedArrayMirrorCandidate(pathNodeKey, nodeKey);
      indexTuples.onNameEntry(nameKey, nodeKey);
    }
    final KeyValueLeafPage kvl = currentPage;
    final long absOffset = kvl.prepareHeapForDirectWriteOrOverflow(scratchNamedArrayNode.estimateSerializedSize(), 0);
    if (absOffset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
      kvl.setRecord(new ObjectNamedArrayNode(nodeKey, parentKey, NULL_KEY, leftSibKey, NULL_KEY, NULL_KEY, nameKey,
          pathNodeKey, Constants.NULL_REVISION_NUMBER, revisionNumber, 0, 0, 0, hashFunction, (SirixDeweyID) null));
      return nodeKey;
    }
    final int recordBytes = ObjectNamedArrayNode.writeNewRecord(kvl.getSlottedPage(), absOffset,
        scratchNamedArrayNode.getHeapOffsets(), nodeKey, parentKey, NULL_KEY, leftSibKey, NULL_KEY, NULL_KEY, nameKey,
        pathNodeKey, Constants.NULL_REVISION_NUMBER, revisionNumber, 0L, 0L, 0L);
    kvl.completeDirectWrite(NodeKind.OBJECT_NAMED_ARRAY.getId(), nodeKey, slotOf(nodeKey), recordBytes, null);
    return nodeKey;
  }

  // ==== leaves =================================================================================

  @Override
  public long createStringNode(final long parentKey, final long leftSibKey, final long rightSibKey, final byte[] utf8,
      final int utf8Length, final long notifyPcr) {
    final long nodeKey = mint();
    noteUnnamedString(nodeKey, parentKey, utf8, utf8Length);
    if (indexTuples != null) {
      indexTuples.onCasString(notifyPcr, nodeKey, utf8, utf8Length);
    }
    final KeyValueLeafPage kvl = currentPage;
    final long absOffset = kvl.prepareHeapForDirectWriteOrOverflow(55 + utf8Length, 0);
    if (absOffset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
      final byte[] valueCopy = new byte[utf8Length];
      System.arraycopy(utf8, 0, valueCopy, 0, utf8Length);
      final StringNode node = new StringNode(nodeKey, parentKey, Constants.NULL_REVISION_NUMBER, revisionNumber,
          rightSibKey, leftSibKey, 0, valueCopy, hashFunction, (SirixDeweyID) null, false, null);
      kvl.setRecord(node);
      return nodeKey;
    }
    final int recordBytes = StringNode.writeNewRecord(kvl.getSlottedPage(), absOffset,
        scratchStringNode.getHeapOffsets(), nodeKey, parentKey, rightSibKey, leftSibKey, Constants.NULL_REVISION_NUMBER,
        revisionNumber, utf8, 0, utf8Length, false);
    kvl.completeDirectWrite(NodeKind.STRING_VALUE.getId(), nodeKey, slotOf(nodeKey), recordBytes, null);
    return nodeKey;
  }

  @Override
  public long createObjectNamedStringNode(final long parentKey, final long leftSibKey, final long rightSibKey,
      final long pathNodeKey, final String name, final byte[] utf8, final int utf8Length) {
    final int nameKey = resolvedNameKey(name);
    final long nodeKey = mint();
    for (final ProjectionChunkRowBatch batch : projectionBatches) {
      batch.onNamedString(pathNodeKey, utf8, utf8Length);
    }
    if (indexTuples != null) {
      indexTuples.onPathEntry(pathNodeKey, nodeKey);
      indexTuples.onNameEntry(nameKey, nodeKey);
      indexTuples.onCasString(pathNodeKey, nodeKey, utf8, utf8Length);
    }
    final KeyValueLeafPage kvl = currentPage;
    final long absOffset = kvl.prepareHeapForDirectWriteOrOverflow(64 + utf8Length, 0);
    if (absOffset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
      final byte[] valueCopy = new byte[utf8Length];
      System.arraycopy(utf8, 0, valueCopy, 0, utf8Length);
      final ObjectNamedStringNode node = new ObjectNamedStringNode(nodeKey, parentKey, rightSibKey, leftSibKey, nameKey,
          pathNodeKey, Constants.NULL_REVISION_NUMBER, revisionNumber, 0, valueCopy, hashFunction, (SirixDeweyID) null,
          false, null);
      kvl.setRecord(node);
      return nodeKey;
    }
    final int recordBytes = ObjectNamedStringNode.writeNewRecord(kvl.getSlottedPage(), absOffset,
        scratchNamedStringNode.getHeapOffsets(), nodeKey, parentKey, rightSibKey, leftSibKey, nameKey, pathNodeKey,
        Constants.NULL_REVISION_NUMBER, revisionNumber, 0, utf8, 0, utf8Length, false);
    kvl.completeDirectWrite(NodeKind.OBJECT_NAMED_STRING.getId(), nodeKey, slotOf(nodeKey), recordBytes, null);
    return nodeKey;
  }

  @Override
  public long createNumberNode(final long parentKey, final long leftSibKey, final long rightSibKey, final Number value,
      final long notifyPcr) {
    final long nodeKey = mint();
    noteUnnamedNonString(nodeKey, parentKey);
    if (indexTuples != null) {
      indexTuples.onCasNumber(notifyPcr, nodeKey, value);
    }
    final KeyValueLeafPage kvl = currentPage;
    final long absOffset = kvl.prepareHeapForDirectWriteOrOverflow(NumberNode.estimateSerializedSize(value), 0);
    if (absOffset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
      kvl.setRecord(new NumberNode(nodeKey, parentKey, Constants.NULL_REVISION_NUMBER, revisionNumber, rightSibKey,
          leftSibKey, 0, value, hashFunction, (SirixDeweyID) null));
      return nodeKey;
    }
    final int recordBytes =
        NumberNode.writeNewRecord(kvl.getSlottedPage(), absOffset, scratchNumberNode.getHeapOffsets(), nodeKey,
            parentKey, rightSibKey, leftSibKey, Constants.NULL_REVISION_NUMBER, revisionNumber, value);
    kvl.completeDirectWrite(NodeKind.NUMBER_VALUE.getId(), nodeKey, slotOf(nodeKey), recordBytes, null);
    return nodeKey;
  }

  @Override
  public long createNumberNode(final long parentKey, final long leftSibKey, final long rightSibKey, final int value,
      final long notifyPcr) {
    final long nodeKey = mint();
    noteUnnamedNonString(nodeKey, parentKey);
    if (indexTuples != null) {
      indexTuples.onCasInt(notifyPcr, nodeKey, value);
    }
    final KeyValueLeafPage kvl = currentPage;
    final long absOffset = kvl.prepareHeapForDirectWriteOrOverflow(NumberNode.estimateSerializedIntSize(value), 0);
    if (absOffset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
      kvl.setRecord(new NumberNode(nodeKey, parentKey, Constants.NULL_REVISION_NUMBER, revisionNumber, rightSibKey,
          leftSibKey, 0, value, hashFunction, (SirixDeweyID) null));
      return nodeKey;
    }
    final int recordBytes =
        NumberNode.writeNewIntRecord(kvl.getSlottedPage(), absOffset, scratchNumberNode.getHeapOffsets(), nodeKey,
            parentKey, rightSibKey, leftSibKey, Constants.NULL_REVISION_NUMBER, revisionNumber, value);
    kvl.completeDirectWrite(NodeKind.NUMBER_VALUE.getId(), nodeKey, slotOf(nodeKey), recordBytes, null);
    return nodeKey;
  }

  @Override
  public long createNumberNode(final long parentKey, final long leftSibKey, final long rightSibKey, final long value,
      final long notifyPcr) {
    final long nodeKey = mint();
    noteUnnamedNonString(nodeKey, parentKey);
    if (indexTuples != null) {
      indexTuples.onCasLong(notifyPcr, nodeKey, value);
    }
    final KeyValueLeafPage kvl = currentPage;
    final long absOffset = kvl.prepareHeapForDirectWriteOrOverflow(NumberNode.estimateSerializedLongSize(value), 0);
    if (absOffset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
      kvl.setRecord(new NumberNode(nodeKey, parentKey, Constants.NULL_REVISION_NUMBER, revisionNumber, rightSibKey,
          leftSibKey, 0, value, hashFunction, (SirixDeweyID) null));
      return nodeKey;
    }
    final int recordBytes =
        NumberNode.writeNewLongRecord(kvl.getSlottedPage(), absOffset, scratchNumberNode.getHeapOffsets(), nodeKey,
            parentKey, rightSibKey, leftSibKey, Constants.NULL_REVISION_NUMBER, revisionNumber, value);
    kvl.completeDirectWrite(NodeKind.NUMBER_VALUE.getId(), nodeKey, slotOf(nodeKey), recordBytes, null);
    return nodeKey;
  }

  @Override
  public long createObjectNamedNumberNode(final long parentKey, final long leftSibKey, final long rightSibKey,
      final long pathNodeKey, final String name, final Number value) {
    final int nameKey = resolvedNameKey(name);
    final long nodeKey = mint();
    for (final ProjectionChunkRowBatch batch : projectionBatches) {
      batch.onNamedNumber(pathNodeKey, value);
    }
    if (indexTuples != null) {
      indexTuples.onPathEntry(pathNodeKey, nodeKey);
      indexTuples.onNameEntry(nameKey, nodeKey);
      indexTuples.onCasNumber(pathNodeKey, nodeKey, value);
    }
    final KeyValueLeafPage kvl = currentPage;
    final long absOffset =
        kvl.prepareHeapForDirectWriteOrOverflow(ObjectNamedNumberNode.estimateSerializedSize(value), 0);
    if (absOffset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
      kvl.setRecord(new ObjectNamedNumberNode(nodeKey, parentKey, rightSibKey, leftSibKey, nameKey, pathNodeKey,
          Constants.NULL_REVISION_NUMBER, revisionNumber, 0, value, hashFunction, (SirixDeweyID) null));
      return nodeKey;
    }
    final int recordBytes = ObjectNamedNumberNode.writeNewRecord(kvl.getSlottedPage(), absOffset,
        scratchNamedNumberNode.getHeapOffsets(), nodeKey, parentKey, rightSibKey, leftSibKey, nameKey, pathNodeKey,
        Constants.NULL_REVISION_NUMBER, revisionNumber, 0, value);
    kvl.completeDirectWrite(NodeKind.OBJECT_NAMED_NUMBER.getId(), nodeKey, slotOf(nodeKey), recordBytes, null);
    return nodeKey;
  }

  @Override
  public long createObjectNamedNumberNode(final long parentKey, final long leftSibKey, final long rightSibKey,
      final long pathNodeKey, final String name, final int value) {
    final int nameKey = resolvedNameKey(name);
    final long nodeKey = mint();
    for (final ProjectionChunkRowBatch batch : projectionBatches) {
      batch.onNamedInt(pathNodeKey, value);
    }
    if (indexTuples != null) {
      indexTuples.onPathEntry(pathNodeKey, nodeKey);
      indexTuples.onNameEntry(nameKey, nodeKey);
      indexTuples.onCasInt(pathNodeKey, nodeKey, value);
    }
    final KeyValueLeafPage kvl = currentPage;
    final long absOffset =
        kvl.prepareHeapForDirectWriteOrOverflow(ObjectNamedNumberNode.estimateSerializedIntSize(value), 0);
    if (absOffset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
      kvl.setRecord(new ObjectNamedNumberNode(nodeKey, parentKey, rightSibKey, leftSibKey, nameKey, pathNodeKey,
          Constants.NULL_REVISION_NUMBER, revisionNumber, 0, value, hashFunction, (SirixDeweyID) null));
      return nodeKey;
    }
    final int recordBytes = ObjectNamedNumberNode.writeNewIntRecord(kvl.getSlottedPage(), absOffset,
        scratchNamedNumberNode.getHeapOffsets(), nodeKey, parentKey, rightSibKey, leftSibKey, nameKey, pathNodeKey,
        Constants.NULL_REVISION_NUMBER, revisionNumber, 0, value);
    kvl.completeDirectWrite(NodeKind.OBJECT_NAMED_NUMBER.getId(), nodeKey, slotOf(nodeKey), recordBytes, null);
    return nodeKey;
  }

  @Override
  public long createObjectNamedNumberNode(final long parentKey, final long leftSibKey, final long rightSibKey,
      final long pathNodeKey, final String name, final long value) {
    final int nameKey = resolvedNameKey(name);
    final long nodeKey = mint();
    for (final ProjectionChunkRowBatch batch : projectionBatches) {
      batch.onNamedLong(pathNodeKey, value);
    }
    if (indexTuples != null) {
      indexTuples.onPathEntry(pathNodeKey, nodeKey);
      indexTuples.onNameEntry(nameKey, nodeKey);
      indexTuples.onCasLong(pathNodeKey, nodeKey, value);
    }
    final KeyValueLeafPage kvl = currentPage;
    final long absOffset =
        kvl.prepareHeapForDirectWriteOrOverflow(ObjectNamedNumberNode.estimateSerializedLongSize(value), 0);
    if (absOffset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
      kvl.setRecord(new ObjectNamedNumberNode(nodeKey, parentKey, rightSibKey, leftSibKey, nameKey, pathNodeKey,
          Constants.NULL_REVISION_NUMBER, revisionNumber, 0, value, hashFunction, (SirixDeweyID) null));
      return nodeKey;
    }
    final int recordBytes = ObjectNamedNumberNode.writeNewLongRecord(kvl.getSlottedPage(), absOffset,
        scratchNamedNumberNode.getHeapOffsets(), nodeKey, parentKey, rightSibKey, leftSibKey, nameKey, pathNodeKey,
        Constants.NULL_REVISION_NUMBER, revisionNumber, 0, value);
    kvl.completeDirectWrite(NodeKind.OBJECT_NAMED_NUMBER.getId(), nodeKey, slotOf(nodeKey), recordBytes, null);
    return nodeKey;
  }

  @Override
  public long createBooleanNode(final long parentKey, final long leftSibKey, final long rightSibKey,
      final boolean value, final long notifyPcr) {
    final long nodeKey = mint();
    noteUnnamedNonString(nodeKey, parentKey);
    if (indexTuples != null) {
      indexTuples.onCasBoolean(notifyPcr, nodeKey, value);
    }
    final KeyValueLeafPage kvl = currentPage;
    final long absOffset = kvl.prepareHeapForDirectWriteOrOverflow(scratchBooleanNode.estimateSerializedSize(), 0);
    if (absOffset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
      kvl.setRecord(new BooleanNode(nodeKey, parentKey, Constants.NULL_REVISION_NUMBER, revisionNumber, rightSibKey,
          leftSibKey, 0, value, hashFunction, (SirixDeweyID) null));
      return nodeKey;
    }
    final int recordBytes =
        BooleanNode.writeNewRecord(kvl.getSlottedPage(), absOffset, scratchBooleanNode.getHeapOffsets(), nodeKey,
            parentKey, rightSibKey, leftSibKey, Constants.NULL_REVISION_NUMBER, revisionNumber, value);
    kvl.completeDirectWrite(NodeKind.BOOLEAN_VALUE.getId(), nodeKey, slotOf(nodeKey), recordBytes, null);
    return nodeKey;
  }

  @Override
  public long createObjectNamedBooleanNode(final long parentKey, final long leftSibKey, final long rightSibKey,
      final long pathNodeKey, final String name, final boolean value) {
    final int nameKey = resolvedNameKey(name);
    final long nodeKey = mint();
    for (final ProjectionChunkRowBatch batch : projectionBatches) {
      batch.onNamedBoolean(pathNodeKey, value);
    }
    if (indexTuples != null) {
      indexTuples.onPathEntry(pathNodeKey, nodeKey);
      indexTuples.onNameEntry(nameKey, nodeKey);
      indexTuples.onCasBoolean(pathNodeKey, nodeKey, value);
    }
    final KeyValueLeafPage kvl = currentPage;
    final long absOffset = kvl.prepareHeapForDirectWriteOrOverflow(scratchNamedBooleanNode.estimateSerializedSize(), 0);
    if (absOffset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
      kvl.setRecord(new ObjectNamedBooleanNode(nodeKey, parentKey, rightSibKey, leftSibKey, nameKey, pathNodeKey,
          Constants.NULL_REVISION_NUMBER, revisionNumber, 0, value, hashFunction, (SirixDeweyID) null));
      return nodeKey;
    }
    final int recordBytes = ObjectNamedBooleanNode.writeNewRecord(kvl.getSlottedPage(), absOffset,
        scratchNamedBooleanNode.getHeapOffsets(), nodeKey, parentKey, rightSibKey, leftSibKey, nameKey, pathNodeKey,
        Constants.NULL_REVISION_NUMBER, revisionNumber, 0, value);
    kvl.completeDirectWrite(NodeKind.OBJECT_NAMED_BOOLEAN.getId(), nodeKey, slotOf(nodeKey), recordBytes, null);
    return nodeKey;
  }

  @Override
  public long createNullNode(final long parentKey, final long leftSibKey, final long rightSibKey,
      final long notifyPcr) {
    final long nodeKey = mint();
    noteUnnamedNonString(nodeKey, parentKey);
    final KeyValueLeafPage kvl = currentPage;
    final long absOffset = kvl.prepareHeapForDirectWriteOrOverflow(scratchNullNode.estimateSerializedSize(), 0);
    if (absOffset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
      kvl.setRecord(new NullNode(nodeKey, parentKey, Constants.NULL_REVISION_NUMBER, revisionNumber, rightSibKey,
          leftSibKey, 0, hashFunction, (SirixDeweyID) null));
      return nodeKey;
    }
    final int recordBytes = NullNode.writeNewRecord(kvl.getSlottedPage(), absOffset, scratchNullNode.getHeapOffsets(),
        nodeKey, parentKey, rightSibKey, leftSibKey, Constants.NULL_REVISION_NUMBER, revisionNumber);
    kvl.completeDirectWrite(NodeKind.NULL_VALUE.getId(), nodeKey, slotOf(nodeKey), recordBytes, null);
    return nodeKey;
  }

  @Override
  public long createObjectNamedNullNode(final long parentKey, final long leftSibKey, final long rightSibKey,
      final long pathNodeKey, final String name) {
    final int nameKey = resolvedNameKey(name);
    final long nodeKey = mint();
    for (final ProjectionChunkRowBatch batch : projectionBatches) {
      batch.onNamedNull(pathNodeKey);
    }
    if (indexTuples != null) {
      indexTuples.onPathEntry(pathNodeKey, nodeKey);
      indexTuples.onNameEntry(nameKey, nodeKey);
    }
    final KeyValueLeafPage kvl = currentPage;
    final long absOffset = kvl.prepareHeapForDirectWriteOrOverflow(scratchNamedNullNode.estimateSerializedSize(), 0);
    if (absOffset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
      kvl.setRecord(new ObjectNamedNullNode(nodeKey, parentKey, rightSibKey, leftSibKey, nameKey, pathNodeKey,
          Constants.NULL_REVISION_NUMBER, revisionNumber, 0, hashFunction, (SirixDeweyID) null));
      return nodeKey;
    }
    final int recordBytes = ObjectNamedNullNode.writeNewRecord(kvl.getSlottedPage(), absOffset,
        scratchNamedNullNode.getHeapOffsets(), nodeKey, parentKey, rightSibKey, leftSibKey, nameKey, pathNodeKey,
        Constants.NULL_REVISION_NUMBER, revisionNumber, 0);
    kvl.completeDirectWrite(NodeKind.OBJECT_NAMED_NULL.getId(), nodeKey, slotOf(nodeKey), recordBytes, null);
    return nodeKey;
  }

  // ==== fixups =================================================================================

  @Override
  public void fixupContainer(final long containerKey, final long firstChildKey, final long lastChildKey,
      final long childCount, final long rightSibKey) {
    final StructNode container = bindOwnContainer(containerKey);
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
    throw new IllegalStateException("a worker chunk never owns the document node");
  }

  @Override
  public void accountRecords(final int mutations) {
    // Workers have no epoch machinery; the coordinator accounts at adoption time.
  }

  /**
   * Binds a scratch flyweight to a container record in the worker's OWN un-adopted pages — the same
   * dir-entry mechanics as the transaction binder, safe here because nothing else can see these pages
   * yet.
   */
  private StructNode bindOwnContainer(final long containerKey) {
    if (containerKey < firstKey || containerKey >= nextKey) {
      throw new IllegalStateException(
          "container " + containerKey + " is outside this chunk's built range [" + firstKey + ", " + nextKey + ")");
    }
    final int pageIndex = (int) ((containerKey >>> 10) - (firstKey >>> 10));
    final KeyValueLeafPage page = pageIndex == pages.size()
        ? currentPage
        : pages.get(pageIndex);
    final int slot = slotOf(containerKey);
    final DataRecord materializedRecord = page.getRecord(slot);
    if (materializedRecord != null) {
      if (materializedRecord instanceof StructNode structNode) {
        return structNode;
      }
      throw new IllegalStateException(
          "unexpected materialized non-container kind " + materializedRecord.getKind() + " at " + containerKey);
    }
    final MemorySegment slottedPage = page.getSlottedPage();
    final int nodeKindId = PageLayout.getDirNodeKindId(slottedPage, slot);
    final long recordBase = PageLayout.heapAbsoluteOffset(PageLayout.getDirHeapOffset(slottedPage, slot));
    return switch (nodeKindId) {
      case 24 -> { // OBJECT
        scratchObjectNode.bind(slottedPage, recordBase, containerKey, slot);
        scratchObjectNode.setOwnerPage(page);
        yield scratchObjectNode;
      }
      case 25 -> { // ARRAY
        scratchArrayNode.bind(slottedPage, recordBase, containerKey, slot);
        scratchArrayNode.setOwnerPage(page);
        yield scratchArrayNode;
      }
      case 52 -> { // OBJECT_NAMED_OBJECT
        scratchNamedObjectNode.bind(slottedPage, recordBase, containerKey, slot);
        scratchNamedObjectNode.setOwnerPage(page);
        yield scratchNamedObjectNode;
      }
      case 53 -> { // OBJECT_NAMED_ARRAY
        scratchNamedArrayNode.bind(slottedPage, recordBase, containerKey, slot);
        scratchNamedArrayNode.setOwnerPage(page);
        yield scratchNamedArrayNode;
      }
      default -> throw new IllegalStateException("unexpected container kind id " + nodeKindId + " at " + containerKey);
    };
  }

}
