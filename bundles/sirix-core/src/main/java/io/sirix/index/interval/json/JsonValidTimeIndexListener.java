/*
 * [New BSD License]
 * Copyright (c) 2026, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.index.interval.json;

import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.sirix.access.trx.node.IndexController;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexType;
import io.sirix.index.PathNodeKeyChangeListener;
import io.sirix.index.interval.ValidTimeIntervalIndexWriter;
import io.sirix.index.interval.ValidTimeIntervalIndexWriter.Interval;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.NameNode;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.ValueNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.settings.Fixed;
import org.jspecify.annotations.Nullable;

import java.time.Instant;

/**
 * Incremental maintainer for a valid-time interval index.
 *
 * <p>
 * The listener receives one primitive change event per valid-time value-node
 * ({@code INSERT}/{@code DELETE}, with the field's local name and its string value). On the first
 * event for a record it reads that record's transaction-current bounds without moving the node
 * cursor. For a DELETE notification this is the still-persisted old interval; for an INSERT it
 * excludes the just-inserted node and thereby reconstructs the interval that preceded the insert.
 * The original interval is retained until the record's event burst ends, so a value replacement's
 * DELETE/INSERT pair becomes one old-delete/new-insert pair rather than two transient rewrites.
 * </p>
 *
 * <h2>Bounded coalescing</h2>
 * <p>
 * JSON mutation and shred notifications for one record are contiguous. When the containing object
 * key changes, the preceding record is reconciled and its state is retired. The final record is
 * reconciled before commit or asynchronous page flush. This bounds listener memory to one small
 * state object even for a 100M-record load; a non-contiguous revisit remains correct because the
 * earlier interval has already been published and the later burst re-seeds from the current record.
 * No document cursor is moved and no full index rebuild is involved.
 * </p>
 *
 * <h2>Object-key resolution</h2>
 * <p>
 * A valid-time value-node's containing record is its first ancestor that plays the OBJECT role:
 * either a plain {@link NodeKind#OBJECT} or a fused {@link NodeKind#OBJECT_NAMED_OBJECT}. The
 * parent-aware primitive notification normally makes this one record load; the compatibility
 * overload walks from the value node. Neither path moves the transaction cursor.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public final class JsonValidTimeIndexListener implements PathNodeKeyChangeListener {

  private static final long NO_OBJECT = Fixed.NULL_NODE_KEY.getStandardProperty();

  /** One contiguous record mutation's original registration and transaction-current bounds. */
  private static final class State {
    @Nullable
    Instant from;
    @Nullable
    Instant to;
    long fromNodeKey = NO_OBJECT;
    long toNodeKey = NO_OBJECT;
    long fromFieldCount;
    long toFieldCount;
    @Nullable
    Interval registered;
  }

  private final StorageEngineWriter storageEngineWriter;
  private final ValidTimeIntervalIndexWriter indexWriter;
  private final String validFromField;
  private final String validToField;

  private final State activeState = new State();
  private final State structuralSourceState = new State();
  private final State structuralDestinationState = new State();
  private long activeObjectKey = NO_OBJECT;
  private long structuralNodeKey = NO_OBJECT;
  private long structuralSourceObjectKey = NO_OBJECT;
  private boolean active;
  private boolean directBoundStructuralChange;

  public JsonValidTimeIndexListener(final StorageEngineWriter storageEngineWriter,
      final ValidTimeIntervalIndexWriter indexWriter, final String validFromField, final String validToField) {
    this.storageEngineWriter = storageEngineWriter;
    this.indexWriter = indexWriter;
    this.validFromField = validFromField;
    this.validToField = validToField;
  }

  @Override
  public void listen(final IndexController.ChangeType type, final ImmutableNode node, final long pathNodeKey) {
    // The interval index is maintained exclusively via the primitive (name+value) event below,
    // which carries the field name and instant value we need without a snapshot. The ImmutableNode
    // variant is a no-op (it is only invoked when no primitive listener path applies).
  }

  /**
   * Process a primitive change event for a (possible) valid-time value-node.
   *
   * @param type INSERT or DELETE
   * @param nodeKey the value-node's key
   * @param nodeKind the value-node's kind
   * @param pathNodeKey the value-node's path-class record (unused here)
   * @param name the field's local name (may be {@code null} — then the event is ignored)
   * @param value the field's string value (may be {@code null})
   */
  @Override
  public void listen(final IndexController.ChangeType type, final long nodeKey, final NodeKind nodeKind,
      final long pathNodeKey, final @Nullable QNm name, final @Nullable Str value) {
    onPrimitiveChange(type, nodeKey, nodeKind, NO_OBJECT, name, value);
  }

  @Override
  public void listen(final IndexController.ChangeType type, final long nodeKey, final NodeKind nodeKind,
      final long parentKey, final long pathNodeKey, final @Nullable QNm name, final @Nullable Str value) {
    onPrimitiveChange(type, nodeKey, nodeKind, parentKey, name, value);
  }

  private void onPrimitiveChange(final IndexController.ChangeType type, final long nodeKey, final NodeKind nodeKind,
      final long parentKey, final @Nullable QNm name, final @Nullable Str value) {
    if (directBoundStructuralChange && nodeKey == structuralNodeKey) {
      // A direct bound-field MOVE/rename is reconciled once from the before/after snapshots below.
      // Its primitive DELETE/INSERT pair cannot describe sibling order, and applying it as well
      // would publish the same record twice.
      return;
    }

    final boolean stringNode = nodeKind == NodeKind.OBJECT_NAMED_STRING || nodeKind == NodeKind.STRING_VALUE;
    final String local = name == null
        ? null
        : name.getLocalName();
    final boolean isFrom = stringNode && validFromField.equals(local);
    final boolean isTo = stringNode && validToField.equals(local);

    // Whole-object removal is post-order: an unrelated first child can be physically removed before
    // the first valid-time field callback. Seed on the first fused named DELETE of each object while
    // that direct child is still alive. Non-valid fields do not alter the state; they merely provide
    // the earliest safe snapshot point. The common INSERT path remains restricted to the two bounds.
    final boolean deletionSeed =
        type == IndexController.ChangeType.DELETE && name != null && nodeKind.isFusedObjectNamed();
    if (!isFrom && !isTo && !deletionSeed) {
      return;
    }

    final long objectKey;
    try {
      objectKey = parentKey == NO_OBJECT
          ? resolveContainingObjectKey(nodeKey)
          : resolveContainingObjectKeyFromParent(parentKey);
    } catch (final RuntimeException | Error failure) {
      // The parent-aware INSERT follows linkage, and a post-order DELETE may already have removed
      // earlier siblings. Losing ancestry at either point cannot be allowed to commit unindexed.
      markRollbackOnly(failure);
      throw failure;
    }
    if (objectKey == NO_OBJECT) {
      final IllegalStateException failure =
          new IllegalStateException("Unable to resolve containing object for valid-time node " + nodeKey);
      markRollbackOnly(failure);
      throw failure;
    }

    final State state = activeState;
    if (objectKey != activeObjectKey || !active) {
      reconcileActiveObject();
      try {
        seedState(objectKey, type == IndexController.ChangeType.INSERT
            ? nodeKey
            : NO_OBJECT,
            type == IndexController.ChangeType.DELETE
                ? nodeKey
                : NO_OBJECT,
            state);
      } catch (final RuntimeException | Error failure) {
        // INSERT follows linkage of the new record; a subtree DELETE may follow removal of earlier
        // post-order siblings. In either case a failed seed can occur after document publication and
        // must make the transaction non-committable.
        markRollbackOnly(failure);
        throw failure;
      }
      activeObjectKey = objectKey;
      active = true;
    }

    if (!isFrom && !isTo) {
      return;
    }

    try {
      if (type == IndexController.ChangeType.INSERT) {
        handleInsert(objectKey, nodeKey, isFrom, isTo, value, state);
      } else {
        handleDelete(objectKey, nodeKey, isFrom, isTo, state);
      }
    } catch (final RuntimeException | Error failure) {
      // INSERT follows linkage and duplicate DELETE/UPDATE reconciliation can inspect an already
      // changing record. A failed record-local rescan must never leave the document committable with
      // its old interval still registered.
      markRollbackOnly(failure);
      throw failure;
    }
  }

  /** Common unique-field INSERT stays O(1); only an actual duplicate needs a record-local rescan. */
  private void handleInsert(final long objectKey, final long nodeKey, final boolean isFrom, final boolean isTo,
      final @Nullable Str value, final State state) {
    if ((isFrom && isTo) || (isFrom && state.fromFieldCount != 0) || (isTo && state.toFieldCount != 0)) {
      // Builder semantics are document-order based: the first PARSEABLE duplicate wins. The newly
      // linked node may be before or after an existing winner, and a MOVE arrives as a DELETE/INSERT
      // pair, so derive both winners from the transaction-current direct children.
      readBounds(objectKey, NO_OBJECT, NO_OBJECT, state);
      return;
    }

    final Instant instant = ValidTimeIntervalIndexWriter.parseInstant(value == null
        ? null
        : value.stringValue());
    if (isFrom) {
      state.fromFieldCount = 1;
      state.from = instant;
      state.fromNodeKey = instant == null
          ? NO_OBJECT
          : nodeKey;
    } else {
      state.toFieldCount = 1;
      state.to = instant;
      state.toNodeKey = instant == null
          ? NO_OBJECT
          : nodeKey;
    }
  }

  /** Ignored-duplicate DELETE stays O(1); removing the selected winner discovers its successor. */
  private void handleDelete(final long objectKey, final long nodeKey, final boolean isFrom, final boolean isTo,
      final State state) {
    final boolean selectedFrom = isFrom && nodeKey == state.fromNodeKey;
    final boolean selectedTo = isTo && nodeKey == state.toNodeKey;

    if (isFrom && state.fromFieldCount > 0) {
      state.fromFieldCount--;
    }
    if (isTo && state.toFieldCount > 0) {
      state.toFieldCount--;
    }

    if (selectedFrom && state.fromFieldCount == 0) {
      state.from = null;
      state.fromNodeKey = NO_OBJECT;
    }
    if (selectedTo && state.toFieldCount == 0) {
      state.to = null;
      state.toNodeKey = NO_OBJECT;
    }

    if ((selectedFrom && state.fromFieldCount != 0) || (selectedTo && state.toFieldCount != 0)) {
      // DELETE fires before unlink/rewrite; excluding the event node computes the post-delete first
      // parseable winners. During post-order subtree removal, fallbackChildKey skips a stale,
      // already-removed prefix without retaining any per-record collection.
      readBounds(objectKey, nodeKey, nodeKey, state);
    }
  }

  /**
   * Seed the interval registered before the first event in a record burst. INSERT notifications are
   * delivered after the node is linked, so that one node is excluded while reconstructing the old
   * record. DELETE notifications are delivered before unlinking/mutation and therefore read the
   * complete old record.
   */
  private void seedState(final long objectKey, final long excludedNodeKey, final long fallbackChildKey,
      final State state) {
    readBounds(objectKey, excludedNodeKey, fallbackChildKey, state);
    state.registered = indexWriter.toInterval(state.from, state.to);
  }

  /** Reconcile and retire the one active mutation burst. */
  private void reconcileActiveObject() {
    reconcileActiveObject(false);
  }

  /**
   * Reconcile the active burst. A page pre-flush retains its tiny state because it can split the two
   * field events of one streaming record; ordinary transitions/commits retire it.
   */
  private void reconcileActiveObject(final boolean retainState) {
    if (!active) {
      return;
    }

    final State state = activeState;
    reconcileState(activeObjectKey, state);
    if (!retainState) {
      clearActiveObject();
    }
  }

  /** Reconcile one reusable record snapshot without retaining any per-record collection. */
  private void reconcileState(final long objectKey, final State state) {
    final Interval desired = indexWriter.toInterval(state.from, state.to);
    final Interval current = state.registered;
    if (current == null) {
      throw new IllegalStateException("Valid-time state for object " + objectKey + " has no registered interval");
    }

    final boolean same = current.present() == desired.present()
        && (!current.present() || current.lo() == desired.lo() && current.hi() == desired.hi());
    if (same) {
      return;
    }

    boolean publicationStarted = false;
    try {
      if (current.present()) {
        publicationStarted = true;
        indexWriter.delete(objectKey, current.lo(), current.hi());
      }
      if (desired.present()) {
        publicationStarted = true;
        indexWriter.insert(objectKey, desired.lo(), desired.hi());
      }
      state.registered = desired;
    } catch (final RuntimeException | Error failure) {
      if (publicationStarted) {
        markRollbackOnly(failure);
      }
      throw failure;
    }
  }

  private void markRollbackOnly(final Throwable failure) {
    try {
      storageEngineWriter.markTransactionRollbackOnly(failure);
    } catch (final RuntimeException | Error rollbackFailure) {
      if (failure != rollbackFailure) {
        try {
          failure.addSuppressed(rollbackFailure);
        } catch (final RuntimeException | Error ignored) {
          // Preserve the original maintenance failure even if suppression itself is unavailable.
        }
      }
    }
  }

  private void clearActiveObject() {
    activeObjectKey = NO_OBJECT;
    clearState(activeState);
    active = false;
  }

  private static void clearState(final State state) {
    state.from = null;
    state.to = null;
    state.fromNodeKey = NO_OBJECT;
    state.toNodeKey = NO_OBJECT;
    state.fromFieldCount = 0;
    state.toFieldCount = 0;
    state.registered = null;
  }

  @Override
  public void beforeCommit() {
    reconcileActiveObject();
  }

  @Override
  public void beforePageFlush() {
    reconcileActiveObject(true);
  }

  @Override
  public void transactionAborted() {
    clearActiveObject();
    clearStructuralChange();
  }

  /**
   * Snapshot a directly moved/renamed valid-time field before sibling or parent linkage changes.
   * Primitive notifications do not carry sibling order, so this one leaf is reconciled at the
   * structural boundary instead. Moving a container keeps the default primitive path because the
   * order of the valid-time fields inside each contained object does not change.
   */
  @Override
  public void beforeStructuralChange(final long movedNodeKey) {
    if (directBoundStructuralChange) {
      throw new IllegalStateException("Nested valid-time structural change for node " + movedNodeKey);
    }

    reconcileActiveObject();
    final ImmutableNode movedNode = loadNode(movedNodeKey);
    if (!isDirectBoundField(movedNode)) {
      return;
    }

    final long sourceObjectKey = resolveContainingObjectKeyFromParent(movedNode.getParentKey());
    if (sourceObjectKey == NO_OBJECT) {
      throw new IllegalStateException(
          "Unable to resolve source object for structurally changed valid-time node " + movedNodeKey);
    }

    seedState(sourceObjectKey, NO_OBJECT, NO_OBJECT, structuralSourceState);
    structuralNodeKey = movedNodeKey;
    structuralSourceObjectKey = sourceObjectKey;
    directBoundStructuralChange = true;
  }

  @Override
  public void afterStructuralChange(final long movedNodeKey) {
    if (!directBoundStructuralChange) {
      return;
    }
    if (movedNodeKey != structuralNodeKey) {
      final IllegalStateException failure = new IllegalStateException("Valid-time structural change ended for node "
          + movedNodeKey + " while node " + structuralNodeKey + " was active");
      markRollbackOnly(failure);
      throw failure;
    }

    try {
      final ImmutableNode movedNode = loadNode(movedNodeKey);
      if (movedNode == null || !movedNode.hasParent()) {
        throw new IllegalStateException("Structurally changed valid-time node " + movedNodeKey + " is unreadable");
      }

      final long destinationObjectKey = resolveContainingObjectKeyFromParent(movedNode.getParentKey());
      if (destinationObjectKey == structuralSourceObjectKey) {
        readBounds(structuralSourceObjectKey, NO_OBJECT, NO_OBJECT, structuralSourceState);
        reconcileState(structuralSourceObjectKey, structuralSourceState);
        return;
      }

      // Compute every post-surgery snapshot before publishing either side. If a read fails, the
      // transaction is latched rollback-only without leaving a half-applied source/destination pair.
      readBounds(structuralSourceObjectKey, NO_OBJECT, NO_OBJECT, structuralSourceState);
      if (destinationObjectKey != NO_OBJECT) {
        seedState(destinationObjectKey, movedNodeKey, movedNodeKey, structuralDestinationState);
        readBounds(destinationObjectKey, NO_OBJECT, NO_OBJECT, structuralDestinationState);
      }

      reconcileState(structuralSourceObjectKey, structuralSourceState);
      if (destinationObjectKey != NO_OBJECT) {
        reconcileState(destinationObjectKey, structuralDestinationState);
      }
    } catch (final RuntimeException | Error failure) {
      // The document surgery has completed by this point, even if interval publication has not.
      markRollbackOnly(failure);
      throw failure;
    } finally {
      clearStructuralChange();
    }
  }

  @Override
  public void structuralChangeAborted(final long movedNodeKey) {
    clearStructuralChange();
  }

  private boolean isDirectBoundField(final @Nullable ImmutableNode node) {
    if (node == null || node.getKind() != NodeKind.OBJECT_NAMED_STRING || !(node instanceof NameNode nameNode)
        || !node.hasParent()) {
      return false;
    }
    final String fieldName = storageEngineWriter.getName(nameNode.getLocalNameKey(), NodeKind.OBJECT_NAMED_OBJECT);
    return validFromField.equals(fieldName) || validToField.equals(fieldName);
  }

  private void clearStructuralChange() {
    structuralNodeKey = NO_OBJECT;
    structuralSourceObjectKey = NO_OBJECT;
    clearState(structuralSourceState);
    clearState(structuralDestinationState);
    directBoundStructuralChange = false;
  }

  /**
   * Walk from a valid-time value-node up to its containing object node-key by loading records from
   * the storage engine (no cursor movement).
   *
   * @return the containing object's node key, or {@link #NO_OBJECT} if none was found
   */
  private long resolveContainingObjectKey(final long valueNodeKey) {
    final ImmutableNode valueNode = loadNode(valueNodeKey);
    return valueNode == null || !valueNode.hasParent()
        ? NO_OBJECT
        : resolveContainingObjectKeyFromParent(valueNode.getParentKey());
  }

  private long resolveContainingObjectKeyFromParent(final long parentKey) {
    long key = parentKey;
    for (int hops = 0; hops < 4; hops++) {
      final ImmutableNode node = loadNode(key);
      if (node == null) {
        return NO_OBJECT;
      }
      if (node.getKind() == NodeKind.OBJECT || node.getKind() == NodeKind.OBJECT_NAMED_OBJECT) {
        return node.getNodeKey();
      }
      if (!node.hasParent()) {
        return NO_OBJECT;
      }
      key = node.getParentKey();
    }
    return NO_OBJECT;
  }

  /**
   * Read both direct string bounds of an object, optionally excluding one just-inserted node. This
   * deliberately mirrors {@link ValidTimeIntervalIndexWriter#readIntervalAtCursor}: only direct
   * named-string children participate, and a later duplicate can supply a parseable value if an
   * earlier duplicate is malformed.
   */
  private void readBounds(final long objectKey, final long excludedNodeKey, final long fallbackChildKey,
      final State state) {
    final ImmutableNode object = loadNode(objectKey);
    if (!(object instanceof StructNode structNode)) {
      throw new IllegalStateException("Valid-time record " + objectKey + " is no longer readable as an object");
    }

    Instant from = null;
    Instant to = null;
    long fromNodeKey = NO_OBJECT;
    long toNodeKey = NO_OBJECT;
    long fromFieldCount = 0;
    long toFieldCount = 0;
    long childKey = structNode.getFirstChildKey();
    boolean usedFallback = false;
    while (childKey != NO_OBJECT) {
      final ImmutableNode child = loadNode(childKey);
      if (child == null) {
        // Subtree removal visits descendants in post-order and removes earlier siblings before the
        // first valid-time DELETE event. The parent still carries its pre-removal first-child key,
        // so restart at the event node (which is guaranteed alive until this callback returns).
        if (!usedFallback && fallbackChildKey != NO_OBJECT && childKey != fallbackChildKey) {
          childKey = fallbackChildKey;
          usedFallback = true;
          continue;
        }
        throw new IllegalStateException(
            "Valid-time child " + childKey + " of record " + objectKey + " is no longer readable");
      }
      if (childKey != excludedNodeKey && child.getKind() == NodeKind.OBJECT_NAMED_STRING
          && child instanceof NameNode nameNode && child instanceof ValueNode valueNode) {
        final String fieldName = storageEngineWriter.getName(nameNode.getLocalNameKey(), NodeKind.OBJECT_NAMED_OBJECT);
        if (validFromField.equals(fieldName)) {
          fromFieldCount++;
          if (from == null) {
            from = ValidTimeIntervalIndexWriter.parseInstant(valueNode.getValue());
            if (from != null) {
              fromNodeKey = childKey;
            }
          }
        } else if (validToField.equals(fieldName)) {
          toFieldCount++;
          if (to == null) {
            to = ValidTimeIntervalIndexWriter.parseInstant(valueNode.getValue());
            if (to != null) {
              toNodeKey = childKey;
            }
          }
        }
      }
      childKey = child instanceof StructNode childStruct
          ? childStruct.getRightSiblingKey()
          : NO_OBJECT;
    }
    state.from = from;
    state.to = to;
    state.fromNodeKey = fromNodeKey;
    state.toNodeKey = toNodeKey;
    state.fromFieldCount = fromFieldCount;
    state.toFieldCount = toFieldCount;
  }

  private @Nullable ImmutableNode loadNode(final long key) {
    final DataRecord record = storageEngineWriter.getRecord(key, IndexType.DOCUMENT, 0);
    return record instanceof ImmutableNode node
        ? node
        : null;
  }
}
