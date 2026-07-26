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
import io.sirix.node.interfaces.immutable.ImmutableNode;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.jspecify.annotations.Nullable;

import java.time.Instant;

/**
 * Incremental maintainer for a valid-time interval index.
 *
 * <p>The listener receives one primitive change event per valid-time value-node
 * ({@code INSERT}/{@code DELETE}, with the field's local name and its string value). It accumulates,
 * per containing record OBJECT, the parsed {@code validFrom}/{@code validTo} instants seen so far,
 * and keeps the Relational-Interval-Tree in sync: whenever the {@code (validFrom, validTo)} pair's
 * mapped interval changes, it deletes the previously-registered interval for that object (if any)
 * and inserts the new one.</p>
 *
 * <h2>Why accumulate from the event payload</h2>
 * <p>During a streaming shred the two valid-time fields of one record arrive as SEPARATE events
 * (the sibling field may not exist yet when the first event fires). Re-reading both fields from a
 * cursor mid-transaction is unsafe (a separate read trx cannot see the writer's uncommitted nodes,
 * and the writer's own cursor must not be moved mid-insert). Accumulating the field values straight
 * from the change events sidesteps both problems and is order-independent: the object is registered
 * exactly when both fields have been observed, and re-registered on any later field update.</p>
 *
 * <h2>Object-key resolution</h2>
 * <p>A valid-time value-node's containing OBJECT is its first ancestor that plays the OBJECT role —
 * one hop for the fused {@code OBJECT_NAMED_STRING} shape (the field is a direct child of the
 * object), two for the legacy {@code STRING_VALUE -> OBJECT_KEY -> OBJECT} shape. Resolved by
 * loading the node(s) from the storage engine (no cursor movement).</p>
 *
 * @author Johannes Lichtenberger
 */
public final class JsonValidTimeIndexListener implements PathNodeKeyChangeListener {

  /** Per-object accumulation state: the instants seen + the interval currently registered. */
  private static final class State {
    @Nullable Instant from;
    @Nullable Instant to;
    boolean fromSeen;
    boolean toSeen;
    /** The interval currently registered in the RI-tree for this object ({@code null} = none). */
    @Nullable Interval registered;
  }

  private final StorageEngineWriter storageEngineWriter;
  private final ValidTimeIntervalIndexWriter indexWriter;
  private final String validFromField;
  private final String validToField;

  private final Long2ObjectMap<State> stateByObject = new Long2ObjectOpenHashMap<>();

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
   * @param type     INSERT or DELETE
   * @param nodeKey  the value-node's key
   * @param nodeKind the value-node's kind (unused; the field name + value drive the logic)
   * @param pathNodeKey the value-node's path-class record (unused here)
   * @param name     the field's local name (may be {@code null} — then the event is ignored)
   * @param value    the field's string value (may be {@code null})
   */
  @Override
  public void listen(final IndexController.ChangeType type, final long nodeKey, final NodeKind nodeKind,
      final long pathNodeKey, final @Nullable QNm name, final @Nullable Str value) {
    if (name == null) {
      return;
    }
    final String local = name.getLocalName();
    final boolean isFrom = validFromField.equals(local);
    final boolean isTo = validToField.equals(local);
    if (!isFrom && !isTo) {
      return;
    }

    final long objectKey = resolveContainingObjectKey(nodeKey);
    if (objectKey == Long.MIN_VALUE) {
      return;
    }

    final State state = stateByObject.computeIfAbsent(objectKey, k -> new State());

    final Instant instant = type == IndexController.ChangeType.DELETE
        ? null
        : ValidTimeIntervalIndexWriter.parseInstant(value == null ? null : value.stringValue());

    if (isFrom) {
      state.from = instant;
      state.fromSeen = type != IndexController.ChangeType.DELETE;
    } else {
      state.to = instant;
      state.toSeen = type != IndexController.ChangeType.DELETE;
    }

    reconcile(objectKey, state);
  }

  /**
   * Bring the RI-tree registration for {@code objectKey} in line with its current accumulated
   * {@code (from, to)} instants: delete the old interval if it differs, insert the new one.
   */
  private void reconcile(final long objectKey, final State state) {
    final Interval desired = indexWriter.toInterval(state.from, state.to);
    final Interval current = state.registered;

    final boolean same = current != null && desired.present() && current.present()
        && current.lo() == desired.lo() && current.hi() == desired.hi();
    if (same) {
      return;
    }

    if (current != null && current.present()) {
      indexWriter.delete(objectKey, current.lo(), current.hi());
    }
    if (desired.present()) {
      indexWriter.insert(objectKey, desired.lo(), desired.hi());
      state.registered = desired;
    } else {
      state.registered = null;
      // Drop fully-empty state to bound memory across a long transaction.
      if (!state.fromSeen && !state.toSeen) {
        stateByObject.remove(objectKey);
      }
    }
  }

  /**
   * Walk from a valid-time value-node up to its containing OBJECT node-key by loading nodes from the
   * storage engine (no cursor movement). At most a few hops: fused leaf -&gt; OBJECT (1 hop),
   * legacy {@code STRING_VALUE -> OBJECT_KEY -> OBJECT} (2 hops).
   *
   * @return the containing object's node key, or {@link Long#MIN_VALUE} if none was found
   */
  private long resolveContainingObjectKey(final long valueNodeKey) {
    long key = valueNodeKey;
    for (int hops = 0; hops < 4; hops++) {
      final ImmutableNode node = loadNode(key);
      if (node == null) {
        return Long.MIN_VALUE;
      }
      if (node.getKind() == NodeKind.OBJECT) {
        return node.getNodeKey();
      }
      if (!node.hasParent()) {
        return Long.MIN_VALUE;
      }
      key = node.getParentKey();
    }
    return Long.MIN_VALUE;
  }

  private @Nullable ImmutableNode loadNode(final long key) {
    try {
      final DataRecord record = storageEngineWriter.getRecord(key, IndexType.DOCUMENT, 0);
      return record instanceof ImmutableNode node ? node : null;
    } catch (final RuntimeException e) {
      return null;
    }
  }
}
