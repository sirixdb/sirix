/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.settings.Fixed;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.LongFunction;

/**
 * Projection-owned document ordering: the order labels this index sorts its rows by are minted and
 * stored by the index itself, never read off the document's nodes, so a projection index works on a
 * DEFAULT resource created with {@code storeDeweyIDs} off.
 *
 * <h2>Granularity</h2>
 * The directory holds ONE slot per PROJECTED RECORD plus one per node on a record's root path (the
 * record set's container chain, which every record of a set shares). Fields inside a record — the
 * overwhelming majority of a document's nodes, roughly thirteen out of every fourteen on the
 * ClickBench corpus — never take a slot and never cost the ingestion hot path a read or a write.
 *
 * <h2>Laziness</h2>
 * Nothing is written when a node changes. A label is minted the first time
 * {@link Accessor#fullLabel} needs it, which is exactly once per record, at the point where that
 * record's row is emitted. Minting places the new label strictly between the nearest LABELLED left
 * and right siblings, which makes the result independent of the order records are minted in: an
 * unlabelled sibling is always filled in later, strictly inside its own (correct) bounds.
 *
 * <h2>Bounded labels</h2>
 * ORDPATH's {@link SirixDeweyID#newBetween} grows a label by one division every few
 * insert-as-first-child operations at the same position, without bound. Once a minted label passes
 * {@link #REBALANCE_DIVISIONS} divisions a BOUNDED rebalance re-spreads a fixed-size window of
 * sibling records — never the whole sibling list, never the whole index — over the gap between the
 * window's outer neighbours, restoring short labels and fresh slack. Relabelled records are reported
 * to the {@link RelabelSink} so their persisted row-group order labels are rewritten with them.
 */
final class ProjectionStructuralOrderDirectory {
  static final long BASE = 1L << 50;
  static final long NODE_KEY_LIMIT = 1L << 48;
  static final long LIMIT = BASE + NODE_KEY_LIMIT;

  private static final long NULL_NODE_KEY = Fixed.NULL_NODE_KEY.getStandardProperty();
  private static final byte FORMAT_VERSION = 1;
  private static final int FORMAT_HEADER_BYTES = 1;
  private static final int MAX_LOCAL_LABEL_BYTES = 1 << 14;
  private static final int MAX_ANCESTORS = 1 << 15;
  private static final int MAX_FULL_LABEL_DIVISIONS = 1 << 16;

  /** Divisions a freshly minted local label may reach before the bounded rebalance takes over. */
  private static final int REBALANCE_DIVISIONS = 8;
  /** Sibling windows the rebalance may try, in order. Every entry is a hard bound on its work. */
  private static final int[] REBALANCE_WINDOWS = { 32, 128, 512 };
  /** Odd-value slack a spread reserves per item when its interval is open-ended. */
  private static final int SPREAD_SLACK = 1 << 12;
  /** Hard bound on how many divisions one spread may descend through before it fits. */
  private static final int MAX_SPREAD_DESCENTS = 8;
  /** Even division an exhausted append sequence carries through; it adds no odd division. */
  private static final int APPEND_CARRY_DIVISION = 2;
  /** Division the very first label under a parent takes, leaving slack on BOTH sides. */
  private static final int FIRST_LOCAL_DIVISION = (SPREAD_SLACK << 1) + 1;

  private ProjectionStructuralOrderDirectory() {
  }

  /**
   * Receives the records a bounded rebalance re-labelled, and decides which labelled siblings may
   * take part in one at all. Every mint takes one — there is no way to pass "no sink" and silently
   * leave the rebalance unarmed; a caller that genuinely cannot rewrite persisted rows names that
   * fact by passing {@link #SEALED}.
   */
  interface RelabelSink {
    /**
     * For callers whose rows are not rewritable: a full build and a bulk load emit their rows in
     * document order into storage they have already sealed, so no sibling of theirs may be
     * re-spread. Those paths also cannot GROW a label — a run is spread over its interval in one
     * pass and an append extends the previous label — so declining here costs them nothing.
     */
    RelabelSink SEALED = new RelabelSink() {
      @Override
      public boolean canRelabel(final long nodeKey) {
        return false;
      }

      @Override
      public void relabelled(final long nodeKey, final SirixDeweyID localLabel) {
        throw new IllegalStateException("a sealed projection order sink cannot re-label node " + nodeKey);
      }
    };

    /**
     * Whether this labelled sibling is a projected record whose persisted order label the caller can
     * rewrite. A labelled node that is NOT a record (a record-set container) must never be
     * re-labelled: its label is a shared prefix of every record beneath it, so moving it would
     * invalidate an unbounded number of rows.
     */
    boolean canRelabel(long nodeKey);

    /** The record now carries {@code localLabel}; its persisted row-group label must follow. */
    void relabelled(long nodeKey, SirixDeweyID localLabel);
  }

  static Accessor open(final ProjectionIndexHOTStorage storage) {
    return new Accessor(Objects.requireNonNull(storage));
  }

  static boolean ownsSlot(final long slotKey) {
    return slotKey >= BASE && slotKey < LIMIT;
  }

  private static long slotKey(final long nodeKey) {
    validateNodeKey(nodeKey, "node");
    return BASE + nodeKey;
  }

  private static void validateNodeKey(final long nodeKey, final String role) {
    if (nodeKey < 0 || nodeKey >= NODE_KEY_LIMIT) {
      throw new IllegalArgumentException(role + " key is outside the projection structural-order namespace: " + nodeKey);
    }
  }

  private static void validateOptionalNodeKey(final long nodeKey, final String role) {
    if (nodeKey != NULL_NODE_KEY) {
      validateNodeKey(nodeKey, role);
    }
  }

  static final class Accessor {
    private final ProjectionIndexHOTStorage storage;

    private Accessor(final ProjectionIndexHOTStorage storage) {
      this.storage = storage;
    }

    /** Establish the document root's label so every record's ancestry terminates in a known slot. */
    void seedRoot(final long nodeKey) {
      validateNodeKey(nodeKey, "document root");
      if (localLabel(nodeKey) == null) {
        putLocalLabel(nodeKey, firstLocalLabel());
      }
    }

    void remove(final long nodeKey) {
      storage.tombstoneStructuralOrderSlot(slotKey(nodeKey));
    }

    /**
     * A node whose position changed (a subtree move) may now sit outside its own label's interval.
     * Re-mint it in that case, and ONLY in that case. An unlabelled node is left alone: it is not on
     * a record's root path yet, and {@link #fullLabel} mints it against its new neighbours when it
     * becomes one.
     */
    void relabelDisplaced(final long nodeKey, final LongFunction<ImmutableNode> nodeLookup,
        final RelabelSink sink) {
      validateNodeKey(nodeKey, "node");
      Objects.requireNonNull(nodeLookup);
      Objects.requireNonNull(sink);
      final SirixDeweyID existing = localLabel(nodeKey);
      if (existing == null) {
        return;
      }
      final SirixDeweyID left = neighbourLabel(nodeKey, true, nodeLookup);
      final SirixDeweyID right = neighbourLabel(nodeKey, false, nodeLookup);
      if ((left == null || left.compareTo(existing) < 0) && (right == null || existing.compareTo(right) < 0)) {
        return;
      }
      mint(nodeKey, left, right, nodeLookup, sink);
    }

    /**
     * The record's document-order label: its own local label prefixed by every ancestor's, minted on
     * demand. {@code sink} governs the bounded rebalance for the node ITSELF only — a container's
     * label is a shared prefix of every record beneath it, so it is always minted {@link
     * RelabelSink#SEALED} and never re-spread.
     */
    SirixDeweyID fullLabel(final long nodeKey, final LongFunction<ImmutableNode> nodeLookup,
        final RelabelSink sink) {
      validateNodeKey(nodeKey, "node");
      Objects.requireNonNull(nodeLookup);
      Objects.requireNonNull(sink);

      long[] ancestry = new long[Math.min(16, MAX_ANCESTORS)];
      int depth = 1;
      ancestry[0] = nodeKey;
      long tortoise = nodeKey;
      long hare = parentKey(nodeKey, nodeLookup);
      int power = 1;
      int cycleLength = 1;
      while (hare != NULL_NODE_KEY) {
        if (tortoise == hare) {
          throw new IllegalStateException("cycle in projection structural ancestry at node " + hare);
        }
        if (depth == MAX_ANCESTORS) {
          throw new IllegalStateException("projection structural ancestry exceeds " + MAX_ANCESTORS + " nodes");
        }
        if (depth == ancestry.length) {
          ancestry = Arrays.copyOf(ancestry, Math.min(MAX_ANCESTORS, Math.multiplyExact(depth, 2)));
        }
        ancestry[depth++] = hare;
        if (power == cycleLength) {
          tortoise = hare;
          power = Math.multiplyExact(power, 2);
          cycleLength = 0;
        }
        hare = parentKey(hare, nodeLookup);
        cycleLength++;
      }

      int divisionCount = 1;
      int initialCapacity = Math.max(16, Math.min(MAX_FULL_LABEL_DIVISIONS, Math.addExact(1, depth * 2)));
      int[] divisions = new int[initialCapacity];
      divisions[0] = 1;
      for (int ancestryIndex = depth - 1; ancestryIndex >= 0; ancestryIndex--) {
        final long ancestorKey = ancestry[ancestryIndex];
        final SirixDeweyID localLabel = ancestryIndex == 0
            ? ensureLocalLabel(ancestorKey, nodeLookup, sink, false)
            : ensureLocalLabel(ancestorKey, nodeLookup, RelabelSink.SEALED, true);
        final int[] localDivisions = localLabel.getDivisionValues();
        final int suffixLength = localDivisions.length - 1;
        final int required = Math.addExact(divisionCount, suffixLength);
        if (required > MAX_FULL_LABEL_DIVISIONS) {
          throw new IllegalStateException(
              "projection structural label exceeds " + MAX_FULL_LABEL_DIVISIONS + " divisions");
        }
        if (required > divisions.length) {
          int capacity = divisions.length;
          while (capacity < required) {
            capacity = Math.min(MAX_FULL_LABEL_DIVISIONS, Math.multiplyExact(capacity, 2));
          }
          divisions = Arrays.copyOf(divisions, capacity);
        }
        System.arraycopy(localDivisions, 1, divisions, divisionCount, suffixLength);
        divisionCount = required;
      }

      final SirixDeweyID fullLabel = new SirixDeweyID(divisionCount, divisions);
      if (fullLabel.toBytes().length > ProjectionIndexRowGroupPage.MAX_ORDER_LABEL_BYTES) {
        throw new IllegalStateException("projection structural label exceeds the bounded order-label lane");
      }
      return fullLabel;
    }

    /**
     * Label {@code nodeKey}, and with it the whole maximal RUN of consecutive unlabelled siblings it
     * belongs to.
     *
     * <p>
     * Minting one node at a time by probing outwards for a labelled bound is quadratic: every member
     * of an N-long unlabelled run would walk the rest of the run to find its bound, so a build over
     * an existing array — where no record is labelled yet — costs N²/2 document reads. The bounding
     * pair is therefore resolved ONCE for the run and the whole run is assigned in a single pass:
     * three sibling steps per record, whatever the run's length, and no cap that could refuse a long
     * but perfectly valid array.
     */
    private SirixDeweyID ensureLocalLabel(final long nodeKey, final LongFunction<ImmutableNode> nodeLookup,
        final RelabelSink sink, final boolean container) {
      final SirixDeweyID existing = localLabel(nodeKey);
      if (existing != null) {
        return existing;
      }
      if (container) {
        // A container is labelled ALONE. Run minting exists to amortise a run of projected records
        // over one bound resolution; a container has no run — the siblings beside it are ordinary
        // document nodes that no record's root path crosses. Handing them slots would make them
        // rebalance-ineligible neighbours of a container, and a container's label is a shared prefix
        // of every record beneath it, so it can never be re-spread away from them: a repeatedly
        // moved container would then grow its label with nothing able to reclaim it.
        return mint(nodeKey, neighbourLabel(nodeKey, true, nodeLookup),
            neighbourLabel(nodeKey, false, nodeLookup), nodeLookup, sink);
      }
      mintRun(nodeKey, nodeLookup, sink);
      return requireLocalLabel(nodeKey, "freshly minted");
    }

    private void mintRun(final long nodeKey, final LongFunction<ImmutableNode> nodeLookup,
        final RelabelSink sink) {
      final long headKey = runHead(nodeKey, nodeLookup);
      final long lowerKey = siblingKey(headKey, true, nodeLookup);
      final SirixDeweyID lower = lowerKey == NULL_NODE_KEY
          ? null
          : requireLocalLabel(lowerKey, "left sibling");

      long tailKey = headKey;
      long upperKey = NULL_NODE_KEY;
      long count = 1L;
      for (;;) {
        final long next = siblingKey(tailKey, false, nodeLookup);
        if (next == NULL_NODE_KEY) {
          break;
        }
        if (localLabel(next) != null) {
          upperKey = next;
          break;
        }
        tailKey = next;
        count = Math.incrementExact(count);
        guardSiblingChain(count);
      }
      final SirixDeweyID upper = upperKey == NULL_NODE_KEY
          ? null
          : requireLocalLabel(upperKey, "right sibling");
      if (lower != null && upper != null && lower.compareTo(upper) >= 0) {
        throw new IllegalStateException("structural sibling labels are not strictly ordered");
      }

      final SirixDeweyID assigned = upper == null
          ? appendRun(headKey, count, lower, nodeLookup)
          : spreadRun(headKey, count, lower, upper, nodeKey, nodeLookup);
      if (assigned.getDivisionValues().length > REBALANCE_DIVISIONS) {
        // The interval this run had to fit into was tight enough to need extra divisions. Every
        // member is labelled now, so the bounded rebalance can widen the neighbourhood.
        rebalance(nodeKey, nodeLookup, sink);
      }
    }

    /**
     * Nothing labelled follows the run, so each member simply extends the previous label — the same
     * constant-division step an append has always taken, and no interval arithmetic to get wrong.
     *
     * @return the label the run's LAST member received
     */
    private SirixDeweyID appendRun(final long headKey, final long count, final @Nullable SirixDeweyID lower,
        final LongFunction<ImmutableNode> nodeLookup) {
      SirixDeweyID previous = lower;
      long cursor = headKey;
      SirixDeweyID assigned = null;
      for (long index = 0L; index < count; index++) {
        final SirixDeweyID label = previous == null
            ? firstLocalLabel()
            : nextAppendLabel(previous);
        putLocalLabel(cursor, label);
        previous = label;
        assigned = label;
        if (index + 1L < count) {
          cursor = requireSibling(cursor, false, nodeLookup);
        }
      }
      return assigned;
    }

    /**
     * The run is bounded above, so its members are spread evenly over the open interval. The spread
     * is generated lazily — a run may be arbitrarily long, and materialising one label array per
     * member would be a humongous allocation.
     *
     * @return the label {@code requestedKey} received
     */
    private SirixDeweyID spreadRun(final long headKey, final long count, final @Nullable SirixDeweyID lower,
        final SirixDeweyID upper, final long requestedKey, final LongFunction<ImmutableNode> nodeLookup) {
      final Spread spread = Spread.between(lower == null ? null : lower.getDivisionValues(),
          upper.getDivisionValues(), count);
      long cursor = headKey;
      SirixDeweyID assigned = null;
      for (long index = 0L; index < count; index++) {
        final SirixDeweyID label = new SirixDeweyID(spread.at(index));
        putLocalLabel(cursor, label);
        if (cursor == requestedKey) {
          assigned = label;
        }
        if (index + 1L < count) {
          cursor = requireSibling(cursor, false, nodeLookup);
        }
      }
      if (assigned == null) {
        throw new IllegalStateException("projection structural-order run did not cover node " + requestedKey);
      }
      return assigned;
    }

    /** The leftmost node of the maximal run of consecutive UNLABELLED siblings containing this one. */
    private long runHead(final long nodeKey, final LongFunction<ImmutableNode> nodeLookup) {
      long head = nodeKey;
      long steps = 0L;
      for (;;) {
        final long previous = siblingKey(head, true, nodeLookup);
        if (previous == NULL_NODE_KEY || localLabel(previous) != null) {
          return head;
        }
        head = previous;
        guardSiblingChain(++steps);
      }
    }

    private long requireSibling(final long nodeKey, final boolean leftward,
        final LongFunction<ImmutableNode> nodeLookup) {
      final long sibling = siblingKey(nodeKey, leftward, nodeLookup);
      if (sibling == NULL_NODE_KEY) {
        throw new IllegalStateException("projection structural-order run lost its sibling after node " + nodeKey);
      }
      return sibling;
    }

    private SirixDeweyID mint(final long nodeKey, final @Nullable SirixDeweyID left,
        final @Nullable SirixDeweyID right, final LongFunction<ImmutableNode> nodeLookup,
        final RelabelSink sink) {
      if (left != null && right != null && left.compareTo(right) >= 0) {
        throw new IllegalStateException("structural sibling labels are not strictly ordered");
      }
      final SirixDeweyID minted = left == null && right == null
          ? firstLocalLabel()
          : SirixDeweyID.newBetween(left, right);
      if (minted.getDivisionValues().length > REBALANCE_DIVISIONS) {
        putLocalLabel(nodeKey, minted);
        final SirixDeweyID rebalanced = rebalance(nodeKey, nodeLookup, sink);
        return rebalanced != null ? rebalanced : minted;
      }
      putLocalLabel(nodeKey, minted);
      return minted;
    }

    /**
     * Re-spread a BOUNDED window of sibling records over the gap between the window's outer
     * neighbours. Work is capped by {@link #REBALANCE_WINDOWS}: at most that many sibling slots and
     * that many persisted rows are touched, never the sibling list, the bitmaps, the trie or the
     * projection as a whole.
     *
     * @return the label the node itself received, or {@code null} when no sibling could take part
     *         and the caller must keep its own minted label
     */
    private @Nullable SirixDeweyID rebalance(final long nodeKey, final LongFunction<ImmutableNode> nodeLookup,
        final RelabelSink sink) {
      for (int attempt = 0; attempt < REBALANCE_WINDOWS.length; attempt++) {
        final int window = REBALANCE_WINDOWS[attempt];
        final LongArrayList before = new LongArrayList(window);
        final long lowerBoundKey = collectWindow(nodeKey, true, window, sink, nodeLookup, before);
        final LongArrayList after = new LongArrayList(window);
        final long upperBoundKey = collectWindow(nodeKey, false, window, sink, nodeLookup, after);
        if (before.isEmpty() && after.isEmpty()) {
          return null;
        }

        final long[] items = new long[before.size() + 1 + after.size()];
        int at = 0;
        for (int index = before.size() - 1; index >= 0; index--) {
          items[at++] = before.getLong(index);
        }
        final int selfIndex = at;
        items[at++] = nodeKey;
        for (int index = 0; index < after.size(); index++) {
          items[at++] = after.getLong(index);
        }

        final int[] lower = lowerBoundKey == NULL_NODE_KEY
            ? null
            : requireLocalLabel(lowerBoundKey, "rebalance lower bound").getDivisionValues();
        final int[] upper = upperBoundKey == NULL_NODE_KEY
            ? null
            : requireLocalLabel(upperBoundKey, "rebalance upper bound").getDivisionValues();
        final int[][] spread = spreadLocalLabels(lower, upper, items.length);
        if (spread[selfIndex].length > REBALANCE_DIVISIONS && attempt < REBALANCE_WINDOWS.length - 1) {
          // The neighbourhood is denser than this window can relieve: widen it ONCE more, still
          // within a fixed cap, rather than leaving an unbounded label behind.
          continue;
        }

        SirixDeweyID assigned = null;
        for (int index = 0; index < items.length; index++) {
          final SirixDeweyID label = new SirixDeweyID(spread[index]);
          putLocalLabel(items[index], label);
          if (items[index] == nodeKey) {
            assigned = label;
          } else {
            sink.relabelled(items[index], label);
          }
        }
        return assigned;
      }
      return null;
    }

    /**
     * Walk up to {@code window} labelled, rebalanceable siblings in one direction.
     *
     * @return the first labelled sibling BEYOND the window (its label bounds the spread), or
     *         {@link #NULL_NODE_KEY} when the labelled sibling run ends first
     */
    private long collectWindow(final long from, final boolean leftward, final int window,
        final RelabelSink sink, final LongFunction<ImmutableNode> nodeLookup, final LongArrayList collected) {
      long cursor = from;
      for (int index = 0; index < window; index++) {
        final long sibling = labelledSibling(cursor, leftward, nodeLookup);
        if (sibling == NULL_NODE_KEY || !sink.canRelabel(sibling)) {
          return sibling;
        }
        collected.add(sibling);
        cursor = sibling;
      }
      return labelledSibling(cursor, leftward, nodeLookup);
    }

    private @Nullable SirixDeweyID neighbourLabel(final long nodeKey, final boolean leftward,
        final LongFunction<ImmutableNode> nodeLookup) {
      final long sibling = labelledSibling(nodeKey, leftward, nodeLookup);
      return sibling == NULL_NODE_KEY
          ? null
          : requireLocalLabel(sibling, leftward ? "left sibling" : "right sibling");
    }

    /**
     * The nearest sibling in one direction that already owns a slot, skipping unlabelled ones.
     *
     * <p>
     * Callers that mint reach this only for the single-node case; a run is bounded once by
     * {@link #mintRun} instead. There is deliberately no step cap here: refusing to label a valid
     * document because one of its arrays is long would abort an otherwise valid transaction. The
     * only bound is the node-key space itself, which a sibling chain cannot exceed without a cycle.
     */
    private long labelledSibling(final long nodeKey, final boolean leftward,
        final LongFunction<ImmutableNode> nodeLookup) {
      long cursor = nodeKey;
      long steps = 0L;
      for (;;) {
        final long sibling = siblingKey(cursor, leftward, nodeLookup);
        if (sibling == NULL_NODE_KEY) {
          return NULL_NODE_KEY;
        }
        if (localLabel(sibling) != null) {
          return sibling;
        }
        cursor = sibling;
        guardSiblingChain(++steps);
      }
    }

    private long siblingKey(final long nodeKey, final boolean leftward,
        final LongFunction<ImmutableNode> nodeLookup) {
      final ImmutableNode node = nodeLookup.apply(nodeKey);
      if (!(node instanceof final StructNode structural)) {
        return NULL_NODE_KEY;
      }
      final long sibling = leftward
          ? structural.getLeftSiblingKey()
          : structural.getRightSiblingKey();
      validateOptionalNodeKey(sibling, "sibling");
      if (sibling == nodeKey) {
        throw new IllegalStateException("cycle in projection structural sibling chain at node " + sibling);
      }
      return sibling;
    }

    private static void guardSiblingChain(final long steps) {
      if (steps >= NODE_KEY_LIMIT) {
        throw new IllegalStateException("cycle in projection structural sibling chain");
      }
    }

    private static long parentKey(final long nodeKey, final LongFunction<ImmutableNode> nodeLookup) {
      if (nodeKey == NULL_NODE_KEY) {
        return NULL_NODE_KEY;
      }
      validateNodeKey(nodeKey, "ancestor");
      final ImmutableNode node = nodeLookup.apply(nodeKey);
      if (node == null) {
        throw new IllegalStateException("missing structural node " + nodeKey + " while resolving projection order");
      }
      if (node.getNodeKey() != nodeKey) {
        throw new IllegalStateException(
            "structural node lookup returned " + node.getNodeKey() + " for requested node " + nodeKey);
      }
      final long parentKey = node.getParentKey();
      validateOptionalNodeKey(parentKey, "parent");
      if (parentKey == nodeKey) {
        throw new IllegalStateException("structural node " + nodeKey + " is its own parent");
      }
      return parentKey;
    }

    private SirixDeweyID requireLocalLabel(final long nodeKey, final String role) {
      final SirixDeweyID localLabel = localLabel(nodeKey);
      if (localLabel == null) {
        throw new IllegalStateException("missing projection structural-order label for " + role + " node " + nodeKey);
      }
      return localLabel;
    }

    private @Nullable SirixDeweyID localLabel(final long nodeKey) {
      final byte[] encoded = storage.getStructuralOrderSlot(slotKey(nodeKey));
      if (encoded == null) {
        return null;
      }
      if (encoded.length <= FORMAT_HEADER_BYTES || encoded[0] != FORMAT_VERSION
          || encoded.length - FORMAT_HEADER_BYTES > MAX_LOCAL_LABEL_BYTES) {
        throw new IllegalStateException("invalid projection structural-order encoding for node " + nodeKey);
      }

      final SirixDeweyID localLabel;
      try {
        localLabel = new SirixDeweyID(encoded, FORMAT_HEADER_BYTES, encoded.length - FORMAT_HEADER_BYTES);
      } catch (final RuntimeException exception) {
        throw new IllegalStateException("invalid projection structural-order label for node " + nodeKey, exception);
      }
      final int[] divisions = localLabel.getDivisionValues();
      final byte[] canonical = localLabel.toBytes();
      if (divisions.length < 2 || divisions.length > MAX_FULL_LABEL_DIVISIONS || divisions[0] != 1
          || localLabel.getLevel() != 1 || containsInvalidLocalDivision(divisions)
          || !Arrays.equals(canonical, 0, canonical.length,
              encoded, FORMAT_HEADER_BYTES, encoded.length)) {
        throw new IllegalStateException("invalid projection structural-order label for node " + nodeKey);
      }
      return localLabel;
    }

    private void putLocalLabel(final long nodeKey, final SirixDeweyID localLabel) {
      final int[] divisions = localLabel.getDivisionValues();
      final byte[] payload = localLabel.toBytes();
      if (divisions.length < 2 || divisions.length > MAX_FULL_LABEL_DIVISIONS || divisions[0] != 1
          || localLabel.getLevel() != 1 || containsInvalidLocalDivision(divisions) || payload.length == 0
          || payload.length > MAX_LOCAL_LABEL_BYTES) {
        throw new IllegalStateException("projection structural-order label exceeds its bounded local encoding");
      }
      final byte[] encoded = new byte[Math.addExact(FORMAT_HEADER_BYTES, payload.length)];
      encoded[0] = FORMAT_VERSION;
      System.arraycopy(payload, 0, encoded, FORMAT_HEADER_BYTES, payload.length);
      storage.putStructuralOrderSlot(slotKey(nodeKey), encoded);
    }

    /**
     * One step of an open-ended append sequence.
     *
     * <p>
     * {@link SirixDeweyID#newBetween(SirixDeweyID, SirixDeweyID)}'s open-ended branch advances the
     * final division by an unchecked {@code int} addition, so a long enough append run under one
     * parent wraps it negative — from {@link #FIRST_LOCAL_DIVISION} stepping by the sibling distance
     * that is reachable in the hundreds of millions of records. The wrap then surfaces from
     * {@code toBytes()} as an encoder complaint about a negative division, which names neither
     * ordering nor the append that caused it.
     *
     * <p>
     * The carry is applied HERE rather than inside {@code newBetween} because that method's contract
     * is to return a SIBLING at the same level: it forces the operand's level onto the result, and
     * for the document's own Dewey IDs an extra division is a descendant, not a sibling. Carrying
     * there would relocate nodes in the document's Dewey space. A projection order label is only
     * ever compared, never read as a tree position, so it may carry.
     *
     * <p>
     * The carry appends an EVEN division, which keeps the label's odd-division count — and therefore
     * the level a byte round-trip recomputes — exactly what {@link #putLocalLabel} requires, while
     * being strictly greater because the previous label's divisions are its prefix.
     */
    static SirixDeweyID nextAppendLabel(final SirixDeweyID previous) {
      final int[] divisions = previous.getDivisionValues();
      final int lastDivision = divisions[divisions.length - 1];
      final SirixDeweyID candidate = SirixDeweyID.newBetween(previous, null);
      final int[] candidateDivisions = candidate.getDivisionValues();
      if (candidateDivisions.length == divisions.length
          && candidateDivisions[candidateDivisions.length - 1] > lastDivision) {
        return candidate;
      }
      final int[] carried = Arrays.copyOf(divisions, divisions.length + 1);
      carried[divisions.length] = APPEND_CARRY_DIVISION;
      if (carried.length > MAX_FULL_LABEL_DIVISIONS) {
        throw new IllegalStateException(
            "projection structural-order append sequence exhausted its division budget");
      }
      return new SirixDeweyID(carried, previous.getLevel() + 1);
    }

    private static SirixDeweyID firstLocalLabel() {
      return SirixDeweyID.newRootID().getNewChildID(FIRST_LOCAL_DIVISION);
    }

    private static boolean containsInvalidLocalDivision(final int[] divisions) {
      for (int index = 1; index < divisions.length; index++) {
        if (divisions[index] < 2) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * {@code count} strictly increasing level-1 labels, evenly spread strictly between {@code left}
   * and {@code right} (either may be {@code null} for an open end), with a reserved gap left at BOTH
   * ends so the next few insertions at either edge stay short.
   *
   * <p>
   * The interval arithmetic is resolved ONCE, in {@link #between}, and each label is generated on
   * demand by {@link #at}. A run of consecutive unlabelled siblings may be arbitrarily long, so
   * materialising one array per member would be a humongous allocation; a {@code Spread} is O(1) in
   * the run's length.
   *
   * <p>
   * The spread stays at the shortest division count that fits: only when the interval at the current
   * division is too tight does it descend one division — through an EVEN separator, which keeps the
   * "exactly one odd division after the leading 1" shape that makes concatenated ancestor suffixes
   * prefix-free — and re-spread there against an open end.
   */
  private record Spread(int[] prefix, long firstOdd, long available, long count) {

    static Spread between(final int @Nullable [] left, final int @Nullable [] right, final long count) {
      if (count <= 0L) {
        throw new IllegalArgumentException("a structural-order spread needs at least one label");
      }
      int[] prefix = { 1 };
      int[] lower = left;
      int[] upper = right;
      int index = 1;
      for (int descent = 0; descent <= MAX_SPREAD_DESCENTS; descent++) {
        while (lower != null && upper != null && index < lower.length && index < upper.length
            && lower[index] == upper[index]) {
          prefix = appendDivision(prefix, requireSeparatorDivision(lower[index]));
          index++;
        }
        if (upper != null && upper.length == index) {
          throw new IllegalStateException("projection structural-order bounds are not strictly ordered");
        }
        final long low = lower != null && lower.length > index ? lower[index] : 1L;
        final long high = upper != null && upper.length > index ? upper[index] : -1L;
        if (high >= 0L && high <= low) {
          throw new IllegalStateException("projection structural-order bounds are not strictly ordered");
        }

        long firstOdd = low + 1L;
        if ((firstOdd & 1L) == 0L) {
          firstOdd++;
        }
        long lastOdd = high < 0L
            ? Math.min(Integer.MAX_VALUE - 1L, firstOdd + 2L * SPREAD_SLACK * (count + 2L))
            : high - 1L;
        if ((lastOdd & 1L) == 0L) {
          lastOdd--;
        }
        final long available = lastOdd >= firstOdd ? (lastOdd - firstOdd) / 2L + 1L : 0L;
        if (available >= count + 2L) {
          return new Spread(prefix, firstOdd, available, count);
        }

        long separator = low + 1L;
        if ((separator & 1L) == 1L) {
          separator++;
        }
        if (separator >= 2L && (high < 0L || separator < high)) {
          prefix = appendDivision(prefix, (int) separator);
          index = prefix.length;
          lower = null;
          upper = null;
          continue;
        }
        if (lower != null && lower.length > index + 1) {
          prefix = appendDivision(prefix, requireSeparatorDivision(lower[index]));
          index++;
          upper = null;
          continue;
        }
        if (upper != null && upper.length > index + 1) {
          prefix = appendDivision(prefix, requireSeparatorDivision(upper[index]));
          index++;
          lower = null;
          continue;
        }
        throw new IllegalStateException("projection structural-order interval admits no bounded label");
      }
      throw new IllegalStateException(
          "projection structural-order spread exceeded " + MAX_SPREAD_DESCENTS + " descents");
    }

    int[] at(final long item) {
      if (item < 0L || item >= count) {
        throw new IndexOutOfBoundsException("structural-order spread item " + item + " of " + count);
      }
      final long slot = (item + 1L) * (available - 1L) / (count + 1L);
      return appendDivision(prefix, (int) (firstOdd + 2L * slot));
    }
  }

  private static int[][] spreadLocalLabels(final int @Nullable [] left, final int @Nullable [] right,
      final int count) {
    final Spread spread = Spread.between(left, right, count);
    final int[][] labels = new int[count][];
    for (int item = 0; item < count; item++) {
      labels[item] = spread.at(item);
    }
    return labels;
  }

  /**
   * A division a label may be extended through. ORDPATH reserves EVEN divisions for exactly that —
   * only the final, odd division names a node — which is what keeps the concatenated ancestor
   * suffixes prefix-free.
   */
  private static int requireSeparatorDivision(final int division) {
    if (division < 2 || (division & 1) != 0) {
      throw new IllegalStateException(
          "projection structural-order label cannot be extended through division " + division);
    }
    return division;
  }

  private static int[] appendDivision(final int[] divisions, final int division) {
    final int[] extended = Arrays.copyOf(divisions, divisions.length + 1);
    extended[divisions.length] = division;
    return extended;
  }
}
