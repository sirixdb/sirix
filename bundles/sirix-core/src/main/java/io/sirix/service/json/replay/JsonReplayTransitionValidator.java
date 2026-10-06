package io.sirix.service.json.replay;

import io.sirix.utils.ReplayWorkDiagnostics;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.node.NodeKind;
import io.sirix.index.IndexType;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.StructNode;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * Validate an exact transition from the already validated base graph. Every changed record and both
 * its old and new boundaries participate. Parent chains prove ancestry; affected sibling lists
 * prove reachability, order and counts. A pure append retains the base's proven prefix and checks
 * only its fresh suffix. General permutations inspect their affected sibling lists, never unchanged
 * subtrees. Complete change discovery belongs to the package-private authoritative delta factories.
 */
public final class JsonReplayTransitionValidator {
  private record Shape(long key, NodeKind kind, long parent, long left, long right, long first, long last,
      long children, long descendants, @Nullable SirixDeweyID dewey) {
    static Shape of(final StructNode node) {
      return new Shape(node.getNodeKey(), node.getKind(), node.getParentKey(), node.getLeftSiblingKey(),
          node.getRightSiblingKey(), node.getFirstChildKey(), node.getLastChildKey(), node.getChildCount(),
          node.getDescendantCount(), node.getDeweyID());
    }
  }

  private final JsonNodeReadOnlyTrx target;
  private final JsonNodeReadOnlyTrx base;
  private final JsonIdentityDelta delta;
  private final Long2ObjectOpenHashMap<Shape> oldShapes = new Long2ObjectOpenHashMap<>();
  private final Long2ObjectOpenHashMap<Shape> shapes = new Long2ObjectOpenHashMap<>();
  private final LongSet boundaries = new LongOpenHashSet();
  private final LongSet parents = new LongOpenHashSet();
  private final LongSet nonAppendParents = new LongOpenHashSet();
  private final Long2LongOpenHashMap childChanges = new Long2LongOpenHashMap();
  private final Long2LongOpenHashMap descendantChanges = new Long2LongOpenHashMap();
  private final Long2ObjectOpenHashMap<LongSet> requiredChildren = new Long2ObjectOpenHashMap<>();
  private final boolean childCounts;
  private final boolean descendantCounts;
  private final boolean deweyIDs;

  private JsonReplayTransitionValidator(final JsonNodeReadOnlyTrx target, final JsonNodeReadOnlyTrx base,
      final JsonIdentityDelta delta) {
    this.target = target;
    this.base = base;
    this.delta = delta;
    final var config = target.getResourceSession().getResourceConfig();
    childCounts = config.storeChildCount();
    descendantCounts = config.hashType != HashType.NONE;
    deweyIDs = config.areDeweyIDsStored;
  }

  public static void validate(final JsonNodeReadOnlyTrx target, final JsonIdentityDelta delta) {
    Objects.requireNonNull(target);
    Objects.requireNonNull(delta);
    if (target.getMaxNodeKey() != delta.manifest().targetFrontier()) {
      throw new IllegalStateException("Replay allocation frontier differs from its manifest");
    }
    for (final JsonReplayRecord expected : delta.puts().values()) {
      if (!target.moveTo(expected.key()) || !matches(expected, JsonReplayRecord.capture(target), delta.manifest())) {
        throw new IllegalStateException("Replay record differs from its target payload: " + expected.key());
      }
    }
    if (delta.manifest().baseRevision() == 0) {
      JsonReplayGraphValidator.validate(target, delta.puts().keySet());
      return;
    }
    if (delta.puts().isEmpty() && delta.deletes().isEmpty()) {
      return;
    }
    try (final JsonNodeReadOnlyTrx base =
        target.getResourceSession().beginNodeReadOnlyTrx(delta.manifest().destinationRevision() - 1)) {
      new JsonReplayTransitionValidator(target, base, delta).validate();
    }
  }

  private void validate() {
    for (final var keys = delta.deletes().iterator(); keys.hasNext();) {
      final long key = keys.nextLong();
      final Shape old = old(key);
      if (old == null || shape(key) != null) {
        throw new IllegalStateException("Replay deletion does not remove a base identity: " + key);
      }
      observe(old, null);
    }
    for (final var keys = delta.puts().keySet().iterator(); keys.hasNext();) {
      final long key = keys.nextLong();
      observe(old(key), require(key));
    }
    boundaries.add(0);
    // Validate old boundaries too: otherwise an untouched neighbour could still reference a removed
    // identity, or a disconnected unchanged prefix could escape final-chain traversal.
    for (final var keys = boundaries.iterator(); keys.hasNext();) {
      final Shape node = shape(keys.nextLong());
      if (node != null) {
        validateBoundary(node);
      }
    }
    validateAncestry();
    for (final var keys = parents.iterator(); keys.hasNext();) {
      final long key = keys.nextLong();
      final Shape current = shape(key);
      if (current == null) {
        continue;
      }
      final Shape prior = old(key);
      if ((childCounts && current.children() != Math.addExact(prior == null
          ? 0
          : prior.children(), childChanges.get(key)))
          || (descendantCounts && current.descendants() != Math.addExact(prior == null
              ? 0
              : prior.descendants(), descendantChanges.get(key)))) {
        throw new IllegalStateException("Replay child contributions disagree at " + key);
      }
      final boolean append = !nonAppendParents.contains(key) && (prior == null
          || (prior.kind() == current.kind() && (prior.first() == -1 || prior.first() == current.first())));
      validateChildren(current, prior, append);
    }
  }

  // Every non-root old parent exists in the already validated base graph.
  @SuppressWarnings("NullAway")
  private void observe(final @Nullable Shape prior, final @Nullable Shape current) {
    addBoundaries(prior);
    addBoundaries(current);
    if (prior != null && prior.parent() >= 0) {
      childChanges.addTo(prior.parent(), -1);
      descendantChanges.addTo(prior.parent(), -Math.addExact(prior.descendants(), 1));
    }
    if (current != null && current.parent() >= 0) {
      childChanges.addTo(current.parent(), 1);
      descendantChanges.addTo(current.parent(), Math.addExact(current.descendants(), 1));
      requiredChildren.computeIfAbsent(current.parent(), ignored -> new LongOpenHashSet()).add(current.key());
    }
    if (descendantCounts && prior != null && current != null && prior.descendants() != current.descendants()) {
      if (prior.parent() >= 0) {
        parents.add(prior.parent());
      }
      if (current.parent() >= 0) {
        parents.add(current.parent());
      }
    }
    if (prior == null || current == null || prior.parent() != current.parent() || prior.left() != current.left()
        || prior.right() != current.right()) {
      if (prior != null && prior.parent() >= 0) {
        parents.add(prior.parent());
        if (current == null || prior.parent() != current.parent() || prior.left() != current.left()
            || old(prior.parent()).last() != prior.key()) {
          nonAppendParents.add(prior.parent());
        }
      }
      if (current != null && current.parent() >= 0) {
        parents.add(current.parent());
        if (prior != null && prior.parent() != current.parent()) {
          nonAppendParents.add(current.parent());
        }
      }
    }
    if (current != null && (prior == null || prior.kind() != current.kind() || prior.first() != current.first()
        || prior.last() != current.last() || prior.children() != current.children()
        || prior.descendants() != current.descendants())) {
      parents.add(current.key());
    }
  }

  private void addBoundaries(final @Nullable Shape node) {
    if (node != null) {
      boundaries.add(node.key());
      addBoundary(node.parent());
      addBoundary(node.left());
      addBoundary(node.right());
      addBoundary(node.first());
      addBoundary(node.last());
    }
  }

  private void addBoundary(final long key) {
    if (key >= 0) {
      boundaries.add(key);
    }
  }

  private void validateBoundary(final Shape node) {
    if ((node.key() == 0) != (node.kind() == NodeKind.JSON_DOCUMENT) || (node.key() == 0
        && (node.parent() != -1 || node.left() != -1 || node.right() != -1 || node.first() != node.last()))) {
      throw new IllegalStateException("Invalid replay document root");
    }
    if (node.key() != 0) {
      final Shape parent = require(node.parent());
      if (!JsonReplayGraphValidator.compatible(parent.kind(), node.kind())) {
        throw new IllegalStateException("Incompatible replay parent at " + node.key());
      }
      if (node.left() == -1
          ? parent.first() != node.key()
          : require(node.left()).right() != node.key() || require(node.left()).parent() != node.parent()) {
        throw new IllegalStateException("Invalid replay left boundary at " + node.key());
      }
      if (node.right() == -1
          ? parent.last() != node.key()
          : require(node.right()).left() != node.key() || require(node.right()).parent() != node.parent()) {
        throw new IllegalStateException("Invalid replay right boundary at " + node.key());
      }
      if (deweyIDs && (node.dewey() == null || parent.dewey() == null || !node.dewey().isDescendantOf(parent.dewey())
          || (node.left() != -1 && require(node.left()).dewey().compareTo(node.dewey()) >= 0))) {
        throw new IllegalStateException("Invalid replay Dewey boundary at " + node.key());
      }
    }
    if ((node.first() == -1) != (node.last() == -1)
        || (node.first() >= 0 && (require(node.first()).parent() != node.key() || require(node.first()).left() != -1
            || require(node.last()).parent() != node.key() || require(node.last()).right() != -1))) {
      throw new IllegalStateException("Invalid replay child boundary at " + node.key());
    }
  }

  private void validateAncestry() {
    final LongSet rooted = new LongOpenHashSet();
    rooted.add(0);
    final LongSet active = new LongOpenHashSet();
    final LongArrayList chain = new LongArrayList();
    for (final var keys = delta.puts().keySet().iterator(); keys.hasNext();) {
      long key = keys.nextLong();
      while (!rooted.contains(key)) {
        if (!active.add(key)) {
          throw new IllegalStateException("Replay parent cycle at " + key);
        }
        ReplayWorkDiagnostics.ancestorStep();
        chain.add(key);
        key = require(key).parent();
      }
      while (!chain.isEmpty()) {
        final long rootedKey = chain.removeLong(chain.size() - 1);
        active.remove(rootedKey);
        rooted.add(rootedKey);
      }
    }
  }

  private void validateChildren(final Shape parent, final @Nullable Shape prior, final boolean append) {
    long previous = append && prior != null
        ? prior.last()
        : -1;
    long key = previous >= 0
        ? require(previous).right()
        : parent.first();
    long count = append && prior != null
        ? prior.children()
        : 0;
    long descendants = 0;
    final LongSet visited = new LongOpenHashSet();
    while (key >= 0) {
      if (!visited.add(key) || (append && old(key) != null)) {
        throw new IllegalStateException("Replay child cycle or non-new append suffix at " + key);
      }
      final Shape child = require(key);
      if (child.parent() != parent.key() || child.left() != previous
          || !JsonReplayGraphValidator.compatible(parent.kind(), child.kind())) {
        throw new IllegalStateException("Invalid replay child chain at " + key);
      }
      previous = key;
      key = child.right();
      count++;
      descendants = Math.addExact(descendants, Math.addExact(child.descendants(), 1));
    }
    if (parent.last() != previous || (childCounts && parent.children() != count)
        || (!append && descendantCounts && parent.descendants() != descendants)) {
      throw new IllegalStateException("Replay child chain counts disagree at " + parent.key());
    }
    final LongSet required = requiredChildren.get(parent.key());
    if (required != null) {
      for (final var keys = required.iterator(); keys.hasNext();) {
        final long requiredKey = keys.nextLong();
        if (!visited.contains(requiredKey)) {
          final Shape oldChild = append
              ? old(requiredKey)
              : null;
          if (oldChild == null || oldChild.parent() != parent.key()) {
            throw new IllegalStateException("Unreachable changed replay child " + requiredKey);
          }
        }
      }
    }
  }

  private @Nullable Shape old(final long key) {
    return read(base.getStorageEngineReader(), oldShapes, key);
  }

  private @Nullable Shape shape(final long key) {
    return read(target.getStorageEngineReader(), shapes, key);
  }

  private Shape require(final long key) {
    final Shape node = key < 0 || key > delta.manifest().targetFrontier()
        ? null
        : shape(key);
    if (node == null) {
      throw new IllegalStateException("Missing referenced replay identity " + key);
    }
    return node;
  }

  private static @Nullable Shape read(final StorageEngineReader reader, final Long2ObjectOpenHashMap<Shape> cache,
      final long key) {
    if (key < 0) {
      return null;
    }
    if (!cache.containsKey(key)) {
      final var record = reader.getRecord(key, IndexType.DOCUMENT, -1);
      cache.put(key, record instanceof final StructNode node
          ? Shape.of(node)
          : null);
    }
    return cache.get(key);
  }

  private static boolean matches(final JsonReplayRecord expected, final JsonReplayRecord actual,
      final JsonReplayManifest manifest) {
    return expected.key() == actual.key() && expected.kind() == actual.kind() && expected.parent() == actual.parent()
        && expected.left() == actual.left() && expected.right() == actual.right()
        && expected.firstChild() == actual.firstChild() && expected.lastChild() == actual.lastChild()
        && expected.childCount() == actual.childCount() && expected.descendantCount() == actual.descendantCount()
        && expected.pathKey() == actual.pathKey() && expected.nameKey() == actual.nameKey()
        && expected.hash() == actual.hash() && Objects.equals(expected.deweyID(), actual.deweyID())
        && Objects.equals(expected.name(), actual.name())
        && Objects.equals(expected.stringValue(), actual.stringValue())
        && Objects.equals(expected.numberValue(), actual.numberValue())
        && expected.booleanValue() == actual.booleanValue()
        && (expected.key() == 0
            || (manifest.mapPreviousRevision(expected.previousRevision()) == actual.previousRevision()
                && manifest.mapLastModifiedRevision(expected.lastModifiedRevision()) == actual.lastModifiedRevision()));
  }
}
