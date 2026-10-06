package io.sirix.service.json.replay;

import io.brackit.query.atomic.QNm;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.index.IndexType;
import io.sirix.index.path.summary.PathNode;
import io.sirix.index.path.summary.PathStats;
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.json.JsonDocumentRootNode;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.Objects;

/** Imports changed logical path records from paired immutable tries, including exact statistics. */
public final class JsonReplayPaths {
  private JsonReplayPaths() {}

  /** A null record denotes a removed path. Logical roots invalidate descendant path expressions. */
  public record Changes(Long2ObjectMap<StructNode> records, LongSet logicalRoots) {
    public boolean namespaceChanged() {
      return !logicalRoots.isEmpty();
    }
  }

  public static Changes importChanges(final JsonNodeReadOnlyTrx source, final StorageEngineWriter target,
      final JsonReplayManifest manifest) {
    final var changed = new Long2ObjectOpenHashMap<StructNode>();
    final LongSet logicalRoots = new LongOpenHashSet();
    final var result = new Changes(changed, logicalRoots);
    final var config = target.getResourceSession().getResourceConfig();
    if (!config.withPathSummary) {
      return result;
    }
    if (!source.getResourceSession().getResourceConfig().withPathSummary) {
      throw new IllegalArgumentException("Replay requires matching path-summary configuration");
    }
    try (final var base = source.getResourceSession().beginNodeReadOnlyTrx(manifest.baseRevision());
        final var buffer = Bytes.elasticHeapByteBuffer()) {
      final var before = base.getStorageEngineReader();
      final var after = source.getStorageEngineReader();
      final var oldPage = before.getPathSummaryPage(before.getActualRevisionRootPage());
      final var newPage = after.getPathSummaryPage(after.getActualRevisionRootPage());
      new JsonReplayPageWalk(before, after, IndexType.PATH_SUMMARY, 0, (key, oldExists, newExists) -> {
        final StructNode old = oldExists
            ? pathRecord(before, key)
            : null;
        final StructNode current = newExists
            ? pathRecord(after, key)
            : null;
        final QNm oldName = old instanceof final PathNode path
            ? name(before, path)
            : null;
        final QNm newName = current instanceof final PathNode path
            ? name(after, path)
            : null;
        final byte[] oldStats = statistics(old, buffer);
        final byte[] newStats = statistics(current, buffer);
        if (same(old, current) && Objects.equals(oldName, newName) && Arrays.equals(oldStats, newStats)) {
          return;
        }
        if (key != 0 && (old == null || current == null || old.getParentKey() != current.getParentKey()
            || !Objects.equals(oldName, newName)
            || (old instanceof final PathNode a && current instanceof final PathNode b
                && (a.getPathKind() != b.getPathKind() || a.getLevel() != b.getLevel())))) {
          logicalRoots.add(key);
        }
        if (current == null) {
          if (old != null) {
            target.removeRecord(key, IndexType.PATH_SUMMARY, 0);
            changed.put(key, null);
          }
          return;
        }
        final StructNode copy;
        if (current instanceof final PathNode path) {
          // The same PathNode branch above computes a non-null newName.
          @SuppressWarnings("NullAway")
          final var pathCopy = new PathNode(newName, path.getPathKind(), path.getReferences(), path.getLevel(), key,
              path.getParentKey(), manifest.mapPreviousRevision(path.getPreviousRevisionNumber()),
              manifest.mapLastModifiedRevision(path.getLastModifiedRevisionNumber()), (SirixDeweyID) null,
              path.getFirstChildKey(), path.getLastChildKey(), path.getRightSiblingKey(), path.getLeftSiblingKey(),
              path.getChildCount(), path.getDescendantCount(), path.getURIKey(), path.getPrefixKey(),
              path.getLocalNameKey(), path.getPathNodeKey());
          if (newStats != null) {
            pathCopy.setStats(PathStats.readFrom(new ByteArrayBytesIn(newStats)));
          }
          copy = pathCopy;
        } else if (key == 0) {
          copy = new JsonDocumentRootNode(0, current.getFirstChildKey(), current.getLastChildKey(),
              current.getChildCount(), current.getDescendantCount(), config.nodeHashFunction);
        } else {
          throw new IllegalStateException("Invalid path summary record " + key);
        }
        target.persistRecord(copy, IndexType.PATH_SUMMARY, 0);
        changed.put(key, copy);
      }).read(oldPage.getIndirectPageReference(0), oldPage.getCurrentMaxLevelOfIndirectPages(0),
          newPage.getIndirectPageReference(0), newPage.getCurrentMaxLevelOfIndirectPages(0));
      target.getPathSummaryPage(target.getActualRevisionRootPage()).setMaxNodeKey(0, newPage.getMaxNodeKey(0));
    }
    return result;
  }

  private static @Nullable StructNode pathRecord(final StorageEngineReader reader, final long key) {
    final DataRecord record = reader.getRecord(key, IndexType.PATH_SUMMARY, 0);
    if (record == null) {
      return null;
    }
    if (!(record instanceof StructNode node)) {
      throw new IllegalStateException("Invalid path record " + key);
    }
    return node;
  }

  private static QNm name(final StorageEngineReader reader, final PathNode path) {
    return new QNm("", path.getPrefixKey() == -1
        ? ""
        : reader.getName(path.getPrefixKey(), path.getPathKind()),
        path.getLocalNameKey() == -1
            ? ""
            : reader.getName(path.getLocalNameKey(), path.getPathKind()));
  }

  private static byte @Nullable [] statistics(final @Nullable StructNode node, final BytesOut<?> buffer) {
    if (!(node instanceof final PathNode path) || path.getStats() == null) {
      return null;
    }
    buffer.clear();
    path.getStats().writeTo(buffer);
    return buffer.toByteArray();
  }

  private static boolean same(final @Nullable StructNode left, final @Nullable StructNode right) {
    if (left == null || right == null) {
      return left == null && right == null;
    }
    if (left.getKind() != right.getKind() || left.getParentKey() != right.getParentKey()
        || left.getFirstChildKey() != right.getFirstChildKey() || left.getLastChildKey() != right.getLastChildKey()
        || left.getLeftSiblingKey() != right.getLeftSiblingKey()
        || left.getRightSiblingKey() != right.getRightSiblingKey() || left.getChildCount() != right.getChildCount()
        || left.getDescendantCount() != right.getDescendantCount()
        || left.getPreviousRevisionNumber() != right.getPreviousRevisionNumber()
        || left.getLastModifiedRevisionNumber() != right.getLastModifiedRevisionNumber()) {
      return false;
    }
    return !(left instanceof final PathNode a) || (right instanceof final PathNode b
        && a.getPathKind() == b.getPathKind() && a.getReferences() == b.getReferences() && a.getLevel() == b.getLevel()
        && a.getURIKey() == b.getURIKey() && a.getPrefixKey() == b.getPrefixKey()
        && a.getLocalNameKey() == b.getLocalNameKey() && a.getPathNodeKey() == b.getPathNodeKey());
  }
}
