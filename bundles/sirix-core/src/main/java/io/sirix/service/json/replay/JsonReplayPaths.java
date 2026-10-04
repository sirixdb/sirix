package io.sirix.service.json.replay;

import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.IncludeSelf;
import io.sirix.index.IndexType;
import io.sirix.index.path.summary.PathNode;
import io.sirix.index.path.summary.PathStats;
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.Bytes;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.json.JsonDocumentRootNode;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

/**
 * Rebuilds the complete logical path namespace for the initial snapshot import. Subsequent epochs
 * must feed changed path records only. No source page, dictionary buffer or mutable statistic is shared.
 */
public final class JsonReplayPaths {
  private JsonReplayPaths() {
  }

  public static void rebuild(final JsonNodeReadOnlyTrx source, final StorageEngineWriter target,
      final JsonReplayManifest manifest) {
    final var config = target.getResourceSession().getResourceConfig();
    if (!config.withPathSummary) {
      return;
    }
    if (!source.getResourceSession().getResourceConfig().withPathSummary) {
      throw new IllegalArgumentException("Replay requires matching path-summary configuration");
    }
    final var oldKeys = collect(target);
    try (final var paths = source.getResourceSession().openPathSummary(source.getRevisionNumber());
        final var buffer = Bytes.elasticHeapByteBuffer()) {
      final var axis = new DescendantAxis(paths, IncludeSelf.YES);
      while (axis.hasNext()) {
        final long key = axis.nextLong();
        oldKeys.remove(key);
        final StructNode original = paths.getStorageEngineReader().getRecord(key, IndexType.PATH_SUMMARY, 0);
        final StructNode copy;
        if (original instanceof PathNode path) {
          final var pathCopy = new PathNode(paths.getName(), path.getPathKind(), path.getReferences(),
              path.getLevel(), key, path.getParentKey(), manifest.mapPreviousRevision(path.getPreviousRevisionNumber()),
              manifest.mapLastModifiedRevision(path.getLastModifiedRevisionNumber()), (SirixDeweyID) null,
              path.getFirstChildKey(), path.getLastChildKey(), path.getRightSiblingKey(), path.getLeftSiblingKey(),
              path.getChildCount(), path.getDescendantCount(), path.getURIKey(), path.getPrefixKey(),
              path.getLocalNameKey(), path.getPathNodeKey());
          final PathStats stats = path.getStats();
          if (stats != null) {
            buffer.clear();
            stats.writeTo(buffer);
            pathCopy.setStats(PathStats.readFrom(new ByteArrayBytesIn(buffer.toByteArray())));
          }
          copy = pathCopy;
        } else if (key == 0) {
          copy = new JsonDocumentRootNode(0, original.getFirstChildKey(), original.getLastChildKey(),
              original.getChildCount(), original.getDescendantCount(), config.nodeHashFunction);
        } else {
          throw new IllegalStateException("Invalid path summary record " + key);
        }
        target.persistRecord(copy, IndexType.PATH_SUMMARY, 0);
      }
      for (final long key : oldKeys) {
        target.removeRecord(key, IndexType.PATH_SUMMARY, 0);
      }
      final var sourcePage = paths.getStorageEngineReader()
          .getPathSummaryPage(paths.getStorageEngineReader().getActualRevisionRootPage());
      target.getPathSummaryPage(target.getActualRevisionRootPage()).setMaxNodeKey(0, sourcePage.getMaxNodeKey(0));
    }
  }

  private static LongOpenHashSet collect(final StorageEngineReader reader) {
    final var records = new LongOpenHashSet();
    final var pending = new LongArrayList();
    pending.add(0);
    while (!pending.isEmpty()) {
      final long key = pending.removeLong(pending.size() - 1);
      if (!records.add(key)) {
        throw new IllegalStateException("Cyclic path summary");
      }
      final StructNode node = reader.getRecord(key, IndexType.PATH_SUMMARY, 0);
      if (node == null) {
        throw new IllegalStateException("Missing path summary identity " + key);
      }
      final long right = node.getRightSiblingKey();
      final long first = node.getFirstChildKey();
      if (right != -1) {
        pending.add(right);
      }
      if (first != -1) {
        pending.add(first);
      }
    }
    return records;
  }
}
