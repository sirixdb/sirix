package io.sirix.service.json.replay;

import io.sirix.access.trx.node.HashType;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.index.IndexType;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.StructNode;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongSets;

/** Full graph validation used while bringing up the importer and by its snapshot shadow oracle. */
public final class JsonReplayGraphValidator {
  private static final int WIDTH = 5;

  private JsonReplayGraphValidator() {}

  public static void validate(final JsonNodeReadOnlyTrx transaction) {
    validate(transaction, LongSets.emptySet());
  }

  public static void validate(final JsonNodeReadOnlyTrx transaction, final LongSet requiredIdentities) {
    final StorageEngineReader reader = transaction.getStorageEngineReader();
    final var config = transaction.getResourceSession().getResourceConfig();
    final LongOpenHashSet visited = new LongOpenHashSet();
    final LongArrayList frames = new LongArrayList(64 * WIDTH);
    push(reader, frames, visited, 0, transaction.getMaxNodeKey());
    while (!frames.isEmpty()) {
      final int offset = frames.size() - WIDTH;
      final long key = frames.getLong(offset);
      final long childKey = frames.getLong(offset + 1);
      final StructNode node = requireRecord(reader, key);
      if (childKey == -1) {
        final long children = frames.getLong(offset + 3);
        final long descendants = frames.getLong(offset + 4);
        if (node.getLastChildKey() != frames.getLong(offset + 2)
            || (config.storeChildCount() && node.getChildCount() != children)
            || (config.hashType != HashType.NONE && node.getDescendantCount() != descendants)) {
          throw new IllegalStateException("Replay child links or counts disagree at " + key + ": last="
              + node.getLastChildKey() + "/" + frames.getLong(offset + 2) + ", children=" + node.getChildCount() + "/"
              + children + ", descendants=" + node.getDescendantCount() + "/" + descendants);
        }
        if (key == 0 && (node.getKind() != NodeKind.JSON_DOCUMENT || children > 1 || node.getParentKey() != -1
            || node.getLeftSiblingKey() != -1 || node.getRightSiblingKey() != -1)) {
          throw new IllegalStateException("Invalid replay document root");
        }
        frames.size(offset);
        if (!frames.isEmpty()) {
          final int parentOffset = frames.size() - WIDTH;
          frames.set(parentOffset + 4, Math.addExact(frames.getLong(parentOffset + 4), descendants + 1));
        }
        continue;
      }
      final StructNode child = requireRecord(reader, childKey);
      if (child.getParentKey() != key || child.getLeftSiblingKey() != frames.getLong(offset + 2)
          || !compatible(node.getKind(), child.getKind())) {
        throw new IllegalStateException("Replay parent or ordered sibling links disagree at " + childKey);
      }
      if (config.areDeweyIDsStored) {
        if (node.getDeweyID() == null || child.getDeweyID() == null
            || !child.getDeweyID().isDescendantOf(node.getDeweyID())) {
          throw new IllegalStateException("Invalid replay Dewey ancestry at " + childKey);
        }
        final long left = frames.getLong(offset + 2);
        if (left != -1 && requireRecord(reader, left).getDeweyID().compareTo(child.getDeweyID()) >= 0) {
          throw new IllegalStateException("Invalid replay Dewey sibling order at " + childKey);
        }
      }
      frames.set(offset + 1, child.getRightSiblingKey());
      frames.set(offset + 2, childKey);
      frames.set(offset + 3, frames.getLong(offset + 3) + 1);
      push(reader, frames, visited, childKey, transaction.getMaxNodeKey());
    }
    for (final long key : requiredIdentities) {
      if (!visited.contains(key)) {
        throw new IllegalStateException("Unreachable staged replay identity " + key);
      }
    }
  }

  public static boolean compatible(final NodeKind parent, final NodeKind child) {
    return switch (parent) {
      case OBJECT, OBJECT_NAMED_OBJECT -> child.playsObjectKeyRole();
      case ARRAY, OBJECT_NAMED_ARRAY, JSON_DOCUMENT -> !child.playsObjectKeyRole() && child != NodeKind.JSON_DOCUMENT;
      default -> false;
    };
  }

  private static void push(final StorageEngineReader reader, final LongArrayList frames, final LongOpenHashSet visited,
      final long key, final long frontier) {
    if (key < 0 || key > frontier || !visited.add(key)) {
      throw new IllegalStateException("Cycle, duplicate or out-of-frontier replay identity " + key);
    }
    final StructNode record = requireRecord(reader, key);
    frames.add(key);
    frames.add(record.getFirstChildKey());
    frames.add(-1);
    frames.add(0);
    frames.add(0);
  }

  private static StructNode requireRecord(final StorageEngineReader reader, final long key) {
    final var record = reader.getRecord(key, IndexType.DOCUMENT, -1);
    if (!(record instanceof StructNode node) || record.getKind() == NodeKind.DELETE) {
      throw new IllegalStateException("Missing referenced replay identity " + key);
    }
    return node;
  }
}
