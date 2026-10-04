package io.sirix.diff;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.brackit.query.atomic.QNm;
import io.brackit.query.util.path.Path;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.node.NodeKind;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.settings.Fixed;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongSets;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;

public final class JsonDiffSerializer {

  /** Observes the actual backing storage once per revision cache, outside the traversal loop. */
  private static volatile @Nullable BiConsumer<@Nullable Long2IntOpenHashMap, LongArrayList> arrayPositionCacheObserver;

  static @Nullable BiConsumer<@Nullable Long2IntOpenHashMap, LongArrayList> setArrayPositionCacheObserverForTesting(
      final @Nullable BiConsumer<@Nullable Long2IntOpenHashMap, LongArrayList> observer) {
    final var previous = arrayPositionCacheObserver;
    arrayPositionCacheObserver = observer;
    return previous;
  }

  private final String databaseName;
  private final JsonResourceSession resourceSession;
  private final int oldRevisionNumber;
  private final int newRevisionNumber;
  private final Collection<DiffTuple> diffs;

  public JsonDiffSerializer(final String databaseName, JsonResourceSession resourceSession, int oldRevisionNumber,
      int newRevisionNumber, Collection<DiffTuple> diffs) {
    this.databaseName = databaseName;
    this.resourceSession = resourceSession;
    this.oldRevisionNumber = oldRevisionNumber;
    this.newRevisionNumber = newRevisionNumber;
    this.diffs = diffs;
  }

  public String serialize(boolean emitFromDiffAlgorithm) {
    return serialize(emitFromDiffAlgorithm, false, null);
  }

  /** Serialize the compact internal per-revision sidecar, including integrity metadata. */
  public String serializeSidecar() {
    return serialize(false, true, null);
  }

  /**
   * Serializes with transient ordinals captured while ingesting the new revision. The caller must
   * discard hints after any edit that can shift them. They must describe {@code newRevisionNumber},
   * remain unchanged during this call, and are never used for the old revision or retained here.
   */
  public String serializeSidecar(final Long2IntMap knownNewArrayPositions) {
    return serialize(false, true, Objects.requireNonNull(knownNewArrayPositions));
  }

  private String serialize(final boolean emitFromDiffAlgorithm, final boolean includeIntegrityMetadata,
      final @Nullable Long2IntMap knownNewArrayPositions) {
    final var resourceName = resourceSession.getResourceConfig().getName();

    final JsonObject json = createMetaInfo(databaseName, resourceName, oldRevisionNumber, newRevisionNumber);

    if (diffs.size() == 1) {
      final var tuple = diffs.iterator().next();
      if (tuple.getDiff() == DiffFactory.DiffType.SAME || tuple.getDiff() == DiffFactory.DiffType.SAMEHASH) {
        return finish(json, includeIntegrityMetadata);
      }
    }

    if (emitFromDiffAlgorithm) {
      diffs.removeIf(diffTuple -> diffTuple.getDiff() == DiffFactory.DiffType.SAME
          || diffTuple.getDiff() == DiffFactory.DiffType.SAMEHASH
          || diffTuple.getDiff() == DiffFactory.DiffType.REPLACEDOLD);
    }

    if (diffs.isEmpty()) {
      return finish(json, includeIntegrityMetadata);
    }

    final var jsonDiffs = json.getAsJsonArray("diffs");

    try (final var oldRtx = resourceSession.beginNodeReadOnlyTrx(oldRevisionNumber);
        final var newRtx = resourceSession.beginNodeReadOnlyTrx(newRevisionNumber)) {
      final var oldArrayPositions = new ArrayPositionCache(null);
      final var newArrayPositions = new ArrayPositionCache(knownNewArrayPositions);

      for (final var diffTuple : diffs) {
        final var diffType = diffTuple.getDiff();

        // A tuple whose node key does not resolve in its revision is stale (e.g. recorded for a
        // node that was later removed, or inserted and removed within the same transaction).
        // Serializing it would read from an unpositioned cursor — skip it instead.
        if (diffType == DiffFactory.DiffType.INSERTED) {
          if (!newRtx.moveTo(diffTuple.getNewNodeKey())) {
            continue;
          }
        } else if (diffType == DiffFactory.DiffType.DELETED) {
          if (!oldRtx.moveTo(diffTuple.getOldNodeKey())) {
            continue;
          }
        } else {
          if (!newRtx.moveTo(diffTuple.getNewNodeKey()) || !oldRtx.moveTo(diffTuple.getOldNodeKey())) {
            continue;
          }
        }

        switch (diffType) {
          case INSERTED:
            final var insertedJson = new JsonObject();
            final var jsonInsertDiff = new JsonObject();

            insertBasedOnNewRtx(newRtx, jsonInsertDiff);

            // Add path using PathSummary (always available by default)
            // Pass true to include parent path for value nodes (STRING_VALUE, etc.)
            addPathIfAvailable(jsonInsertDiff, newRtx, newRevisionNumber, true, newArrayPositions);

            if (resourceSession.getResourceConfig().areDeweyIDsStored) {
              final var deweyId = newRtx.getDeweyID();
              jsonInsertDiff.addProperty("deweyID", deweyId.toString());
              jsonInsertDiff.addProperty("depth", deweyId.getLevel());
            }

            addTypeAndDataProperties(newRtx, jsonInsertDiff, newRevisionNumber, emitFromDiffAlgorithm);

            insertedJson.add("insert", jsonInsertDiff);
            jsonDiffs.add(insertedJson);

            break;
          case DELETED:
            final var deletedJson = new JsonObject();
            final var jsonDeletedDiff = new JsonObject();

            jsonDeletedDiff.addProperty("nodeKey", diffTuple.getOldNodeKey());

            // Add path using PathSummary (always available by default)
            // Pass true to include parent path for value nodes (STRING_VALUE, etc.)
            addPathIfAvailable(jsonDeletedDiff, oldRtx, oldRevisionNumber, true, oldArrayPositions);

            if (resourceSession.getResourceConfig().areDeweyIDsStored) {
              final var deweyId = oldRtx.getDeweyID();
              jsonDeletedDiff.addProperty("deweyID", deweyId.toString());
              jsonDeletedDiff.addProperty("depth", deweyId.getLevel());
            }

            deletedJson.add("delete", jsonDeletedDiff);
            jsonDiffs.add(deletedJson);
            break;
          case REPLACEDNEW:
            final var replaceJson = new JsonObject();
            final var jsonReplaceDiff = new JsonObject();

            replaceJson.add("replace", jsonReplaceDiff);

            jsonReplaceDiff.addProperty("oldNodeKey", diffTuple.getOldNodeKey());
            jsonReplaceDiff.addProperty("newNodeKey", diffTuple.getNewNodeKey());

            // Add path using PathSummary (always available by default)
            // For REPLACE, include parent path for values under OBJECT_KEY
            addPathIfAvailable(jsonReplaceDiff, newRtx, newRevisionNumber, true, newArrayPositions);

            if (resourceSession.getResourceConfig().areDeweyIDsStored) {
              final var deweyId = newRtx.getDeweyID();
              jsonReplaceDiff.addProperty("deweyID", deweyId.toString());
              jsonReplaceDiff.addProperty("depth", deweyId.getLevel());
            }

            addTypeAndDataProperties(newRtx, jsonReplaceDiff, newRevisionNumber, emitFromDiffAlgorithm);

            jsonDiffs.add(replaceJson);
            break;
          case UPDATED:
            final var updateJson = new JsonObject();
            final var jsonUpdateDiff = new JsonObject();

            jsonUpdateDiff.addProperty("nodeKey", diffTuple.getOldNodeKey());

            // Add path using PathSummary (always available by default)
            // Include parent path for value nodes under OBJECT_KEY
            addPathIfAvailable(jsonUpdateDiff, newRtx, newRevisionNumber, true, newArrayPositions);

            if (resourceSession.getResourceConfig().areDeweyIDsStored) {
              final var deweyId = newRtx.getDeweyID();
              jsonUpdateDiff.addProperty("deweyID", deweyId.toString());
              jsonUpdateDiff.addProperty("depth", deweyId.getLevel());
            }

            final QNm oldName = oldRtx.getName();
            final QNm newName = newRtx.getName();
            if (!Objects.equals(oldName, newName) && newName != null) {
              jsonUpdateDiff.addProperty("name", newName.toString());
            }
            if (!Objects.equals(oldRtx.getValue(), newRtx.getValue())) {
              if (newRtx.getKind() == NodeKind.BOOLEAN_VALUE || newRtx.getKind() == NodeKind.OBJECT_NAMED_BOOLEAN) {
                jsonUpdateDiff.addProperty("type", "boolean");
                jsonUpdateDiff.addProperty("value", newRtx.getBooleanValue());
              } else if (newRtx.getKind() == NodeKind.STRING_VALUE
                  || newRtx.getKind() == NodeKind.OBJECT_NAMED_STRING) {
                jsonUpdateDiff.addProperty("type", "string");
                jsonUpdateDiff.addProperty("value", newRtx.getValue());
              } else if (newRtx.getKind() == NodeKind.NULL_VALUE || newRtx.getKind() == NodeKind.OBJECT_NAMED_NULL) {
                jsonUpdateDiff.addProperty("type", "null");
                jsonUpdateDiff.add("value", null);
              } else if (newRtx.getKind() == NodeKind.NUMBER_VALUE
                  || newRtx.getKind() == NodeKind.OBJECT_NAMED_NUMBER) {
                jsonUpdateDiff.addProperty("type", "number");
                jsonUpdateDiff.addProperty("value", newRtx.getNumberValue());
              }
            }

            // Setters may be called with the value they already hold. Such a tuple carries only
            // routing metadata and is not an operation a reader can replay. Conversely a fused
            // named primitive may change BOTH its field name and inline value in one transaction;
            // the independent tests above deliberately retain both changes in one update payload.
            if (jsonUpdateDiff.has("name") || jsonUpdateDiff.has("type")) {
              updateJson.add("update", jsonUpdateDiff);
              jsonDiffs.add(updateJson);
            }

            // $CASES-OMITTED$
          default:
            // Do nothing.
        }
      }

      if (oldRevisionNumber < newRevisionNumber) {
        orderInserts(jsonDiffs, oldRtx, newRtx);
      }
      json.add("diffs", JsonDiffSidecar.coalesceDeletes(jsonDiffs, oldRtx, newRtx));
      final var observer = arrayPositionCacheObserver;
      if (observer != null) {
        observer.accept(oldArrayPositions.positionsByNodeKey, oldArrayPositions.walkedNodeKeys);
        observer.accept(newArrayPositions.positionsByNodeKey, newArrayPositions.walkedNodeKeys);
      }
    }

    return finish(json, includeIntegrityMetadata);
  }

  private static void orderInserts(final JsonArray diffs, final JsonNodeReadOnlyTrx previousRevision,
      final JsonNodeReadOnlyTrx newRevision) {
    int insertCount = 0;
    long previousKey = -1;
    boolean ordered = true;
    boolean hasRetainedKeys = false;
    final long previousMaxNodeKey = previousRevision.getMaxNodeKey();
    for (final var operation : diffs) {
      final JsonObject object = operation.getAsJsonObject();
      if (object.has("insert")) {
        final JsonObject insert = object.getAsJsonObject("insert");
        final long nodeKey = insert.get("nodeKey").getAsLong();
        hasRetainedKeys |= nodeKey <= previousMaxNodeKey;
        ordered &= nodeKey > previousKey && insert.get("insertPositionNodeKey").getAsLong() < nodeKey;
        previousKey = nodeKey;
        insertCount++;
      }
    }
    if (insertCount < 2 || ordered && !hasRetainedKeys) {
      return;
    }
    final LongSet retainedKeys = hasRetainedKeys
        ? JsonDiffSidecar.retainedNodeKeys(diffs, previousRevision)
        : LongSets.EMPTY_SET;
    if (ordered && retainedKeys.isEmpty()) {
      return;
    }
    final var inserts = new ArrayList<JsonObject>(insertCount);
    final var rightSiblings = new Long2ObjectOpenHashMap<JsonObject>(insertCount);
    for (final var operation : diffs) {
      final JsonObject object = operation.getAsJsonObject();
      if (object.has("insert")) {
        final JsonObject insert = object.getAsJsonObject("insert");
        if (!retainedKeys.contains(insert.get("nodeKey").getAsLong())) {
          inserts.add(object);
        }
        if ("asRightSibling".equals(insert.get("insertPosition").getAsString())) {
          rightSiblings.put(insert.get("insertPositionNodeKey").getAsLong(), insert);
        }
      }
    }
    inserts.sort(Comparator.comparingLong(operation -> operation.getAsJsonObject("insert").get("nodeKey").getAsLong()));
    if (!retainedKeys.isEmpty()) {
      final var moves = new ArrayList<JsonObject>(retainedKeys.size());
      for (final var operation : diffs) {
        final JsonObject object = operation.getAsJsonObject();
        if (object.has("insert")) {
          final long nodeKey = object.getAsJsonObject("insert").get("nodeKey").getAsLong();
          if (retainedKeys.contains(nodeKey)) {
            moves.add(object);
          }
        }
      }
      orderMoves(moves, newRevision);
      inserts.addAll(moves);
    }
    for (int index = inserts.size() - 1; index >= 0; index--) {
      final JsonObject insert = inserts.get(index).getAsJsonObject("insert");
      final long nodeKey = insert.get("nodeKey").getAsLong();
      final long anchor = insert.get("insertPositionNodeKey").getAsLong();
      final String position = insert.get("insertPosition").getAsString();
      final JsonObject rightSibling = rightSiblings.remove(nodeKey);
      if (rightSibling != null) {
        rightSibling.addProperty("insertPositionNodeKey", anchor);
        rightSibling.addProperty("insertPosition", position);
      }
      if ("asRightSibling".equals(position)) {
        if (rightSibling == null) {
          rightSiblings.remove(anchor);
        } else {
          rightSiblings.put(anchor, rightSibling);
        }
      }
    }
    int insertIndex = 0;
    for (int index = 0; index < diffs.size(); index++) {
      if (diffs.get(index).getAsJsonObject().has("insert")) {
        diffs.set(index, inserts.get(insertIndex++));
      }
    }
  }

  public static void orderMoves(final List<JsonObject> operations, final JsonNodeReadOnlyTrx newRevision) {
    Objects.requireNonNull(operations);
    Objects.requireNonNull(newRevision);
    if (operations.size() < 2) {
      return;
    }
    final var moves = new Long2ObjectOpenHashMap<JsonObject>(operations.size());
    for (final var operation : operations) {
      moves.put(operation.getAsJsonObject("insert").get("nodeKey").getAsLong(), operation);
    }
    final var dependencies = new ArrayList<JsonObject>(operations.size());
    final var ordered = new ArrayList<JsonObject>(operations.size());
    final var ancestorMoves = new Long2LongOpenHashMap(operations.size());
    ancestorMoves.defaultReturnValue(Long.MIN_VALUE);
    final var ancestorPath = new LongArrayList();
    final var allMovedKeys = new LongOpenHashSet(moves.keySet());
    for (final var operation : operations) {
      JsonObject move = moves.remove(operation.getAsJsonObject("insert").get("nodeKey").getAsLong());
      while (move != null) {
        dependencies.add(move);
        final long anchor = move.getAsJsonObject("insert").get("insertPositionNodeKey").getAsLong();
        move = moves.remove(enclosingMove(anchor, newRevision, allMovedKeys, ancestorMoves, ancestorPath));
      }
      for (int index = dependencies.size() - 1; index >= 0; index--) {
        ordered.add(dependencies.get(index));
      }
      dependencies.clear();
    }
    operations.clear();
    operations.addAll(ordered);
  }

  private static long enclosingMove(long anchor, final JsonNodeReadOnlyTrx newRevision, final LongSet retainedKeys,
      final Long2LongOpenHashMap ancestorMoves, final LongArrayList ancestorPath) {
    while (anchor != Fixed.NULL_NODE_KEY.getStandardProperty() && !retainedKeys.contains(anchor)) {
      final long cached = ancestorMoves.get(anchor);
      if (cached != Long.MIN_VALUE) {
        anchor = cached;
        break;
      }
      ancestorPath.add(anchor);
      if (!newRevision.moveTo(anchor)) {
        throw new IllegalStateException("Cannot resolve move anchor " + anchor);
      }
      anchor = newRevision.getParentKey();
    }
    for (int index = 0; index < ancestorPath.size(); index++) {
      ancestorMoves.put(ancestorPath.getLong(index), anchor);
    }
    ancestorPath.clear();
    return anchor;
  }

  private static String finish(final JsonObject document, final boolean includeIntegrityMetadata) {
    if (includeIntegrityMetadata) {
      JsonDiffIntegrity.add(document);
    }
    return document.toString();
  }

  private void insertBasedOnNewRtx(JsonNodeReadOnlyTrx newRtx, JsonObject jsonInsertDiff) {
    jsonInsertDiff.addProperty("nodeKey", newRtx.getNodeKey());
    final var insertPosition = newRtx.hasLeftSibling()
        ? "asRightSibling"
        : "asFirstChild";

    jsonInsertDiff.addProperty("insertPositionNodeKey", newRtx.hasLeftSibling()
        ? newRtx.getLeftSiblingKey()
        : newRtx.getParentKey());
    jsonInsertDiff.addProperty("insertPosition", insertPosition);
  }

  private JsonObject createMetaInfo(final String databaseName, final String resourceName, final int oldRevision,
      final int newRevision) {
    final var json = new JsonObject();
    json.addProperty("database", databaseName);
    json.addProperty("resource", resourceName);
    json.addProperty("old-revision", oldRevision);
    json.addProperty("new-revision", newRevision);
    final var diffsArray = new JsonArray();
    json.add("diffs", diffsArray);
    return json;
  }

  private void addTypeAndDataProperties(JsonNodeReadOnlyTrx newRtx, JsonObject json, int newRevisionNumber,
      boolean emitFromDiffAlgorithm) {
    final NodeKind kind = newRtx.getKind();
    // Fused OBJECT_NAMED_* records carry BOTH the key-name role AND the primitive value.
    // Serialize them as a small jsonFragment "{"name":value}" so insert/replace diffs expose
    // the same external shape as a legacy OBJECT_KEY + primitive-child pair.
    if (newRtx.isArray() || newRtx.isObject() || newRtx.isObjectKey() || kind.isFusedAnyNamed()) {
      json.addProperty("type", "jsonFragment");
      if (emitFromDiffAlgorithm) {
        serialize(newRevisionNumber, resourceSession, newRtx, json);
      }
    } else if (kind == NodeKind.BOOLEAN_VALUE) {
      json.addProperty("type", "boolean");
      json.addProperty("data", newRtx.getBooleanValue());
    } else if (kind == NodeKind.STRING_VALUE) {
      json.addProperty("type", "string");
      json.addProperty("data", newRtx.getValue());
    } else if (kind == NodeKind.NULL_VALUE) {
      json.addProperty("type", "null");
      json.add("data", null);
    } else if (kind == NodeKind.NUMBER_VALUE) {
      json.addProperty("type", "number");
      json.addProperty("data", newRtx.getNumberValue());
    }
  }

  public static void serialize(int newRevision, JsonResourceSession resourceSession, JsonNodeReadOnlyTrx newRtx,
      JsonObject jsonObject) {
    try (final var writer = new StringWriter()) {
      final var serializer =
          JsonSerializer.newBuilder(resourceSession, writer, newRevision).startNodeKey(newRtx.getNodeKey()).build();
      serializer.call();
      jsonObject.addProperty("data", writer.toString());
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Get the path for a node using PathSummary. Returns null if PathSummary is not enabled or if the
   * path cannot be retrieved.
   * 
   * For value nodes (STRING_VALUE, BOOLEAN_VALUE, NUMBER_VALUE, NULL_VALUE), the path is obtained
   * from the parent OBJECT_KEY node since value nodes don't have their own path.
   *
   * @param rtx the read-only transaction positioned at the node
   * @param revisionNumber the revision number
   * @return the path string, or null if unavailable
   */
  private String getNodePath(JsonNodeReadOnlyTrx rtx, int revisionNumber, boolean includeParentPathForValues,
      ArrayPositionCache arrayPositions) {
    if (!resourceSession.getResourceConfig().withPathSummary) {
      return null;
    }

    final long originalNodeKey = rtx.getNodeKey();
    final long nullNodeKey = Fixed.NULL_NODE_KEY.getStandardProperty();
    long pathNodeKey = rtx.getPathNodeKey();

    // OBJECT_KEY and ARRAY nodes have pathNodeKeys
    // OBJECT nodes and value nodes (STRING_VALUE, BOOLEAN_VALUE, etc.) have pathNodeKey ==
    // NULL_NODE_KEY (-1)
    if (pathNodeKey == nullNodeKey && rtx.hasParent()) {
      final NodeKind kind = rtx.getKind();
      final NodeKind parentKind = rtx.getParentKind();
      // iter#32 P2: OBJECT_NAMED_ARRAY plays the OBJECT_KEY+ARRAY role under fusion; every
      // child path resolution that previously bottomed out on ARRAY must accept the fused kind
      // too. OBJECT_NAMED_OBJECT plays the OBJECT_KEY role for the OBJECT_KEY-parent fallback.
      final boolean parentIsArrayLike = parentKind == NodeKind.ARRAY || parentKind == NodeKind.OBJECT_NAMED_ARRAY;
      final boolean parentIsObjectKeyLike = parentKind == NodeKind.OBJECT_NAMED_OBJECT;

      // For structural nodes (OBJECT, ARRAY) in arrays, get path from parent array
      // This gives paths like /foo/[0], /foo/[1] for array elements
      if ((kind == NodeKind.OBJECT || kind == NodeKind.ARRAY) && parentIsArrayLike) {
        rtx.moveToParent();
        pathNodeKey = rtx.getPathNodeKey();
        rtx.moveTo(originalNodeKey);
      }
      // For value nodes in arrays, get path from parent array
      else if (parentIsArrayLike) {
        rtx.moveToParent();
        pathNodeKey = rtx.getPathNodeKey();
        rtx.moveTo(originalNodeKey);
      }
      // For value nodes under OBJECT_KEY, include parent path
      else if (includeParentPathForValues && parentIsObjectKeyLike) {
        rtx.moveToParent();
        pathNodeKey = rtx.getPathNodeKey();
        rtx.moveTo(originalNodeKey);
      }
    }

    // iter#32 P2: OBJECT_NAMED_ARRAY's own pathNodeKey points at the synthetic
    // `__array__/ARRAY` layer added so child paths nest correctly. The field-level path of the
    // record itself (the value for diff `path` reporting) is the OBJECT_KEY parent — walk one
    // level up the path summary so the reported path stops at the field name.
    final boolean cursorOnFusedNamedArray = rtx.getKind() == NodeKind.OBJECT_NAMED_ARRAY;

    // If still no pathNodeKey, return null (no path)
    if (pathNodeKey == nullNodeKey) {
      return null;
    }

    try (final PathSummaryReader pathReader = resourceSession.openPathSummary(revisionNumber)) {
      long effectivePathNodeKey = pathNodeKey;
      if (cursorOnFusedNamedArray) {
        if (pathReader.moveTo(pathNodeKey)) {
          final var arrayPathNode = pathReader.getPathNode();
          if (arrayPathNode != null) {
            final long parentKey = arrayPathNode.getParentKey();
            if (parentKey >= 0 && parentKey != Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
              effectivePathNodeKey = parentKey;
            }
          }
        }
      }
      if (!pathReader.moveTo(effectivePathNodeKey)) {
        return null;
      }

      final var pathNode = pathReader.getPathNode();
      if (pathNode == null) {
        return null;
      }

      final Path<QNm> path = pathReader.getPath();
      if (path == null) {
        return null;
      }

      // Resolve array positions like sdb:path() does
      return resolveArrayPositions(rtx, path, arrayPositions);
    } catch (final IllegalStateException e) {
      // Resource may have been closed (e.g., memory-mapped file reader)
      // This can happen during concurrent operations or cleanup
      return null;
    }
  }

  /**
   * Resolve array indices in the path to concrete positions. Converts "/arr/[]" to "/arr/[3]" based
   * on actual sibling position.
   *
   * @param rtx the transaction positioned at the node
   * @param path the path with unresolved array indices
   * @return the path with resolved array indices
   */
  private String resolveArrayPositions(JsonNodeReadOnlyTrx rtx, Path<QNm> path, ArrayPositionCache arrayPositions) {
    final String pathString = path.toString();

    if (!pathString.contains("[]")) {
      return pathString;
    }

    // We need to walk up the tree to resolve array positions
    final var steps = path.steps();
    final var positions = new IntArrayList();

    // Save current position
    final long originalNodeKey = rtx.getNodeKey();

    try {
      for (int i = steps.size() - 1; i >= 0; i--) {
        final var step = steps.get(i);

        if (step.getAxis() == Path.Axis.CHILD_ARRAY) {
          // For CHILD_ARRAY steps, we need to get the position of the node within the array.
          // If our parent is the array, we're already at the array element.
          // If not, we're deeper inside the structure and need to move up first.
          // iter#32 P2: OBJECT_NAMED_ARRAY is the fused OBJECT_KEY+ARRAY — treat it identically.
          final NodeKind pKind = rtx.getParentKind();
          if (pKind == NodeKind.ARRAY || pKind == NodeKind.OBJECT_NAMED_ARRAY) {
            // We're directly inside the array, get position then move up
            positions.add(arrayPositions.positionOf(rtx));
            rtx.moveToParent();
          } else {
            // We're inside a nested structure, move up to the array element first
            rtx.moveToParent();
            positions.add(arrayPositions.positionOf(rtx));
          }
        } else {
          rtx.moveToParent();
        }
      }

      var result = pathString;
      for (int index = positions.size() - 1; index >= 0; index--) {
        final int pos = positions.getInt(index);
        if (pos == -1) {
          // Keep as [] for arrays that are direct children of object keys
          continue;
        }
        result = result.replaceFirst("/\\[]", "/[" + pos + "]");
      }

      // Replace remaining unresolved positions with []
      result = result.replaceAll("/\\[-1]", "/[]");

      return result;
    } finally {
      // Restore original position
      rtx.moveTo(originalNodeKey);
    }
  }

  /**
   * Add path to a diff JSON object if PathSummary is available.
   *
   * @param json the JSON object to add the path to
   * @param rtx the transaction positioned at the node
   * @param revisionNumber the revision number
   * @param includeParentPath whether to include parent path for value nodes (used for REPLACE
   *        operations)
   */
  private void addPathIfAvailable(JsonObject json, JsonNodeReadOnlyTrx rtx, int revisionNumber,
      boolean includeParentPath, ArrayPositionCache arrayPositions) {
    final String path = getNodePath(rtx, revisionNumber, includeParentPath, arrayPositions);
    if (path != null) {
      json.addProperty("path", path);
    }
  }

  /**
   * Memoizes the sibling ordinals a single read-only revision hands out, so the left walk that
   * determines one of them never repeats a step another tuple already paid for.
   *
   * <p>
   * A lookup walks left from the node until it reaches an already-known sibling or the array's first
   * child, then unwinds and assigns every ordinal it passed. A single lookup therefore costs at most
   * the steps its own index needs, and the total over all tuples of one array is bounded by the
   * number of distinct siblings walked plus one step per lookup - linear, not quadratic. Nothing is
   * pre-sized, nothing is allocated until a position is actually resolved, and nothing beyond the
   * walked prefix is stored, so both time and memory follow the largest index actually asked for
   * rather than the array's length.
   *
   * <p>
   * Node keys are unique within a revision, so one key has one ordinal; the two fallback caches are
   * method-local and become unreachable when {@code serialize} returns. The new revision may also
   * borrow valid ingest positions for this call, avoiding the walk and fallback allocation entirely.
   */
  private static final class ArrayPositionCache {

    /** Neither a valid ordinal nor a cached one: {@code Long2IntOpenHashMap}'s miss value. */
    private static final int UNKNOWN_POSITION = -1;

    /**
     * Allocated by the first lookup that reaches the walk. A serialization that resolves no array
     * position - no path summary, or no emitted path with an array step - allocates no backing storage
     * at all.
     */
    private @Nullable Long2IntOpenHashMap positionsByNodeKey;

    /** Read-only, commit-scoped hints; the fallback cache never modifies the ingest map. */
    private final @Nullable Long2IntMap knownPositions;

    private ArrayPositionCache(final @Nullable Long2IntMap knownPositions) {
      this.knownPositions = knownPositions;
    }

    /** Reused across lookups; holds the keys of one walk, nearest sibling last. */
    private final LongArrayList walkedNodeKeys = new LongArrayList();

    private int positionOf(final JsonNodeReadOnlyTrx rtx) {
      // iter#32 P2: OBJECT_NAMED_OBJECT plays the OBJECT_KEY role under fusion. An ARRAY whose
      // parent is the fused OBJECT_KEY-equivalent has no sibling index either.
      final NodeKind parentKind = rtx.getParentKind();
      if (parentKind == NodeKind.OBJECT_NAMED_OBJECT && rtx.isArray()) {
        return -1;
      }

      final long originalNodeKey = rtx.getNodeKey();
      Long2IntOpenHashMap positions = positionsByNodeKey;
      if (knownPositions != null) {
        final int knownPosition = knownPositions.getOrDefault(originalNodeKey, UNKNOWN_POSITION);
        if (knownPosition >= 0) {
          return knownPosition;
        }
      }
      if (positions == null) {
        positions = new Long2IntOpenHashMap();
        positions.defaultReturnValue(UNKNOWN_POSITION);
        positionsByNodeKey = positions;
      } else {
        final int cachedPosition = positions.get(originalNodeKey);
        if (cachedPosition >= 0) {
          return cachedPosition;
        }
      }

      walkedNodeKeys.clear();
      try {
        long anchorNodeKey = originalNodeKey;
        int anchorPosition = UNKNOWN_POSITION;
        while (rtx.hasLeftSibling()) {
          walkedNodeKeys.add(anchorNodeKey);
          rtx.moveToLeftSibling();
          anchorNodeKey = rtx.getNodeKey();
          anchorPosition = positions.get(anchorNodeKey);
          if (anchorPosition < 0 && knownPositions != null) {
            anchorPosition = knownPositions.getOrDefault(anchorNodeKey, UNKNOWN_POSITION);
          }
          if (anchorPosition >= 0) {
            break;
          }
        }

        if (anchorPosition < 0) {
          anchorPosition = 0;
          positions.put(anchorNodeKey, anchorPosition);
        }

        int position = anchorPosition;
        for (int index = walkedNodeKeys.size() - 1; index >= 0; index--) {
          positions.put(walkedNodeKeys.getLong(index), ++position);
        }
        return position;
      } finally {
        rtx.moveTo(originalNodeKey);
      }
    }
  }
}
