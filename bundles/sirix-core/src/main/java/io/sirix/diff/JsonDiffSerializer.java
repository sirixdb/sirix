package io.sirix.diff;

import com.google.api.client.util.Objects;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.brackit.query.atomic.QNm;
import io.brackit.query.util.path.Path;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.node.NodeKind;
import io.sirix.settings.Fixed;
import io.sirix.service.json.serialize.JsonSerializer;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.Collection;

public final class JsonDiffSerializer {

  private final String databaseName;
  private final JsonResourceSession resourceManager;
  private final int oldRevisionNumber;
  private final int newRevisionNumber;
  private final Collection<DiffTuple> diffs;

  public JsonDiffSerializer(final String databaseName,
                            JsonResourceSession resourceManager,
                            int oldRevisionNumber,
                            int newRevisionNumber,
                            Collection<DiffTuple> diffs) {
    this.databaseName = databaseName;
    this.resourceManager = resourceManager;
    this.oldRevisionNumber = oldRevisionNumber;
    this.newRevisionNumber = newRevisionNumber;
    this.diffs = diffs;
  }

  public String serialize(boolean emitFromDiffAlgorithm) {
    final var resourceName = resourceManager.getResourceConfig().getName();

    final JsonObject json = createMetaInfo(databaseName, resourceName, oldRevisionNumber, newRevisionNumber);

    if (diffs.size() == 1) {
      final var tuple = diffs.iterator().next();
      if (tuple.getDiff() == DiffFactory.DiffType.SAME || tuple.getDiff() == DiffFactory.DiffType.SAMEHASH) {
        return json.toString();
      }
    }

    final var jsonDiffs = json.getAsJsonArray("diffs");

    try (final var oldRtx = resourceManager.beginNodeReadOnlyTrx(oldRevisionNumber);
         final var newRtx = resourceManager.beginNodeReadOnlyTrx(newRevisionNumber)) {
      if (emitFromDiffAlgorithm) {
        diffs.removeIf(diffTuple -> diffTuple.getDiff() == DiffFactory.DiffType.SAME
            || diffTuple.getDiff() == DiffFactory.DiffType.SAMEHASH
            || diffTuple.getDiff() == DiffFactory.DiffType.REPLACEDOLD);
      }

      if (diffs.isEmpty()) {
        return json.toString();
      }

      for (final var diffTuple : diffs) {
        final var diffType = diffTuple.getDiff();

        if (diffType == DiffFactory.DiffType.INSERTED) {
          newRtx.moveTo(diffTuple.getNewNodeKey());
        } else if (diffType == DiffFactory.DiffType.DELETED) {
          oldRtx.moveTo(diffTuple.getOldNodeKey());
        } else {
          newRtx.moveTo(diffTuple.getNewNodeKey());
          oldRtx.moveTo(diffTuple.getOldNodeKey());
        }

        switch (diffType) {
          case INSERTED:
            final var insertedJson = new JsonObject();
            final var jsonInsertDiff = new JsonObject();

            insertBasedOnNewRtx(newRtx, jsonInsertDiff);

            // Add path using PathSummary (always available by default)
            addPathIfAvailable(jsonInsertDiff, newRtx, newRevisionNumber, false);

            if (resourceManager.getResourceConfig().areDeweyIDsStored) {
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
            addPathIfAvailable(jsonDeletedDiff, oldRtx, oldRevisionNumber, false);

            if (resourceManager.getResourceConfig().areDeweyIDsStored) {
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
            addPathIfAvailable(jsonReplaceDiff, newRtx, newRevisionNumber, true);

            if (resourceManager.getResourceConfig().areDeweyIDsStored) {
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
            // For UPDATE, only use node's own pathNodeKey (no parent path for values)
            addPathIfAvailable(jsonUpdateDiff, newRtx, newRevisionNumber, false);

            if (resourceManager.getResourceConfig().areDeweyIDsStored) {
              final var deweyId = newRtx.getDeweyID();
              jsonUpdateDiff.addProperty("deweyID", deweyId.toString());
              jsonUpdateDiff.addProperty("depth", deweyId.getLevel());
            }

            if (!Objects.equal(oldRtx.getName(), newRtx.getName())) {
              jsonUpdateDiff.addProperty("name", newRtx.getName().toString());
            } else if (!Objects.equal(oldRtx.getValue(), newRtx.getValue())) {
              if (newRtx.getKind() == NodeKind.BOOLEAN_VALUE || newRtx.getKind() == NodeKind.OBJECT_BOOLEAN_VALUE) {
                jsonUpdateDiff.addProperty("type", "boolean");
                jsonUpdateDiff.addProperty("value", newRtx.getBooleanValue());
              } else if (newRtx.getKind() == NodeKind.STRING_VALUE
                  || newRtx.getKind() == NodeKind.OBJECT_STRING_VALUE) {
                jsonUpdateDiff.addProperty("type", "string");
                jsonUpdateDiff.addProperty("value", newRtx.getValue());
              } else if (newRtx.getKind() == NodeKind.NULL_VALUE || newRtx.getKind() == NodeKind.OBJECT_NULL_VALUE) {
                jsonUpdateDiff.addProperty("type", "null");
                jsonUpdateDiff.add("value", null);
              } else if (newRtx.getKind() == NodeKind.NUMBER_VALUE
                  || newRtx.getKind() == NodeKind.OBJECT_NUMBER_VALUE) {
                jsonUpdateDiff.addProperty("type", "number");
                jsonUpdateDiff.addProperty("value", newRtx.getNumberValue());
              }
            }

            updateJson.add("update", jsonUpdateDiff);
            jsonDiffs.add(updateJson);

            // $CASES-OMITTED$
          default:
            // Do nothing.
        }
      }
    }

    return json.toString();
  }

  private void insertBasedOnNewRtx(JsonNodeReadOnlyTrx newRtx, JsonObject jsonInsertDiff) {
    jsonInsertDiff.addProperty("nodeKey", newRtx.getNodeKey());
    final var insertPosition = newRtx.hasLeftSibling() ? "asRightSibling" : "asFirstChild";

    jsonInsertDiff.addProperty("insertPositionNodeKey",
                               newRtx.hasLeftSibling() ? newRtx.getLeftSiblingKey() : newRtx.getParentKey());
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
    if (newRtx.isArray() || newRtx.isObject() || newRtx.isObjectKey()) {
      json.addProperty("type", "jsonFragment");
      if (emitFromDiffAlgorithm) {
        serialize(newRevisionNumber, resourceManager, newRtx, json);
      }
    } else if (newRtx.getKind() == NodeKind.BOOLEAN_VALUE || newRtx.getKind() == NodeKind.OBJECT_BOOLEAN_VALUE) {
      json.addProperty("type", "boolean");
      json.addProperty("data", newRtx.getBooleanValue());
    } else if (newRtx.getKind() == NodeKind.STRING_VALUE || newRtx.getKind() == NodeKind.OBJECT_STRING_VALUE) {
      json.addProperty("type", "string");
      json.addProperty("data", newRtx.getValue());
    } else if (newRtx.getKind() == NodeKind.NULL_VALUE || newRtx.getKind() == NodeKind.OBJECT_NULL_VALUE) {
      json.addProperty("type", "null");
      json.add("data", null);
    } else if (newRtx.getKind() == NodeKind.NUMBER_VALUE || newRtx.getKind() == NodeKind.OBJECT_NUMBER_VALUE) {
      json.addProperty("type", "number");
      json.addProperty("data", newRtx.getNumberValue());
    }
  }

  public static void serialize(int newRevision, JsonResourceSession resourceManager, JsonNodeReadOnlyTrx newRtx,
      JsonObject jsonObject) {
    try (final var writer = new StringWriter()) {
      final var serializer =
          JsonSerializer.newBuilder(resourceManager, writer, newRevision).startNodeKey(newRtx.getNodeKey()).build();
      serializer.call();
      jsonObject.addProperty("data", writer.toString());
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Get the path for a node using PathSummary.
   * Returns null if PathSummary is not enabled or if the path cannot be retrieved.
   * 
   * For value nodes (STRING_VALUE, BOOLEAN_VALUE, NUMBER_VALUE, NULL_VALUE), the path
   * is obtained from the parent OBJECT_KEY node since value nodes don't have their own path.
   *
   * @param rtx the read-only transaction positioned at the node
   * @param revisionNumber the revision number
   * @return the path string, or null if unavailable
   */
  private String getNodePath(JsonNodeReadOnlyTrx rtx, int revisionNumber, boolean includeParentPathForValues) {
    if (!resourceManager.getResourceConfig().withPathSummary) {
      return null;
    }

    final long originalNodeKey = rtx.getNodeKey();
    final long nullNodeKey = Fixed.NULL_NODE_KEY.getStandardProperty();
    long pathNodeKey = rtx.getPathNodeKey();
    
    // OBJECT_KEY and ARRAY nodes have pathNodeKeys
    // OBJECT nodes and value nodes (STRING_VALUE, BOOLEAN_VALUE, etc.) have pathNodeKey == NULL_NODE_KEY (-1)
    if (pathNodeKey == nullNodeKey && rtx.hasParent()) {
      final NodeKind kind = rtx.getKind();
      final NodeKind parentKind = rtx.getParentKind();
      
      // For structural nodes (OBJECT, ARRAY) in arrays, always get path from parent array
      // This gives paths like /[0], /[1] for array elements
      if ((kind == NodeKind.OBJECT || kind == NodeKind.ARRAY) && parentKind == NodeKind.ARRAY) {
        rtx.moveToParent();
        pathNodeKey = rtx.getPathNodeKey();
        rtx.moveTo(originalNodeKey);
      }
      // For value nodes under OBJECT_KEY, only include parent path for REPLACE operations
      else if (includeParentPathForValues && parentKind == NodeKind.OBJECT_KEY) {
        rtx.moveToParent();
        pathNodeKey = rtx.getPathNodeKey();
        rtx.moveTo(originalNodeKey);
      }
    }
    
    // If still no pathNodeKey, return null (no path)
    if (pathNodeKey == nullNodeKey) {
      return null;
    }

    try (final PathSummaryReader pathReader = resourceManager.openPathSummary(revisionNumber)) {
      if (!pathReader.moveTo(pathNodeKey)) {
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
      return resolveArrayPositions(rtx, path);
    } catch (final IllegalStateException e) {
      // Resource may have been closed (e.g., memory-mapped file reader)
      // This can happen during concurrent operations or cleanup
      return null;
    }
  }

  /**
   * Resolve array indices in the path to concrete positions.
   * Converts "/arr/[]" to "/arr/[3]" based on actual sibling position.
   *
   * @param rtx the transaction positioned at the node
   * @param path the path with unresolved array indices
   * @return the path with resolved array indices
   */
  private String resolveArrayPositions(JsonNodeReadOnlyTrx rtx, Path<QNm> path) {
    final String pathString = path.toString();

    if (!pathString.contains("[]")) {
      return pathString;
    }

    // We need to walk up the tree to resolve array positions
    final var steps = path.steps();
    final var positions = new ArrayDeque<Integer>();

    // Save current position
    final long originalNodeKey = rtx.getNodeKey();

    try {
      for (int i = steps.size() - 1; i >= 0; i--) {
        final var step = steps.get(i);

        if (step.getAxis() == Path.Axis.CHILD_ARRAY) {
          positions.addFirst(getArrayPosition(rtx));
          rtx.moveToParent();
        } else {
          rtx.moveToParent();
        }
      }

      var result = pathString;
      for (Integer pos : positions) {
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
   * Get the array position (index) of the current node among its siblings.
   *
   * @param rtx the transaction positioned at the node
   * @return the 0-based index, or -1 if this is an array directly under an object key
   */
  private int getArrayPosition(JsonNodeReadOnlyTrx rtx) {
    if (rtx.getParentKind() == NodeKind.OBJECT_KEY && rtx.isArray()) {
      return -1;
    }

    int index = 0;
    while (rtx.hasLeftSibling()) {
      rtx.moveToLeftSibling();
      index++;
    }
    return index;
  }

  /**
   * Add path to a diff JSON object if PathSummary is available.
   *
   * @param json the JSON object to add the path to
   * @param rtx the transaction positioned at the node
   * @param revisionNumber the revision number
   * @param includeParentPath whether to include parent path for value nodes (used for REPLACE operations)
   */
  private void addPathIfAvailable(JsonObject json, JsonNodeReadOnlyTrx rtx, int revisionNumber, boolean includeParentPath) {
    final String path = getNodePath(rtx, revisionNumber, includeParentPath);
    if (path != null) {
      json.addProperty("path", path);
    }
  }
}
