package org.sirix.diff;

import com.google.api.client.util.Objects;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.node.NodeKind;
import org.sirix.service.json.serialize.JsonSerializer;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.Collection;

public final class JsonDiffSerializer {
  private final JsonResourceManager resourceManager;
  private final int oldRevisionNumber;
  private final int newRevisionNumber;
  private final Collection<DiffTuple> diffs;

  public JsonDiffSerializer(JsonResourceManager resourceManager, int oldRevisionNumber, int newRevisionNumber,
      Collection<DiffTuple> diffs) {
    this.resourceManager = resourceManager;
    this.oldRevisionNumber = oldRevisionNumber;
    this.newRevisionNumber = newRevisionNumber;
    this.diffs = diffs;
  }

  public String serialize(boolean emitFromDiffAlgorithm) {
    final var databaseName = resourceManager.getDatabase().getName();
    final var resourceName = resourceManager.getResourceConfig().getName();

    final var json = createMetaInfo(databaseName, resourceName, oldRevisionNumber, newRevisionNumber);

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

        if (diffTuple.getDiff() == DiffFactory.DiffType.INSERTED) {
          newRtx.moveTo(diffTuple.getNewNodeKey());
        } else if (diffTuple.getDiff() == DiffFactory.DiffType.DELETED) {
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

            if (resourceManager.getResourceConfig().areDeweyIDsStored) {
              final var jsonDeletedDiff = new JsonObject();

              jsonDeletedDiff.addProperty("nodeKey", diffTuple.getOldNodeKey());

              final var deweyId = oldRtx.getDeweyID();
              jsonDeletedDiff.addProperty("deweyID", deweyId.toString());
              jsonDeletedDiff.addProperty("depth", deweyId.getLevel());

              deletedJson.add("delete", jsonDeletedDiff);
            } else {
              deletedJson.addProperty("delete", diffTuple.getOldNodeKey());
            }

            jsonDiffs.add(deletedJson);
            break;
          case REPLACEDNEW:
            final var replaceJson = new JsonObject();
            final var jsonReplaceDiff = new JsonObject();

            replaceJson.add("replace", jsonReplaceDiff);

            jsonReplaceDiff.addProperty("oldNodeKey", diffTuple.getOldNodeKey());
            jsonReplaceDiff.addProperty("newNodeKey", diffTuple.getNewNodeKey());

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

  public static void serialize(int newRevision, JsonResourceManager resourceManager, JsonNodeReadOnlyTrx newRtx,
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
}