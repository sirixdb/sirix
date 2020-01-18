package org.sirix.service.json;

import com.google.api.client.util.Objects;
import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.sirix.access.trx.node.HashType;
import org.sirix.api.JsonDiff;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.diff.DiffDepth;
import org.sirix.diff.DiffFactory;
import org.sirix.diff.DiffObserver;
import org.sirix.diff.DiffTuple;
import org.sirix.node.NodeKind;
import org.sirix.service.json.serialize.JsonSerializer;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Implements a JSON-diff serialization format.
 *
 * @author Johannes Lichtenberger
 */
public final class BasicJsonDiff implements DiffObserver, JsonDiff {

  private final List<DiffTuple> diffs;

  /**
   * Constructor.
   */
  public BasicJsonDiff() {
    this.diffs = new ArrayList<>();
  }

  /**
   * Diff two revisions.
   *
   * @param resourceManager   the resource manager to use
   * @param oldRevisionNumber the revision number of the older revision
   * @param newRevisionNumber the revision number of the newer revision
   * @return a JSON-String describing the differences encountered between the two revisions
   */
  @Override
  public String generateDiff(JsonResourceManager resourceManager, int oldRevisionNumber, int newRevisionNumber) {
    return generateDiff(resourceManager, oldRevisionNumber, newRevisionNumber, 0);
  }

  /**
   * Diff two revisions.
   *
   * @param resourceManager   the resource manager to use
   * @param oldRevisionNumber the revision number of the older revision
   * @param newRevisionNumber the revision number of the newer revision
   * @return a JSON-String describing the differences encountered between the two revisions
   */
  @Override
  public String generateDiff(JsonResourceManager resourceManager, int oldRevisionNumber, int newRevisionNumber,
      long startNodeKey) {
    diffs.clear();

    final var databaseName = resourceManager.getDatabase().getName();
    final var resourceName = resourceManager.getResourceConfig().getName();

    DiffFactory.invokeJsonDiff(new DiffFactory.Builder<>(resourceManager, newRevisionNumber, oldRevisionNumber,
                                                         resourceManager.getResourceConfig().hashType == HashType.NONE
                                                             ? DiffFactory.DiffOptimized.NO
                                                             : DiffFactory.DiffOptimized.HASHED,
                                                         ImmutableSet.of(this)).skipSubtrees(true).newStartKey(startNodeKey).oldStartKey(startNodeKey));

    final var json = createMetaInfo(databaseName, resourceName, oldRevisionNumber, newRevisionNumber);

    if (diffs.size() == 1 && (diffs.get(0).getDiff() == DiffFactory.DiffType.SAMEHASH || diffs.get(0).getDiff()
        == DiffFactory.DiffType.SAME)) {
      return json.toString();
    }

    final var jsonDiffs = json.getAsJsonArray("diffs");

    try (final var oldRtx = resourceManager.beginNodeReadOnlyTrx(oldRevisionNumber);
        final var newRtx = resourceManager.beginNodeReadOnlyTrx(newRevisionNumber)) {

      final Iterator<DiffTuple> iter = diffs.iterator();
      while (iter.hasNext()) {
        final var diffTuple = iter.next();

        final var diffType = diffTuple.getDiff();

        if (diffType == DiffFactory.DiffType.SAME || diffType == DiffFactory.DiffType.SAMEHASH
            || diffType == DiffFactory.DiffType.REPLACEDOLD) {
          iter.remove();
        } else if (diffType == DiffFactory.DiffType.INSERTED || diffType == DiffFactory.DiffType.REPLACEDNEW) {
          newRtx.moveTo(diffTuple.getNewNodeKey());
        }

        if (diffType == DiffFactory.DiffType.DELETED) {
          oldRtx.moveTo(diffTuple.getOldNodeKey());
        }
      }

      if (diffs.isEmpty())
        return json.toString();

      for (final var diffTuple : diffs) {
        final var diffType = diffTuple.getDiff();
        newRtx.moveTo(diffTuple.getNewNodeKey());
        oldRtx.moveTo(diffTuple.getOldNodeKey());

        switch (diffType) {
          case INSERTED:
            final var insertedJson = new JsonObject();
            final var jsonInsertDiff = new JsonObject();

            final var insertPosition = newRtx.hasLeftSibling() ? "asRightSibling" : "asFirstChild";

            jsonInsertDiff.addProperty("oldNodeKey", diffTuple.getOldNodeKey());
            jsonInsertDiff.addProperty("newNodeKey", diffTuple.getNewNodeKey());
            jsonInsertDiff.addProperty("insertPositionNodeKey",
                                       newRtx.hasLeftSibling() ? newRtx.getLeftSiblingKey() : newRtx.getParentKey());
            jsonInsertDiff.addProperty("insertPosition", insertPosition);

            addTypeAndDataProperties(newRevisionNumber, resourceManager, newRtx, jsonInsertDiff);

            insertedJson.add("insert", jsonInsertDiff);
            jsonDiffs.add(insertedJson);

            break;
          case DELETED:
            final var deletedJson = new JsonObject();

            deletedJson.addProperty("delete", diffTuple.getOldNodeKey());

            jsonDiffs.add(deletedJson);
            break;
          case REPLACEDNEW:
            final var replaceJson = new JsonObject();
            final var jsonReplaceDiff = new JsonObject();

            replaceJson.add("replace", jsonReplaceDiff);

            jsonReplaceDiff.addProperty("oldNodeKey", diffTuple.getOldNodeKey());
            jsonReplaceDiff.addProperty("newNodeKey", diffTuple.getNewNodeKey());

            addTypeAndDataProperties(newRevisionNumber, resourceManager, newRtx, jsonReplaceDiff);

            jsonDiffs.add(replaceJson);
            break;
          case UPDATED:
            final var updateJson = new JsonObject();
            final var jsonUpdateDiff = new JsonObject();

            jsonUpdateDiff.addProperty("nodeKey", diffTuple.getOldNodeKey());

            if (!Objects.equal(oldRtx.getName(), newRtx.getName())) {
              jsonUpdateDiff.addProperty("name", newRtx.getName().toString());
            } else if (!Objects.equal(oldRtx.getValue(), newRtx.getValue())) {
              if (newRtx.getKind() == NodeKind.BOOLEAN_VALUE) {
                jsonUpdateDiff.addProperty("type", "boolean");
                jsonUpdateDiff.addProperty("value", newRtx.getBooleanValue());
              } else if (newRtx.getKind() == NodeKind.STRING_VALUE) {
                jsonUpdateDiff.addProperty("type", "string");
                jsonUpdateDiff.addProperty("value", newRtx.getValue());
              } else if (newRtx.getKind() == NodeKind.NULL_VALUE) {
                jsonUpdateDiff.addProperty("type", "null");
                jsonUpdateDiff.add("value", null);
              } else if (newRtx.getKind() == NodeKind.NUMBER_VALUE) {
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

  private void addTypeAndDataProperties(int newRevision, JsonResourceManager resourceManager,
      JsonNodeReadOnlyTrx newRtx, JsonObject json) {
    if (newRtx.getChildCount() > 0) {
      json.addProperty("type", "jsonFragment");
      serialize(newRevision, resourceManager, newRtx, json);
    } else if (newRtx.getKind() == NodeKind.BOOLEAN_VALUE) {
      json.addProperty("type", "boolean");
      json.addProperty("data", newRtx.getBooleanValue());
    } else if (newRtx.getKind() == NodeKind.STRING_VALUE) {
      json.addProperty("type", "string");
      json.addProperty("data", newRtx.getValue());
    } else if (newRtx.getKind() == NodeKind.NULL_VALUE) {
      json.addProperty("type", "null");
      json.add("data", null);
    } else if (newRtx.getKind() == NodeKind.NUMBER_VALUE) {
      json.addProperty("type", "number");
      json.addProperty("data", newRtx.getNumberValue());
    }
  }

  private void serialize(int newRevision, JsonResourceManager resourceManager, JsonNodeReadOnlyTrx newRtx,
      JsonObject jsonReplaceDiff) {
    try (final var writer = new StringWriter()) {
      final var serializer = JsonSerializer.newBuilder(resourceManager, writer, newRevision).startNodeKey(
          newRtx.getNodeKey()).build();
      serializer.call();
      jsonReplaceDiff.addProperty("data", writer.toString());
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
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

  @Override
  public void diffListener(@Nonnull final DiffFactory.DiffType diffType, final long newNodeKey, final long oldNodeKey,
      @Nonnull final DiffDepth depth) {
    diffs.add(new DiffTuple(diffType, newNodeKey, oldNodeKey, depth));
  }

  @Override
  public void diffDone() {
  }
}
