/*
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.service.json.shredder;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.InsertOperations;
import io.sirix.access.trx.node.json.objectvalue.ArrayValue;
import io.sirix.access.trx.node.json.objectvalue.BooleanValue;
import io.sirix.access.trx.node.json.objectvalue.NullValue;
import io.sirix.access.trx.node.json.objectvalue.NumberValue;
import io.sirix.access.trx.node.json.objectvalue.ObjectRecordValue;
import io.sirix.access.trx.node.json.objectvalue.ObjectValue;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.Axis;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.visitor.JsonNodeVisitor;
import io.sirix.api.visitor.VisitResult;
import io.sirix.api.visitor.VisitResultType;
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.IncludeSelf;
import io.sirix.axis.visitor.VisitorDescendantAxis;
import io.sirix.diff.JsonDiffSerializer;
import io.sirix.diff.JsonDiffSidecar;
import io.sirix.node.NodeKind;
import io.sirix.node.immutable.json.ImmutableArrayNode;
import io.sirix.node.immutable.json.ImmutableObjectNode;
import io.sirix.node.json.ObjectNamedArrayNode;
import io.sirix.node.json.ObjectNamedObjectNode;
import io.sirix.service.InsertPosition;
import io.sirix.service.ShredderCommit;
import io.sirix.service.json.BasicJsonDiff;
import io.sirix.settings.Fixed;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;

import static java.util.Objects.requireNonNull;

/**
 * Copy a resource or a subtree into another resoure. even copy all changes and revisions between a
 * given revision/transaction.
 */
public final class JsonResourceCopy implements Callable<Void> {

  private final String INSERT = InsertOperations.INSERT.getName();
  private final String UPDATE = InsertOperations.UPDATE.getName();
  private final String DELETE = InsertOperations.DELETE.getName();

  private final JsonResourceSession readResourceSession;

  /**
   * {@link JsonNodeTrx}.
   */
  private final JsonNodeTrx wtx;

  /**
   * Determines if changes are going to be commit right after shredding.
   */
  private final ShredderCommit commit;

  private final JsonNodeReadOnlyTrx rtx;

  private final long startNodeKey;

  /**
   * Insertion position.
   */
  private final InsertPosition insert;

  /**
   * Determines if diffs between revisions should be copied.
   */
  private final boolean copyAllRevisionsUpToMostRecent;

  /**
   * Builder to build a {@link JsonResourceCopy} instance.
   */
  public static class Builder {

    /**
     * {@link JsonNodeTrx} implementation.
     */
    private final JsonNodeTrx wtx;

    /**
     * The transaction to read from.
     */
    private final JsonNodeReadOnlyTrx rtx;

    /**
     * Insertion position.
     */
    private final InsertPosition insert;

    /**
     * Determines if after shredding the transaction should be immediately committed.
     */
    private ShredderCommit commit = ShredderCommit.NOCOMMIT;

    private boolean copyAllRevisionsUpToMostRecent;

    /**
     * Constructor.
     *
     * @param wtx the transaction to write to
     * @param rtx the transaction to read from
     * @param insert insertion position
     * @throws NullPointerException if one of the arguments is {@code null}
     */
    public Builder(final JsonNodeTrx wtx, final JsonNodeReadOnlyTrx rtx, final InsertPosition insert) {
      this.wtx = requireNonNull(wtx);
      this.rtx = requireNonNull(rtx);
      this.insert = requireNonNull(insert);
    }

    /**
     * Commit afterwards.
     *
     * @return this builder instance
     */
    public JsonResourceCopy.Builder commitAfterwards() {
      commit = ShredderCommit.COMMIT;
      return this;
    }

    /**
     * Copy and commit the initial source revision and each later revision up to the most recent
     * revision. Source node keys are preserved for replay; the destination must not already contain any
     * key being copied. Snapshot-only copying instead allocates destination keys normally.
     *
     * @return this builder instance
     */
    public JsonResourceCopy.Builder copyAllRevisionsUpToMostRecent() {
      copyAllRevisionsUpToMostRecent = true;
      return this;
    }

    /**
     * Build an instance.
     *
     * @return {@link JsonResourceCopy} instance
     */
    public JsonResourceCopy build() {
      return new JsonResourceCopy(wtx, rtx, this);
    }
  }

  /**
   * Stack for reading end element.
   */
  private final LongArrayList stack = new LongArrayList();

  /**
   * Private constructor.
   *
   * @param wtx the transaction used to write
   * @param rtx the transaction used to read
   * @param builder builder of the JSON resource copy
   */
  private JsonResourceCopy(final JsonNodeTrx wtx, final JsonNodeReadOnlyTrx rtx, final Builder builder) {
    this.wtx = wtx;
    this.rtx = rtx;
    this.readResourceSession = rtx.getResourceSession();
    this.insert = builder.insert;
    this.commit = builder.commit;
    this.startNodeKey = rtx.getNodeKey();
    this.copyAllRevisionsUpToMostRecent = builder.copyAllRevisionsUpToMostRecent;
  }

  public Void call() {
    rtx.moveTo(startNodeKey);

    insert();

    if (copyAllRevisionsUpToMostRecent) {
      preserveAllocationFrontier(rtx);
      wtx.commit();

      for (var revision = rtx.getRevisionNumber() + 1; revision <= rtx.getResourceSession()
                                                                      .getMostRecentRevisionNumber(); revision++) {
        try (final var rtxOnRevision = readResourceSession.beginNodeReadOnlyTrx(revision);
            final var previousRevision = readResourceSession.beginNodeReadOnlyTrx(revision - 1)) {
          // Validate the raw sidecar once, but do not hydrate jsonFragment operations into full
          // strings: replay copies those subtrees directly from rtxOnRevision and must stay bounded.
          final var updateOperationsFile =
              readResourceSession.getResourceConfig()
                                 .getResource()
                                 .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath())
                                 .resolve("diffFromRev" + (revision - 1) + "toRev" + revision + ".json");
          JsonObject sidecar;
          try {
            sidecar = JsonDiffSidecar.read(updateOperationsFile, readResourceSession.getResourceConfig().getName(),
                revision - 1, revision, readResourceSession.getResourceConfig().areDeweyIDsStored);
          } catch (final IOException | RuntimeException e) {
            // A sidecar written before the integrity envelope (or damaged since) must not abort a
            // copy whose earlier revisions are ALREADY committed — that leaves a partial copy.
            // The sidecar only caches the diff: recompute it from the two source revisions.
            final Path resourcePath = readResourceSession.getResourceConfig().getResource();
            final String databaseName = resourcePath.getParent().getParent().getFileName().toString();
            sidecar = JsonParser
                                .parseString(new BasicJsonDiff(databaseName).generateDiffForReplay(readResourceSession,
                                    revision - 1, revision))
                                .getAsJsonObject();
          }

          replay(
              JsonDiffSidecar.normalizeReplacements(sidecar.getAsJsonArray("diffs"), previousRevision, rtxOnRevision),
              previousRevision, rtxOnRevision);
          wtx.commit();
        }
      }
    } else {
      commit.commit(wtx);
    }

    return null;
  }

  private void executeDelete(final long nodeKey) {
    requireMove(wtx.moveTo(nodeKey), "delete destination", nodeKey);
    wtx.remove();
  }

  private void executeUpdate(JsonObject updateObject, JsonNodeReadOnlyTrx rtxOnRevision) {
    final var key = updateObject.get("nodeKey").getAsLong();
    requireMove(wtx.moveTo(key), "update destination", key);

    if (updateObject.has("name")) {
      wtx.setObjectKeyName(updateObject.get("name").getAsString());
    }
    if (!updateObject.has("type")) {
      return;
    }

    requireMove(rtxOnRevision.moveTo(key), "update source", key);
    switch (updateObject.get("type").getAsString()) {
      case "boolean" -> wtx.setBooleanValue(rtxOnRevision.getBooleanValue());
      case "string" -> wtx.setStringValue(rtxOnRevision.getValue());
      case "number" -> wtx.setNumberValue(rtxOnRevision.getNumberValue());
      default -> throw new IllegalStateException("Unsupported replay update type: " + updateObject.get("type"));
    }
  }

  private void executeMove(final JsonObject moveObject, final JsonNodeReadOnlyTrx source) {
    final long nodeKey = moveObject.get("nodeKey").getAsLong();
    final long anchor = moveObject.get("insertPositionNodeKey").getAsLong();
    requireMove(wtx.moveTo(anchor), "move destination", anchor);
    requireMove(source.moveTo(nodeKey), "move source", nodeKey);
    switch (InsertPosition.ofString(moveObject.get("insertPosition").getAsString())) {
      case AS_FIRST_CHILD -> wtx.moveSubtreeToFirstChild(nodeKey);
      case AS_RIGHT_SIBLING -> wtx.moveSubtreeToRightSibling(nodeKey);
      default -> throw new IllegalStateException("Unsupported replay move position");
    }
    requireMove(wtx.moveTo(nodeKey), "moved destination", nodeKey);
    if (source.getKind().playsObjectKeyRole() && !Objects.equals(wtx.getName(), source.getName())) {
      wtx.setObjectKeyName(source.getName().getLocalName());
    }
    switch (source.getKind()) {
      case BOOLEAN_VALUE, OBJECT_NAMED_BOOLEAN -> {
        if (wtx.getBooleanValue() != source.getBooleanValue()) {
          wtx.setBooleanValue(source.getBooleanValue());
        }
      }
      case NUMBER_VALUE, OBJECT_NAMED_NUMBER -> {
        if (!Objects.equals(wtx.getNumberValue(), source.getNumberValue())) {
          wtx.setNumberValue(source.getNumberValue());
        }
      }
      case STRING_VALUE, OBJECT_NAMED_STRING -> {
        if (!Objects.equals(wtx.getValue(), source.getValue())) {
          wtx.setStringValue(source.getValue());
        }
      }
      default -> {
      }
    }
  }

  private void replay(final JsonArray operations, final JsonNodeReadOnlyTrx previousRevision,
      final JsonNodeReadOnlyTrx source) {
    final LongSet retainedKeys = JsonDiffSidecar.retainedNodeKeys(operations, previousRevision);
    final var roots = new LongOpenHashSet(operations.size());
    final var placements = new ArrayList<JsonObject>(operations.size());
    long temporaryObject = Fixed.NULL_NODE_KEY.getStandardProperty();
    long temporaryArray = Fixed.NULL_NODE_KEY.getStandardProperty();
    for (final var operation : operations) {
      final JsonObject object = operation.getAsJsonObject();
      if (object.has(INSERT)) {
        roots.add(object.getAsJsonObject(INSERT).get("nodeKey").getAsLong());
        placements.add(object);
      } else if (object.has(DELETE)) {
        final long key = object.getAsJsonObject(DELETE).get("nodeKey").getAsLong();
        if (previousRevision.moveTo(key)) {
          final NodeKind kind = previousRevision.getKind();
          if (kind == NodeKind.OBJECT || kind == NodeKind.OBJECT_NAMED_OBJECT) {
            temporaryObject = key;
          } else if (kind == NodeKind.ARRAY || kind == NodeKind.OBJECT_NAMED_ARRAY) {
            temporaryArray = key;
          }
        }
      }
    }
    allocateFragments(placements, roots, retainedKeys, source, temporaryObject, temporaryArray);
    JsonDiffSerializer.orderMoves(placements, source);
    for (final var placement : placements) {
      executeMove(placement.getAsJsonObject(INSERT), source);
    }
    for (final var operation : operations) {
      final JsonObject object = operation.getAsJsonObject();
      if (object.has(UPDATE)) {
        executeUpdate(object.getAsJsonObject(UPDATE), source);
      }
    }
    for (final var operation : operations) {
      final JsonObject object = operation.getAsJsonObject();
      if (object.has(DELETE)) {
        final long key = object.getAsJsonObject(DELETE).get("nodeKey").getAsLong();
        if (!retainedKeys.contains(key)) {
          executeDelete(key);
        }
      }
    }
    preserveAllocationFrontier(source);
  }

  private void preserveAllocationFrontier(final JsonNodeReadOnlyTrx source) {
    final long unusedKeys = source.getMaxNodeKey() - wtx.getMaxNodeKey();
    if (unusedKeys > 0) {
      wtx.getStorageEngineWriter().getActualRevisionRootPage().reserveKeyRangeInDocumentIndex(unusedKeys);
    }
  }

  private void allocateFragments(final List<JsonObject> placements, final LongSet roots, final LongSet retainedKeys,
      final JsonNodeReadOnlyTrx source, final long temporaryObject, final long temporaryArray) {
    final var fragments = createFragmentCursors(placements, roots, retainedKeys, source);
    Long2LongOpenHashMap temporaryParents = null;
    final var ancestorPath = new LongArrayList();
    while (!fragments.isEmpty()) {
      final FragmentCursor fragment = fragments.remove();
      final long key = fragment.key;
      requireMove(source.moveTo(key), "allocation source", key);
      final NodeKind kind = source.getKind();
      long parent = source.getParentKey();
      while (parent != Fixed.NULL_NODE_KEY.getStandardProperty()) {
        if (wtx.moveTo(parent) && compatibleParent(kind, wtx.getKind())) {
          break;
        }
        if (temporaryParents == null) {
          temporaryParents = new Long2LongOpenHashMap();
          temporaryParents.defaultReturnValue(Fixed.NULL_NODE_KEY.getStandardProperty());
        }
        final long cacheKey = kind.playsObjectKeyRole()
            ? parent
            : -parent - 1;
        final long cached = temporaryParents.get(cacheKey);
        if (cached != Fixed.NULL_NODE_KEY.getStandardProperty()) {
          parent = cached;
          requireMove(wtx.moveTo(parent), "cached allocation parent", parent);
          break;
        }
        ancestorPath.add(cacheKey);
        requireMove(source.moveTo(parent), "allocation ancestor", parent);
        parent = source.getParentKey();
      }
      if (parent == Fixed.NULL_NODE_KEY.getStandardProperty()) {
        parent = kind.playsObjectKeyRole()
            ? temporaryObject
            : temporaryArray;
        requireMove(wtx.moveTo(parent), "temporary allocation parent", parent);
      }
      if (temporaryParents != null) {
        for (int index = 0; index < ancestorPath.size(); index++) {
          temporaryParents.put(ancestorPath.getLong(index), parent);
        }
        ancestorPath.clear();
      }
      copyAllocatedNode(source, key);
      if (fragment.advance()) {
        fragments.add(fragment);
      }
    }
  }

  private PriorityQueue<FragmentCursor> createFragmentCursors(final List<JsonObject> placements, final LongSet roots,
      final LongSet retainedKeys, final JsonNodeReadOnlyTrx source) {
    final var fragments = new PriorityQueue<FragmentCursor>(Math.max(1, placements.size()),
        Comparator.comparingLong(fragment -> fragment.key));
    for (final var placement : placements) {
      final long key = placement.getAsJsonObject(INSERT).get("nodeKey").getAsLong();
      if (!retainedKeys.contains(key)) {
        requireMove(source.moveTo(key), "fragment source", key);
        final var cursor = new FragmentCursor(copyAxis(source, roots, key), roots, key);
        if (cursor.advance()) {
          fragments.add(cursor);
        }
      }
    }
    return fragments;
  }

  private void copyAllocatedNode(final JsonNodeReadOnlyTrx source, final long key) {
    requireMove(source.moveTo(key), "allocation source", key);
    final InsertPosition position;
    if (!wtx.isDocumentRoot() && wtx.hasLastChild()) {
      wtx.moveToLastChild();
      position = InsertPosition.AS_RIGHT_SIBLING;
    } else {
      position = InsertPosition.AS_FIRST_CHILD;
    }
    if (key <= wtx.getMaxNodeKey()) {
      throw new IllegalStateException("JSON revision copy already allocated node " + key);
    }
    wtx.copyNodeWithKey(source, position);
  }

  private static boolean compatibleParent(final NodeKind kind, final NodeKind parentKind) {
    return kind.playsObjectKeyRole()
        ? parentKind == NodeKind.OBJECT || parentKind == NodeKind.OBJECT_NAMED_OBJECT
        : parentKind == NodeKind.ARRAY || parentKind == NodeKind.OBJECT_NAMED_ARRAY
            || parentKind == NodeKind.JSON_DOCUMENT;
  }

  private static final class FragmentCursor {
    private final Axis axis;
    private final LongSet roots;
    private final long root;
    private long key;

    private FragmentCursor(final Axis axis, final LongSet roots, final long root) {
      this.axis = axis;
      this.roots = roots;
      this.root = root;
    }

    private boolean advance() {
      while (axis.hasNext()) {
        key = axis.nextLong();
        if (key == root || !roots.contains(key)) {
          return true;
        }
      }
      return false;
    }
  }

  private static void requireMove(final boolean moved, final String role, final long nodeKey) {
    if (!moved) {
      throw new IllegalStateException("JSON revision copy cannot resolve " + role + " node " + nodeKey);
    }
  }

  private static Axis copyAxis(final JsonNodeReadOnlyTrx source, final LongSet roots, final long root) {
    return VisitorDescendantAxis.newBuilder(source).includeSelf().visitor(new JsonNodeVisitor() {
      @Override
      public VisitResult visit(final ImmutableArrayNode node) {
        return node.getNodeKey() != root && roots.contains(node.getNodeKey())
            ? VisitResultType.SKIPSUBTREE
            : VisitResultType.CONTINUE;
      }

      @Override
      public VisitResult visit(final ImmutableObjectNode node) {
        return node.getNodeKey() != root && roots.contains(node.getNodeKey())
            ? VisitResultType.SKIPSUBTREE
            : VisitResultType.CONTINUE;
      }

      @Override
      public VisitResult visit(final ObjectNamedArrayNode node) {
        return node.getNodeKey() != root && roots.contains(node.getNodeKey())
            ? VisitResultType.SKIPSUBTREE
            : VisitResultType.CONTINUE;
      }

      @Override
      public VisitResult visit(final ObjectNamedObjectNode node) {
        return node.getNodeKey() != root && roots.contains(node.getNodeKey())
            ? VisitResultType.SKIPSUBTREE
            : VisitResultType.CONTINUE;
      }
    }).build();
  }

  private void insert() {
    final long sourceRoot = rtx.getNodeKey();
    boolean isFirst = true;
    final Axis axis = new DescendantAxis(rtx, IncludeSelf.YES);
    // Iterate over all nodes of the subtree including self.
    while (axis.hasNext()) {
      final long key = axis.nextLong();
      if (rtx.isDocumentRoot()) {
        continue;
      }
      final InsertPosition insertPosition;
      if (isFirst) {
        insertPosition = insert;
      } else {
        final long parentKey = rtx.getParentKey();
        while (!stack.isEmpty() && stack.peekLong(1) != parentKey) {
          stack.popLong();
          stack.popLong();
        }
        requireMove(wtx.moveTo(stack.peekLong(0)), "copy parent", parentKey);
        if (wtx.hasLastChild()) {
          wtx.moveToLastChild();
          insertPosition = InsertPosition.AS_RIGHT_SIBLING;
        } else {
          insertPosition = InsertPosition.AS_FIRST_CHILD;
        }
      }
      // Phase 4: legacy OBJECT_KEY's value child was inserted as part of the OBJECT_KEY pair
      // (so the descendant walk had to skip the value child). With OBJECT_KEY gone, fused
      // OBJECT_NAMED_* records carry the value inline (primitive leaves) or own a real subtree
      // (structural). Children of OBJECT_NAMED_OBJECT are inner fields and MUST be inserted
      // normally — the previous skip-on-parent-OBJECT_KEY guard is no longer needed.
      if (copyAllRevisionsUpToMostRecent) {
        wtx.copyNodeWithKey(rtx, insertPosition);
      } else {
        processNode(wtx, rtx, insertPosition);
      }
      rtx.moveTo(key);

      isFirst = false;

      if (rtx.hasFirstChild()) {
        stack.push(key);
        stack.push(wtx.getNodeKey());
      }
    }
    stack.clear();
    rtx.moveTo(sourceRoot);
  }

  /**
   * Copy the current source node without traversing its children.
   *
   * @param wtx the destination transaction
   * @param rtx Sirix {@link JsonNodeReadOnlyTrx}
   * @param insertPosition insertion position relative to the destination cursor
   */
  public static void processNode(final JsonNodeTrx wtx, final JsonNodeReadOnlyTrx rtx,
      final InsertPosition insertPosition) {
    switch (rtx.getKind()) {
      case JSON_DOCUMENT:
        break;
      case OBJECT:
        if (insertPosition == InsertPosition.AS_FIRST_CHILD) {
          wtx.insertObjectAsFirstChild();
        } else if (insertPosition == InsertPosition.AS_RIGHT_SIBLING) {
          wtx.insertObjectAsRightSibling();
        } else if (insertPosition == InsertPosition.AS_LEFT_SIBLING) {
          wtx.insertObjectAsLeftSibling();
        } else {
          throw new IllegalStateException("Insert location not known!");
        }
        break;
      case ARRAY:
        if (insertPosition == InsertPosition.AS_FIRST_CHILD) {
          wtx.insertArrayAsFirstChild();
        } else if (insertPosition == InsertPosition.AS_RIGHT_SIBLING) {
          wtx.insertArrayAsRightSibling();
        } else if (insertPosition == InsertPosition.AS_LEFT_SIBLING) {
          wtx.insertArrayAsLeftSibling();
        } else {
          throw new IllegalStateException("Insert location not known!");
        }
        break;
      // (Phase 4: legacy OBJECT_KEY case removed — fused records 48-53 carry the field
      // name + inline value/sub-tree on a single slot, handled by the OBJECT_NAMED_*
      // cases below.)
      case BOOLEAN_VALUE:
        if (insertPosition == InsertPosition.AS_FIRST_CHILD) {
          wtx.insertBooleanValueAsFirstChild(rtx.getBooleanValue());
        } else if (insertPosition == InsertPosition.AS_RIGHT_SIBLING) {
          wtx.insertBooleanValueAsRightSibling(rtx.getBooleanValue());
        } else if (insertPosition == InsertPosition.AS_LEFT_SIBLING) {
          wtx.insertBooleanValueAsLeftSibling(rtx.getBooleanValue());
        } else {
          throw new IllegalStateException("Insert location not known!");
        }
        break;
      case NULL_VALUE:
        if (insertPosition == InsertPosition.AS_FIRST_CHILD) {
          wtx.insertNullValueAsFirstChild();
        } else if (insertPosition == InsertPosition.AS_RIGHT_SIBLING) {
          wtx.insertNullValueAsRightSibling();
        } else if (insertPosition == InsertPosition.AS_LEFT_SIBLING) {
          wtx.insertNullValueAsLeftSibling();
        } else {
          throw new IllegalStateException("Insert location not known!");
        }
        break;
      case NUMBER_VALUE:
        if (insertPosition == InsertPosition.AS_FIRST_CHILD) {
          wtx.insertNumberValueAsFirstChild(rtx.getNumberValue());
        } else if (insertPosition == InsertPosition.AS_RIGHT_SIBLING) {
          wtx.insertNumberValueAsRightSibling(rtx.getNumberValue());
        } else if (insertPosition == InsertPosition.AS_LEFT_SIBLING) {
          wtx.insertNumberValueAsLeftSibling(rtx.getNumberValue());
        } else {
          throw new IllegalStateException("Insert location not known!");
        }
        break;
      case STRING_VALUE:
        if (insertPosition == InsertPosition.AS_FIRST_CHILD) {
          wtx.insertStringValueAsFirstChild(rtx.getValue());
        } else if (insertPosition == InsertPosition.AS_RIGHT_SIBLING) {
          wtx.insertStringValueAsRightSibling(rtx.getValue());
        } else if (insertPosition == InsertPosition.AS_LEFT_SIBLING) {
          wtx.insertStringValueAsLeftSibling(rtx.getValue());
        } else {
          throw new IllegalStateException("Insert location not known!");
        }
        break;
      // iter#32 fusion: OBJECT_NAMED_* records carry both the field name and the inline
      // primitive value. Re-emit as an object record so the destination tree gets the
      // (possibly fused) (key, primitive) pair regardless of fusion-mode toggles.
      case OBJECT_NAMED_BOOLEAN:
      case OBJECT_NAMED_NUMBER:
      case OBJECT_NAMED_STRING:
      case OBJECT_NAMED_NULL: {
        final var key = rtx.getName().getLocalName();
        final ObjectRecordValue<?> value = switch (rtx.getKind()) {
          case OBJECT_NAMED_BOOLEAN -> BooleanValue.of(rtx.getBooleanValue());
          case OBJECT_NAMED_NUMBER -> new NumberValue(rtx.getNumberValue());
          case OBJECT_NAMED_STRING -> new StringValue(rtx.getValue());
          case OBJECT_NAMED_NULL -> NullValue.INSTANCE;
          default -> throw new IllegalStateException("unreachable");
        };
        if (insertPosition == InsertPosition.AS_FIRST_CHILD) {
          wtx.insertObjectRecordAsFirstChild(key, value);
        } else if (insertPosition == InsertPosition.AS_RIGHT_SIBLING) {
          wtx.insertObjectRecordAsRightSibling(key, value);
        } else if (insertPosition == InsertPosition.AS_LEFT_SIBLING) {
          wtx.insertObjectRecordAsLeftSibling(key, value);
        } else {
          throw new IllegalStateException("Insert location not known!");
        }
        break;
      }
      // P2 fusion: OBJECT_NAMED_OBJECT/ARRAY records carry the field name + the start of the
      // structural value. Re-emit through insertObjectRecordAsXxx with ObjectValue/ArrayValue —
      // the destination will fuse if the target supports it. If the destination cursor sits in
      // a context that cannot accept a named field (the JSON_DOCUMENT root, an array, or the
      // fused ARRAY-equivalent), drop the field name and emit a plain OBJECT/ARRAY instead so
      // the structural copy still completes.
      case OBJECT_NAMED_OBJECT:
      case OBJECT_NAMED_ARRAY: {
        final NodeKind anchorKind = insertPosition == InsertPosition.AS_FIRST_CHILD
            ? wtx.getKind()
            : wtx.getParentKind();
        final boolean anchorAcceptsNamedField =
            anchorKind == NodeKind.OBJECT || anchorKind == NodeKind.OBJECT_NAMED_OBJECT;

        if (!anchorAcceptsNamedField) {
          if (rtx.getKind() == NodeKind.OBJECT_NAMED_OBJECT) {
            if (insertPosition == InsertPosition.AS_FIRST_CHILD) {
              wtx.insertObjectAsFirstChild();
            } else if (insertPosition == InsertPosition.AS_RIGHT_SIBLING) {
              wtx.insertObjectAsRightSibling();
            } else if (insertPosition == InsertPosition.AS_LEFT_SIBLING) {
              wtx.insertObjectAsLeftSibling();
            } else {
              throw new IllegalStateException("Insert location not known!");
            }
          } else {
            if (insertPosition == InsertPosition.AS_FIRST_CHILD) {
              wtx.insertArrayAsFirstChild();
            } else if (insertPosition == InsertPosition.AS_RIGHT_SIBLING) {
              wtx.insertArrayAsRightSibling();
            } else if (insertPosition == InsertPosition.AS_LEFT_SIBLING) {
              wtx.insertArrayAsLeftSibling();
            } else {
              throw new IllegalStateException("Insert location not known!");
            }
          }
          break;
        }

        final var key = rtx.getName().getLocalName();
        final ObjectRecordValue<?> value = rtx.getKind() == NodeKind.OBJECT_NAMED_OBJECT
            ? ObjectValue.INSTANCE
            : ArrayValue.INSTANCE;
        if (insertPosition == InsertPosition.AS_FIRST_CHILD) {
          wtx.insertObjectRecordAsFirstChild(key, value);
        } else if (insertPosition == InsertPosition.AS_RIGHT_SIBLING) {
          wtx.insertObjectRecordAsRightSibling(key, value);
        } else if (insertPosition == InsertPosition.AS_LEFT_SIBLING) {
          wtx.insertObjectRecordAsLeftSibling(key, value);
        } else {
          throw new IllegalStateException("Insert location not known!");
        }
        break;
      }
      // $CASES-OMITTED$
      default:
        throw new IllegalStateException("Node kind not known!");
    }
  }
}
