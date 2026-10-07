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

import io.sirix.access.trx.node.json.InternalJsonNodeTrx;
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
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.IncludeSelf;
import io.sirix.node.NodeKind;
import io.sirix.service.InsertPosition;
import io.sirix.service.ShredderCommit;
import io.sirix.service.json.replay.JsonIdentityDeltaReader;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.concurrent.Callable;

import static java.util.Objects.requireNonNull;

/**
 * Copy an allocating subtree snapshot or replicate complete resource revisions by persistent node
 * identity. Presentation diffs are independent of the history import protocol.
 */
public final class JsonResourceCopy implements Callable<Void> {

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
     * revision. Requires a fresh destination document, matching resource configuration and the source
     * document (or its sole top-level value) at AS_FIRST_CHILD. Keys, topology, allocation frontier and
     * stored Dewey IDs are preserved. A history suffix maps its initial source revision to destination
     * revision one. Snapshot-only copying allocates destination keys normally.
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
   * Parent pairs with the destination key above the source key; snapshot copies may allocate
   * different destination keys.
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

  @Override
  public Void call() {
    requireMove(rtx.moveTo(startNodeKey), "copy source", startNodeKey);
    if (copyAllRevisionsUpToMostRecent) {
      copyRevisionHistory();
    } else {
      insert();
      commit.commit(wtx);
    }
    return null;
  }

  private void copyRevisionHistory() {
    if (!(wtx instanceof final InternalJsonNodeTrx importer)) {
      throw new IllegalArgumentException("Identity history copy requires an internal JSON transaction");
    }
    if (insert != InsertPosition.AS_FIRST_CHILD || !wtx.isDocumentRoot()
        || (!rtx.isDocumentRoot() && rtx.getParentKey() != 0)) {
      throw new IllegalArgumentException(
          "Identity history copy requires a complete source document and destination root");
    }
    if (wtx.getRevisionNumber() != 1 || wtx.getMaxNodeKey() != 0 || wtx.hasFirstChild()) {
      throw new IllegalArgumentException("Identity history copy requires a fresh destination");
    }
    final int firstRevision = rtx.getRevisionNumber();
    final int lastRevision = readResourceSession.getMostRecentRevisionNumber();
    importer.importRevision(JsonIdentityDeltaReader.snapshot(rtx, 1), rtx);
    for (int revision = firstRevision + 1; revision <= lastRevision; revision++) {
      try (final var source = readResourceSession.beginNodeReadOnlyTrx(revision);
          final var previous = readResourceSession.beginNodeReadOnlyTrx(revision - 1)) {
        importer.importRevision(JsonIdentityDeltaReader.between(previous, source, revision - firstRevision + 1),
            source);
      }
    }
  }

  private static void requireMove(final boolean moved, final String role, final long nodeKey) {
    if (!moved) {
      throw new IllegalStateException("Cannot resolve " + role + " node " + nodeKey);
    }
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
      processNode(wtx, rtx, insertPosition);
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
