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

package io.sirix.service.json.serialize;

import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.node.NodeKind;
import io.sirix.settings.Fixed;
import io.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Callable;

import static java.util.Objects.requireNonNull;

/**
 * <p>
 * A self-contained JSON serializer that handles maxLevel and maxChildren limits directly within the
 * traversal loop, without relying on the visitor pattern.
 * </p>
 * 
 * <h2>Algorithm Invariants</h2>
 * <ul>
 * <li>Level counting: Root node is at level 1. Children are at level 2, etc.</li>
 * <li>ObjectKey nodes do NOT increment level (key and value are at the same level)</li>
 * <li>maxChildren is checked BEFORE emitting each child (pre-check, not post-check)</li>
 * <li>maxLevel is checked BEFORE descending into children</li>
 * </ul>
 * 
 * <h2>Formal Correctness</h2>
 * <p>
 * For a node n to be emitted:
 * <ol>
 * <li>L(n) ≤ maxLevel + 1 (where L is effective level)</li>
 * <li>S(n) ≤ maxChildren (where S is sibling index among non-ObjectKey siblings)</li>
 * <li>All ancestors of n satisfy (1) and (2)</li>
 * </ol>
 * </p>
 */
public final class JsonLimitedSerializer implements Callable<Void> {

  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory.getLogger(JsonLimitedSerializer.class));

  // ═══════════════════════════════════════════════════════════════════
  // Configuration (from Builder)
  // ═══════════════════════════════════════════════════════════════════
  private final JsonResourceSession session;
  private final Appendable out;
  private final long startNodeKey;
  private final int maxLevel; // 0 = unlimited
  private final int maxChildren; // 0 = unlimited
  private final long maxNodes; // 0 = unlimited
  private final int[] revisions;
  private final boolean indent;
  private final int indentSpaces;
  private final boolean withMetaData;
  private final boolean withNodeKeyMetaData;
  private final boolean withNodeKeyAndChildCountMetaData;
  private final boolean serializeTimestamp;
  private final boolean serializeStartNodeWithBrackets;
  private final boolean emitXQueryResultSequence;

  // ═══════════════════════════════════════════════════════════════════
  // Traversal State
  // ═══════════════════════════════════════════════════════════════════
  private int currentLevel;
  private int currentIndent;
  private boolean hadToAddBracket;
  private long nodeCount; // Count of nodes emitted (for maxNodes limit)

  /**
   * Private constructor - use Builder to create instances.
   */
  private JsonLimitedSerializer(final Builder builder) {
    this.session = requireNonNull(builder.resourceMgr);
    this.out = requireNonNull(builder.stream);
    this.startNodeKey = builder.startNodeKey;
    this.maxLevel = builder.maxLevel;
    this.maxChildren = builder.maxChildren;
    this.maxNodes = builder.maxNodes;
    this.revisions = builder.revisions;
    this.indent = builder.indent;
    this.indentSpaces = builder.indentSpaces;
    this.withMetaData = builder.withMetaData;
    this.withNodeKeyMetaData = builder.withNodeKey;
    this.withNodeKeyAndChildCountMetaData = builder.withNodeKeyAndChildCount;
    this.serializeTimestamp = builder.serializeTimestamp;
    this.serializeStartNodeWithBrackets = builder.serializeStartNodeWithBrackets;
    this.emitXQueryResultSequence = builder.emitXQueryResultSequence;
  }

  // ═══════════════════════════════════════════════════════════════════
  // Main Entry Point
  // ═══════════════════════════════════════════════════════════════════

  @Override
  public Void call() {
    try {
      emitStartDocument();

      for (int revision : revisions) {
        try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
          emitRevisionStartNode(rtx);

          if (!rtx.moveTo(startNodeKey)) {
            // startNodeKey points to a non-existent node - emit empty output and continue
            LOGWRAPPER.debug("moveTo failed for startNodeKey {}, node does not exist", startNodeKey);
            emitRevisionEndNode(rtx);
            continue;
          }

          // Handle JSON_DOCUMENT node specially - move to first child
          if (rtx.getKind() == NodeKind.JSON_DOCUMENT) {
            if (rtx.hasFirstChild()) {
              rtx.moveToFirstChild();
            } else {
              // Empty document
              continue;
            }
          }

          serializeSubtree(rtx);

          emitRevisionEndNode(rtx);
        }
      }

      emitEndDocument();
    } catch (IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
      throw new RuntimeException(e);
    }
    return null;
  }

  // ═══════════════════════════════════════════════════════════════════
  // Core Traversal Algorithm
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Serialize the subtree starting at the current node.
   * 
   * <p>
   * <b>Invariant:</b> currentLevel is set before calling this method.
   * </p>
   */
  private void serializeSubtree(JsonNodeReadOnlyTrx rtx) throws IOException {
    currentLevel = 1;

    // Initialize node count based on start node type (Scheme B):
    // - Root (non-ObjectKey start): counts as 1
    // - ObjectKey start: ObjectKey + value together = 2 (both counted upfront)
    if (rtx.isObjectKey()) {
      nodeCount = 2; // ObjectKey (1) + its value (1) = 2
    } else {
      nodeCount = 1; // Root counts as 1
    }

    // Emit the start node
    boolean willVisitChildren = shouldVisitChildren(rtx) && !hasExceededNodeLimit();
    emitNode(rtx, willVisitChildren);

    if (willVisitChildren) {
      visitChildren(rtx); // Return value ignored at top level
      emitEndNode(rtx, true);
    }
  }

  /**
   * Visit and serialize children of the current node.
   * 
   * <p>
   * <b>Pre-condition:</b> rtx is positioned at a node with children.
   * </p>
   * <p>
   * <b>Post-condition:</b> rtx is repositioned back to the parent.
   * </p>
   * 
   * @param rtx the read-only transaction positioned at the parent
   * @return true if we should terminate (maxNodes exceeded), false otherwise
   */
  private boolean visitChildren(JsonNodeReadOnlyTrx rtx) throws IOException {
    if (!rtx.hasFirstChild()) {
      return false;
    }

    final long parentKey = rtx.getNodeKey();
    rtx.moveToFirstChild();

    int childIndex = 0;
    boolean shouldTerminate = false;

    boolean isFirstChild = true;

    do {
      // Count non-ObjectKey-value siblings for maxChildren check
      // ObjectKey values don't count as separate children (they're part of their ObjectKey parent)
      boolean isValueOfObjectKey = isObjectKeyValue(rtx);
      if (!isValueOfObjectKey) {
        childIndex++;
      }

      // Check maxChildren limit BEFORE visiting (pre-check)
      if (maxChildren > 0 && childIndex > maxChildren) {
        break; // Stop visiting siblings
      }

      // For maxNodes counting:
      // ObjectKey + value together count as 2
      // Children of containers (Array/Object) count as 1 each
      boolean isObjectKey = rtx.isObjectKey();

      // For ObjectKeys: look ahead and count ObjectKey + value as 2
      if (isObjectKey && maxNodes > 0) {
        // ObjectKey + value = 2 nodes
        if (nodeCount + 2 > maxNodes) {
          shouldTerminate = true;
          break; // Don't emit this ObjectKey since there's no room for it + value
        }
        nodeCount += 2; // Count both together (ObjectKey = 1, value = 1)
      }

      // Non-ObjectKey nodes that are NOT ObjectKey values: count as 1
      // ObjectKey values were pre-counted with their ObjectKey parent above
      if (!isObjectKey && !isValueOfObjectKey) {
        if (maxNodes > 0) {
          // Pre-check (consistent with ObjectKey handling):
          // with maxNodes=N we must still emit the node that makes nodeCount == N.
          if (nodeCount + 1 > maxNodes) {
            shouldTerminate = true;
            break;
          }
        }
        nodeCount++;
      }

      // Print comma before each sibling (except the first and ObjectKey values)
      if (!isFirstChild && !isValueOfObjectKey) {
        appendSeparator();
      }
      isFirstChild = false;

      // Determine if this node increments level
      // When entering children, level normally increases
      // EXCEPT: ObjectKey's VALUE does NOT increment level (it's at the same level as the key)
      // ObjectKey itself DOES increment level (it's a child of the parent container)
      boolean incrementsLevel = !isValueOfObjectKey;

      if (incrementsLevel) {
        currentLevel++;
      }

      // Check if we should visit this node's children
      boolean willVisitChildren = shouldVisitChildren(rtx);

      // Emit the node (without commas - we handle them above)
      emitNodeWithoutTrailingComma(rtx, willVisitChildren);

      // Recursively visit children if allowed (and not terminated)
      if (willVisitChildren && !shouldTerminate) {
        final long currentKey = rtx.getNodeKey();
        boolean childTerminated = visitChildren(rtx);
        rtx.moveTo(currentKey);

        // Propagate termination from children
        if (childTerminated) {
          shouldTerminate = true;
        }

        // Determine if this is the last child we'll emit
        boolean isLastEmittedChild =
            !rtx.hasRightSibling() || (maxChildren > 0 && childIndex >= maxChildren) || shouldTerminate;
        emitEndNode(rtx, isLastEmittedChild);
      } else if (willVisitChildren) {
        // We were going to visit children but terminated - still need to emit end node
        emitEndNode(rtx, true);
      }

      // Stop if we should terminate
      if (shouldTerminate) {
        break;
      }

      // Restore level after visiting
      if (incrementsLevel) {
        currentLevel--;
      }

    } while (rtx.hasRightSibling() && rtx.moveToRightSibling());

    // Return to parent
    rtx.moveTo(parentKey);
    return shouldTerminate;
  }

  /**
   * Check if we've exceeded the maxNodes limit. With maxNodes=N, we should emit exactly N nodes (not
   * counting ObjectKeys). The check happens AFTER incrementing nodeCount and emitting the node.
   */
  private boolean hasExceededNodeLimit() {
    if (maxNodes <= 0) {
      return false; // No limit
    }
    // nodeCount > maxNodes means we've exceeded the limit
    return nodeCount > maxNodes;
  }

  /**
   * Check if the current node is the value child of a legacy OBJECT_KEY. ObjectKey values don't
   * count as separate children for maxChildren and don't increment level.
   * <p>
   * iter#32 P2 / Phase 4: legacy OBJECT_KEY has been DELETED (see {@link NodeKind} comments).
   * Under structural fusion, a record can no longer be a "single value of an OBJECT_KEY":
   * <ul>
   *   <li>Fused OBJECT_NAMED_OBJECT / OBJECT_NAMED_ARRAY parents are structural containers; their
   *       children are ARRAY elements or OBJECT_KEY-shaped fused siblings. Commas MUST be emitted
   *       between them, and they DO occupy a deeper level slot.</li>
   *   <li>Fused OBJECT_NAMED_BOOLEAN/NUMBER/STRING/NULL records carry both the field name and the
   *       inline primitive — they ARE leaves, no separate value child exists.</li>
   * </ul>
   * The legacy OBJECT_KEY → primitive-VALUE pair simply does not occur on disk anymore, so this
   * check can never fire and we keep the predicate as a hard {@code false} sentinel rather than
   * a stale {@code OBJECT_NAMED_OBJECT}-based path that produced the nested-object collapse bug.
   */
  private boolean isObjectKeyValue(JsonNodeReadOnlyTrx rtx) {
    return false;
  }

  /**
   * Determine if we should visit the children of the current node.
   *
   * <p>iter#32 P2 / Phase 4: legacy OBJECT_KEY (whose single VALUE_* child sat at the same level
   * as the key) has been deleted. The remaining containers — OBJECT, ARRAY, OBJECT_NAMED_OBJECT,
   * OBJECT_NAMED_ARRAY — all hold children at {@code currentLevel + 1}, so the rule is uniform:
   *
   * <ul>
   *   <li>No children → false</li>
   *   <li>{@code maxLevel} unset (== 0) → visit</li>
   *   <li>Children fit within the limit ({@code currentLevel < maxLevel}) → visit</li>
   *   <li>Otherwise → suppress descent (the structural close-branch in {@link #emitNode} will
   *       have already collapsed the bracket pair to {@code {}} / {@code []}).</li>
   * </ul>
   */
  private boolean shouldVisitChildren(JsonNodeReadOnlyTrx rtx) {
    if (!rtx.hasFirstChild()) {
      return false;
    }

    // Children would be at currentLevel + 1; allow them only if that level still fits.
    if (maxLevel > 0 && currentLevel >= maxLevel) {
      return false;
    }

    return true;
  }

  // ═══════════════════════════════════════════════════════════════════
  // Emission Methods
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Emit a node without trailing comma (comma handled by caller).
   */
  private void emitNodeWithoutTrailingComma(JsonNodeReadOnlyTrx rtx, boolean willVisitChildren) throws IOException {
    emitNode(rtx, willVisitChildren, false);
  }

  /**
   * Emit a node.
   * 
   * @param rtx the transaction positioned at the node
   * @param willVisitChildren whether children will be visited (affects bracket emission)
   */
  private void emitNode(JsonNodeReadOnlyTrx rtx, boolean willVisitChildren) throws IOException {
    emitNode(rtx, willVisitChildren, true);
  }

  /**
   * Emit a node.
   * 
   * @param rtx the transaction positioned at the node
   * @param willVisitChildren whether children will be visited (affects bracket emission)
   * @param printTrailingComma whether to print comma if there's a right sibling
   */
  private void emitNode(JsonNodeReadOnlyTrx rtx, boolean willVisitChildren, boolean printTrailingComma)
      throws IOException {
    switch (rtx.getKind()) {
      case JSON_DOCUMENT:
        // Skip document node
        break;

      case OBJECT:
        emitMetaData(rtx);
        if (withMetaDataField() && willVisitChildren) {
          appendArrayStart(true);
        }
        // Always emit objectStart - for metadata+children it wraps ALL ObjectKey entries
        appendObjectStart(willVisitChildren);
        if (!willVisitChildren) {
          appendObjectEnd(false);
          if (withMetaDataField()) {
            appendObjectEnd(true);
          }
          if (printTrailingComma)
            printCommaIfNeeded(rtx);
        }
        break;

      case ARRAY:
        emitMetaData(rtx);
        appendArrayStart(willVisitChildren);
        if (!willVisitChildren) {
          appendArrayEnd(false);
          if (withMetaDataField()) {
            appendObjectEnd(true);
          }
          if (printTrailingComma)
            printCommaIfNeeded(rtx);
        }
        break;

      case OBJECT_NAMED_OBJECT:
      case OBJECT_NAMED_ARRAY: {
        // P2 fused-structural emission: a single OBJECT_NAMED_OBJECT/ARRAY record carries both
        // the field name and the start of the structural value. Mirror the legacy two-emit path:
        //   - OBJECT_KEY pre-emit:   "<name>":
        //   - OBJECT pre-emit:       {  (or [ for ARRAY)
        // On emitEndNode this same record emits the corresponding `}` / `]`.
        final boolean innerHasChildren = rtx.hasFirstChild();
        final boolean isNamedObject = rtx.getKind() == NodeKind.OBJECT_NAMED_OBJECT;

        if (startNodeKey != Fixed.NULL_NODE_KEY.getStandardProperty() && rtx.getNodeKey() == startNodeKey
            && serializeStartNodeWithBrackets) {
          appendObjectStart(innerHasChildren);
          hadToAddBracket = true;
        }

        if (withMetaDataField()) {
          if (rtx.hasLeftSibling()
              && !(startNodeKey != Fixed.NULL_NODE_KEY.getStandardProperty() && rtx.getNodeKey() == startNodeKey)) {
            appendObjectStart(true);
          }
          appendObjectKeyValue(quote("key"), quote(StringValue.escape(rtx.getName().stringValue()))).appendSeparator()
              .appendObjectKey(quote("metadata"))
              .appendObjectStart(true);
          if (withNodeKeyMetaData || withNodeKeyAndChildCountMetaData) {
            appendObjectKeyValue(quote("nodeKey"), String.valueOf(rtx.getNodeKey()));
            // Mirror legacy OBJECT/ARRAY emitMetaData: emit the comma after nodeKey if any
            // further metadata field follows (hash/type/descendantCount via withMetaData, or
            // childCount via withNodeKeyAndChildCountMetaData). Without this, fused-structural
            // records emit `"nodeKey":N"childCount":M` without a separator.
            if (withMetaData || withNodeKeyAndChildCountMetaData) {
              appendSeparator();
            }
          }
          if (withMetaData) {
            if (rtx.getHash() != 0L) {
              appendObjectKeyValue(quote("hash"), quote(printHashValue(rtx)));
              appendSeparator();
            }
            // Present fused structural record externally as OBJECT_KEY so downstream
            // consumers keep the legacy OBJECT_KEY-shaped contract. Emit the literal string
            // {@code "OBJECT_KEY"} (the legacy NodeKind name) instead of the new fused enum
            // name — fixtures and external callers still see the historical metadata shape.
            appendObjectKeyValue(quote("type"), quote("OBJECT_KEY"));
            // Mirror legacy OBJECT_KEY: emit descendantCount when a hash is present.
            // Fused record's descendantCount equals the inner OBJECT/ARRAY's descendantCount —
            // the fusion collapses one OBJECT_KEY level so the count drops by 1 vs legacy.
            if (rtx.getHash() != 0L) {
              appendSeparator().appendObjectKeyValue(quote("descendantCount"),
                  String.valueOf(rtx.getDescendantCount()));
            }
          }
          // Fused structural record carries the inner OBJECT/ARRAY's childCount: emit it when
          // childCount-metadata is requested (mirrors the legacy OBJECT/ARRAY emitMetaData).
          if (withNodeKeyAndChildCountMetaData) {
            if (withMetaData) {
              appendSeparator();
            }
            appendObjectKeyValue(quote("childCount"), String.valueOf(rtx.getChildCount()));
          }
          appendObjectEnd(innerHasChildren).appendSeparator();
          appendObjectKey(quote("value"));
        } else {
          appendObjectKey(quote(StringValue.escape(rtx.getName().stringValue())));
        }

        // In metadata mode a named OBJECT whose children are actually visited renders them as an
        // array of child records (`[{...}]`, matching the unbounded JsonSerializer). When the
        // children are pruned by a limit (maxLevel/maxNodes/maxChildren) or there are none, the
        // value is the bare empty placeholder object `{}` — which the client reads as "children
        // not loaded". A named ARRAY always opens `[`. Without metadata the value is bare.
        final boolean wrapNamedObjectChildren =
            isNamedObject && withMetaDataField() && willVisitChildren && innerHasChildren;
        if (wrapNamedObjectChildren) {
          appendArrayStart(true);
        }
        if (isNamedObject) {
          appendObjectStart(willVisitChildren && innerHasChildren);
        } else {
          appendArrayStart(willVisitChildren && innerHasChildren);
        }

        if (!innerHasChildren || !willVisitChildren) {
          if (isNamedObject) {
            appendObjectEnd(false);
          } else {
            appendArrayEnd(false);
          }
          // Close shape mirrors emitEndNode (above): close the metadata-wrapper `}` when it
          // exists (either non-startNode siblings, or startNode with hadToAddBracket).
          final boolean isStartNode =
              startNodeKey != Fixed.NULL_NODE_KEY.getStandardProperty() && rtx.getNodeKey() == startNodeKey;
          if (withMetaDataField()) {
            if (!isStartNode || hadToAddBracket) {
              appendObjectEnd(true);
            }
          } else if (hadToAddBracket && isStartNode) {
            appendObjectEnd(false);
          }
          if (printTrailingComma) {
            printCommaIfNeeded(rtx);
          }
        }
        break;
      }

      case OBJECT_NAMED_BOOLEAN:
      case OBJECT_NAMED_NUMBER:
      case OBJECT_NAMED_STRING:
      case OBJECT_NAMED_NULL:
        // iter#30: fused OBJECT_NAMED_* — emit as if it were OBJECT_KEY + primitive-value.
        // Per-record `{` is opened by the parent OBJECT body's `appendObjectStart` for the
        // FIRST child, and by this case for subsequent siblings. Fused records are leaves,
        // so emitEndNode is not invoked for them — close `}` + emit the inter-sibling `,` here.
        if (withMetaDataField()) {
          if (rtx.hasLeftSibling()
              && !(startNodeKey != Fixed.NULL_NODE_KEY.getStandardProperty() && rtx.getNodeKey() == startNodeKey)) {
            appendObjectStart(true);
          }
          appendObjectKeyValue(quote("key"), quote(StringValue.escape(rtx.getName().stringValue()))).appendSeparator()
              .appendObjectKey(quote("metadata"))
              .appendObjectStart(true);
          if (withNodeKeyMetaData || withNodeKeyAndChildCountMetaData) {
            appendObjectKeyValue(quote("nodeKey"), String.valueOf(rtx.getNodeKey()));
          }
          if (withMetaData) {
            appendSeparator();
            if (rtx.getHash() != 0L) {
              appendObjectKeyValue(quote("hash"), quote(printHashValue(rtx)));
              appendSeparator();
            }
            // Present fused leaves externally as OBJECT_KEY so downstream consumers (and the
            // pagination-style fixtures used by JsonLimitedSerializer / JsonRecordSerializer)
            // keep the legacy OBJECT_KEY-shaped envelope. The unbounded {@link JsonSerializer}
            // path keeps the precise fused leaf kind for its own metadata fixture.
            appendObjectKeyValue(quote("type"), quote("OBJECT_KEY"));
          }
          appendObjectEnd(true).appendSeparator();
          appendObjectKey(quote("value"));
        } else {
          appendObjectKey(quote(StringValue.escape(rtx.getName().stringValue())));
        }
        // Now emit the primitive value. Use getValue() / dispatch by kind.
        switch (rtx.getKind()) {
          case OBJECT_NAMED_BOOLEAN -> appendObjectValue(String.valueOf(rtx.getBooleanValue()));
          case OBJECT_NAMED_NUMBER -> appendObjectValue(String.valueOf(rtx.getNumberValue()));
          case OBJECT_NAMED_STRING -> appendObjectValue(quote(StringValue.escape(rtx.getValue())));
          case OBJECT_NAMED_NULL -> appendObjectValue("null");
          default -> throw new IllegalStateException("unexpected fused kind: " + rtx.getKind());
        }
        // Close the per-record `{` so subsequent commas land outside the wrapper.
        if (withMetaDataField()
            && !(startNodeKey != Fixed.NULL_NODE_KEY.getStandardProperty()
                 && rtx.getNodeKey() == startNodeKey)) {
          appendObjectEnd(true);
        }
        if (printTrailingComma) {
          printCommaIfNeeded(rtx);
        }
        break;

      // (Phase 4: legacy OBJECT_KEY case removed — fused OBJECT_NAMED_* records emit
      //  through the dedicated cases below.)

      case BOOLEAN_VALUE:
        emitMetaData(rtx);
        appendObjectValue(rtx.getValue());
        if (withMetaDataField()) {
          appendObjectEnd(true);
        }
        if (printTrailingComma)
          printCommaIfNeeded(rtx);
        break;

      case NULL_VALUE:
        emitMetaData(rtx);
        appendObjectValue("null");
        if (withMetaDataField()) {
          appendObjectEnd(true);
        }
        if (printTrailingComma)
          printCommaIfNeeded(rtx);
        break;

      case NUMBER_VALUE:
        emitMetaData(rtx);
        appendObjectValue(rtx.getValue());
        if (withMetaDataField()) {
          appendObjectEnd(true);
        }
        if (printTrailingComma)
          printCommaIfNeeded(rtx);
        break;

      case STRING_VALUE:
        emitMetaData(rtx);
        appendObjectValue(quote(StringValue.escape(rtx.getValue())));
        if (withMetaDataField()) {
          appendObjectEnd(true);
        }
        if (printTrailingComma)
          printCommaIfNeeded(rtx);
        break;

      default:
        throw new IllegalStateException("Node kind not known: " + rtx.getKind());
    }
  }

  /**
   * Emit end tag for a node.
   */
  private void emitEndNode(JsonNodeReadOnlyTrx rtx, boolean isLast) throws IOException {
    switch (rtx.getKind()) {
      case ARRAY -> {
        if (withMetaDataField()) {
          appendArrayEnd(true).appendObjectEnd(true);
        } else {
          appendArrayEnd(true);
        }
        // Don't print comma here - handled by visitChildren BEFORE next sibling
      }
      case OBJECT -> {
        if (withMetaDataField()) {
          appendArrayEnd(true).appendObjectEnd(true);
        } else {
          appendObjectEnd(true);
        }
        // Don't print comma here - handled by visitChildren BEFORE next sibling
      }
      case OBJECT_NAMED_OBJECT -> {
        // P2 fused-structural close: same record emitted `<name>:{` on enter; emit matching `}`.
        // Close shape (with metadata):
        //   - non-startNode: close `}` (record body) then `}` (outer metadata wrapper).
        //   - startNode w/o serializeStartNodeWithBrackets: only close `}` (record body) —
        //     the outer wrapper was supplied by the caller (e.g. JsonRecordSerializer).
        //   - startNode with serializeStartNodeWithBrackets (hadToAddBracket): close `}` (body)
        //     then `}` (the start-node bracket itself, which doubles as the metadata wrapper).
        final boolean isStartNode =
            startNodeKey != Fixed.NULL_NODE_KEY.getStandardProperty() && rtx.getNodeKey() == startNodeKey;
        if (withMetaDataField()) {
          // Close the value array `]` (children live in it), then the metadata wrapper `}`.
          appendArrayEnd(true);
          if (!isStartNode || hadToAddBracket) {
            appendObjectEnd(true);
          }
        } else {
          appendObjectEnd(rtx.hasFirstChild());
          if (hadToAddBracket && isStartNode) {
            appendObjectEnd(false);
          }
        }
      }
      case OBJECT_NAMED_ARRAY -> {
        // P2 fused-structural close: same record emitted `<name>:[` on enter; emit matching `]`.
        // Same logic as OBJECT_NAMED_OBJECT (above) but with ARRAY body.
        final boolean isStartNode =
            startNodeKey != Fixed.NULL_NODE_KEY.getStandardProperty() && rtx.getNodeKey() == startNodeKey;
        if (withMetaDataField()) {
          appendArrayEnd(true);
          if (!isStartNode || hadToAddBracket) {
            appendObjectEnd(true);
          }
        } else {
          appendArrayEnd(rtx.hasFirstChild());
          if (hadToAddBracket && isStartNode) {
            appendObjectEnd(false);
          }
        }
      }
      // iter#32 fusion: primitive OBJECT_NAMED_* records are leaves; emitEndNode is not invoked
      // for them. The wrapper-close + sibling separator are emitted in emitNode (above) instead.
      default -> {
        // No end tag needed for value nodes
      }
    }
  }

  private void emitMetaData(JsonNodeReadOnlyTrx rtx) throws IOException {
    if (withMetaDataField()) {
      appendObjectStart(true).appendObjectKey(quote("metadata")).appendObjectStart(true);

      if (withNodeKeyMetaData || withNodeKeyAndChildCountMetaData) {
        appendObjectKeyValue(quote("nodeKey"), String.valueOf(rtx.getNodeKey()));
        if (withMetaData || withNodeKeyAndChildCountMetaData
            && (rtx.getKind() == NodeKind.OBJECT || rtx.getKind() == NodeKind.ARRAY)) {
          appendSeparator();
        }
      }

      if (withMetaData) {
        if (rtx.getHash() != 0L) {
          appendObjectKeyValue(quote("hash"), quote(printHashValue(rtx)));
          appendSeparator();
        }
        appendObjectKeyValue(quote("type"), quote(rtx.getKind().toString()));
        if (rtx.getHash() != 0L && (rtx.getKind() == NodeKind.OBJECT || rtx.getKind() == NodeKind.ARRAY)) {
          appendSeparator().appendObjectKeyValue(quote("descendantCount"), String.valueOf(rtx.getDescendantCount()));
        }
      }

      if (withNodeKeyAndChildCountMetaData && (rtx.getKind() == NodeKind.OBJECT || rtx.getKind() == NodeKind.ARRAY)) {
        if (withMetaData) {
          appendSeparator();
        }
        appendObjectKeyValue(quote("childCount"), String.valueOf(rtx.getChildCount()));
      }

      appendObjectEnd(true).appendSeparator().appendObjectKey(quote("value"));
    }
  }

  private void printCommaIfNeeded(JsonNodeReadOnlyTrx rtx, boolean isLastChild) throws IOException {
    if (!isLastChild && rtx.hasRightSibling() && rtx.getNodeKey() != startNodeKey) {
      // Check if we'll continue with more siblings
      if (!isObjectKeyValue(rtx) || rtx.hasRightSibling()) {
        appendSeparator();
      }
    }
  }

  // Overload for backward compatibility - assumes not last child if has right sibling
  private void printCommaIfNeeded(JsonNodeReadOnlyTrx rtx) throws IOException {
    printCommaIfNeeded(rtx, false);
  }

  // ═══════════════════════════════════════════════════════════════════
  // Document Start/End and Revision Handling
  // ═══════════════════════════════════════════════════════════════════

  private void emitStartDocument() throws IOException {
    if (revisions.length > 1) {
      appendObjectStart(true);
      if (indent) {
        // For multi-revision output
      }
      appendObjectKey(quote("sirix"));
      appendArrayStart(true);
    }
  }

  private void emitEndDocument() throws IOException {
    if (revisions.length > 1) {
      appendArrayEnd(true).appendObjectEnd(true);
    }
  }

  private void emitRevisionStartNode(JsonNodeReadOnlyTrx rtx) throws IOException {
    // NOTE: unlike JsonSerializer this has no wrapRevisionResultInObject step — a single fused
    // named-member query result (`.products[0].id`) would serialize as the invalid bare fragment
    // `"revision":"id":"A"`. That is safe ONLY because the sole XQuery-result caller
    // (JsonDBSerializer) sets no maxLevel/maxChildren/maxNodes, so JsonSerializer.call() never
    // delegates here for a result sequence — and the delegation chokepoint now THROWS on that
    // combination. To support it, port the wrap from JsonSerializer#emitRevisionStartNode
    // (+ its start-node bracket suppression) and drop the guard.
    if (emitXQueryResultSequence || revisions.length > 1) {
      appendObjectStart(rtx.hasChildren())
                                          .appendObjectKeyValue(quote("revisionNumber"),
                                              Integer.toString(rtx.getRevisionNumber()))
                                          .appendSeparator();

      if (serializeTimestamp) {
        appendObjectKeyValue(quote("revisionTimestamp"), quote(DateTimeFormatter.ISO_INSTANT.withZone(
            ZoneOffset.UTC).format(rtx.getRevisionTimestamp()))).appendSeparator();
      }

      appendObjectKey(quote("revision"));
    }
  }

  private void emitRevisionEndNode(JsonNodeReadOnlyTrx rtx) throws IOException {
    if (emitXQueryResultSequence || revisions.length > 1) {
      appendObjectEnd(rtx.hasChildren());

      // Check if there are more revisions
      if (hasMoreRevisionsToSerialize(rtx)) {
        appendSeparator();
      }
    }
  }

  private boolean hasMoreRevisionsToSerialize(JsonNodeReadOnlyTrx rtx) {
    return rtx.getRevisionNumber() < revisions[revisions.length - 1] || (revisions.length == 1 && revisions[0] == -1
        && rtx.getRevisionNumber() < rtx.getResourceSession().getMostRecentRevisionNumber());
  }

  // ═══════════════════════════════════════════════════════════════════
  // Helper Methods
  // ═══════════════════════════════════════════════════════════════════

  private boolean withMetaDataField() {
    return withMetaData || withNodeKeyMetaData || withNodeKeyAndChildCountMetaData;
  }

  private String printHashValue(JsonNodeReadOnlyTrx rtx) {
    return String.format("%016x", rtx.getHash());
  }

  private String quote(String value) {
    return "\"" + value + "\"";
  }


  // ═══════════════════════════════════════════════════════════════════
  // Output Formatting
  // ═══════════════════════════════════════════════════════════════════

  private void indent() throws IOException {
    if (indent) {
      for (int i = 0; i < currentIndent; i++) {
        out.append(" ");
      }
    }
  }

  private void newLine() throws IOException {
    if (indent) {
      out.append("\n");
      indent();
    }
  }

  private JsonLimitedSerializer appendObjectStart(boolean hasChildren) throws IOException {
    out.append('{');
    if (hasChildren) {
      currentIndent += indentSpaces;
      newLine();
    }
    return this;
  }

  private JsonLimitedSerializer appendObjectEnd(boolean hasChildren) throws IOException {
    if (hasChildren) {
      currentIndent -= indentSpaces;
      newLine();
    }
    out.append('}');
    return this;
  }

  private JsonLimitedSerializer appendArrayStart(boolean hasChildren) throws IOException {
    out.append('[');
    if (hasChildren) {
      currentIndent += indentSpaces;
      newLine();
    }
    return this;
  }

  private JsonLimitedSerializer appendArrayEnd(boolean hasChildren) throws IOException {
    if (hasChildren) {
      currentIndent -= indentSpaces;
      newLine();
    }
    out.append(']');
    return this;
  }

  private JsonLimitedSerializer appendObjectKey(String key) throws IOException {
    out.append(key);
    if (indent) {
      out.append(": ");
    } else {
      out.append(":");
    }
    return this;
  }

  private void appendObjectValue(String value) throws IOException {
    out.append(value);
  }

  private JsonLimitedSerializer appendObjectKeyValue(String key, String value) throws IOException {
    out.append(key);
    if (indent) {
      out.append(": ");
    } else {
      out.append(":");
    }
    out.append(value);
    return this;
  }

  private JsonLimitedSerializer appendSeparator() throws IOException {
    out.append(',');
    newLine();
    return this;
  }

  // ═══════════════════════════════════════════════════════════════════
  // Builder Pattern
  // ═══════════════════════════════════════════════════════════════════

  public static Builder newBuilder(JsonResourceSession session, Writer stream, int... revisions) {
    return new Builder(session, stream, revisions);
  }

  public static final class Builder {
    private final JsonResourceSession resourceMgr;
    private final Appendable stream;
    private long startNodeKey = 0;
    private int maxLevel = 0; // 0 = unlimited
    private int maxChildren = 0; // 0 = unlimited
    private long maxNodes = 0; // 0 = unlimited
    private int[] revisions;
    private boolean indent = false;
    private int indentSpaces = 2;
    private boolean withMetaData = false;
    private boolean withNodeKey = false;
    private boolean withNodeKeyAndChildCount = false;
    private boolean serializeTimestamp = false;
    private boolean serializeStartNodeWithBrackets = true;
    private boolean emitXQueryResultSequence = false;

    public Builder(JsonResourceSession resourceMgr, Appendable stream, int... revisions) {
      this.resourceMgr = requireNonNull(resourceMgr);
      this.stream = requireNonNull(stream);
      if (revisions == null || revisions.length == 0) {
        this.revisions = new int[] {resourceMgr.getMostRecentRevisionNumber()};
      } else {
        this.revisions = revisions;
      }
    }

    public Builder startNodeKey(long nodeKey) {
      this.startNodeKey = nodeKey;
      return this;
    }

    public Builder maxLevel(int maxLevel) {
      this.maxLevel = maxLevel;
      return this;
    }

    public Builder maxChildren(int maxChildren) {
      this.maxChildren = maxChildren;
      return this;
    }

    public Builder maxNodes(long maxNodes) {
      this.maxNodes = maxNodes;
      return this;
    }

    public Builder prettyPrint() {
      this.indent = true;
      return this;
    }

    public Builder prettyPrintIf(boolean condition) {
      this.indent = condition;
      return this;
    }

    public Builder indentSpaces(int spaces) {
      this.indentSpaces = spaces;
      return this;
    }

    public Builder withMetaData(boolean withMetaData) {
      this.withMetaData = withMetaData;
      if (withMetaData) {
        this.withNodeKey = true;
        this.withNodeKeyAndChildCount = true;
      }
      return this;
    }

    public Builder withNodeKeyMetaData(boolean withNodeKey) {
      this.withNodeKey = withNodeKey;
      return this;
    }

    public Builder withNodeKeyAndChildCountMetaData(boolean withNodeKeyAndChildCount) {
      this.withNodeKeyAndChildCount = withNodeKeyAndChildCount;
      return this;
    }

    public Builder serializeTimestamp(boolean serializeTimestamp) {
      this.serializeTimestamp = serializeTimestamp;
      return this;
    }

    public Builder serializeStartNodeWithBrackets(boolean value) {
      this.serializeStartNodeWithBrackets = value;
      return this;
    }

    public Builder isXQueryResultSequence() {
      this.emitXQueryResultSequence = true;
      return this;
    }

    public Builder isXQueryResultSequenceIf(boolean condition) {
      this.emitXQueryResultSequence = condition;
      return this;
    }

    public Builder revisions(int[] revisions) {
      this.revisions = revisions;
      return this;
    }

    public JsonLimitedSerializer build() {
      return new JsonLimitedSerializer(this);
    }
  }
}
