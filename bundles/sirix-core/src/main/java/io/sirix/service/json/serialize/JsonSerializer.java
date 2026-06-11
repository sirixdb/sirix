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

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.ResourceSession;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.visitor.VisitResultType;
import io.sirix.axis.IncludeSelf;
import io.sirix.node.NodeKind;
import io.sirix.service.AbstractSerializer;
import io.sirix.service.xml.serialize.XmlSerializerProperties;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import io.sirix.utils.LogWrapper;
import io.sirix.utils.SirixFiles;
import io.brackit.query.util.serialize.Serializer;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ConcurrentMap;

import static io.sirix.utils.Preconditions.checkArgument;
import static io.sirix.service.xml.serialize.XmlSerializerProperties.S_INDENT;
import static io.sirix.service.xml.serialize.XmlSerializerProperties.S_INDENT_SPACES;
import static java.util.Objects.requireNonNull;

/**
 * <p>
 * Serializes a subtree into the JSON-format.
 * </p>
 */
public final class JsonSerializer extends AbstractSerializer<JsonNodeReadOnlyTrx, JsonNodeTrx> {

  /**
   * {@link LogWrapper} reference.
   */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory.getLogger(JsonSerializer.class));

  /**
   * OutputStream to write to.
   */
  private final JsonOutputSink out;

  /**
   * Indent output.
   */
  private final boolean indent;

  /**
   * Number of spaces to indent.
   */
  private final int indentSpaces;

  /**
   * Determines if serializing with initial indentation.
   */
  private final boolean withInitialIndent;

  private final boolean emitXQueryResultSequence;

  private final boolean serializeTimestamp;

  /**
   * Set per revision-start when the result node is a single fused named member, so the matching
   * revision-end closes the object we wrapped it in. See {@link #emitRevisionStartNode}.
   */
  private boolean wrapRevisionResultInObject;

  private final boolean withMetaData;

  private final boolean withNodeKeyMetaData;

  private final boolean withNodeKeyAndChildNodeKeyMetaData;

  private final boolean serializeStartNodeWithBrackets;

  private boolean hadToAddBracket;

  private int currentIndent;

  // Store builder values for delegation to JsonLimitedSerializer
  private final long maxLevelLimit;
  private final long maxNodesLimit;
  private final long maxChildNodesLimit;
  private final JsonResourceSession resourceSession;
  private final int[] revisions;

  /**
   * Private constructor.
   *
   * @param resourceMgr resource session to read the resource
   * @param builder builder of the JSON serializer
   */
  private JsonSerializer(final JsonResourceSession resourceMgr, final Builder builder) {
    super(resourceMgr,
        builder.maxLevel == Long.MAX_VALUE && builder.maxNodes == Long.MAX_VALUE
            && builder.maxChildNodes == Long.MAX_VALUE
                ? null
                : new JsonMaxLevelMaxNodesMaxChildNodesVisitor(builder.startNodeKey, IncludeSelf.YES, builder.maxLevel,
                    builder.maxNodes, builder.maxChildNodes),
        builder.startNodeKey, builder.version, builder.versions);
    // Byte sink when an OutputStream target was configured (UTF-8 end-to-end, raw value-byte
    // fast path); otherwise the classic char pipeline behind an unsynchronized chunk buffer
    // (the target is typically a StringWriter whose backing StringBuffer takes a monitor on
    // EVERY append — millions of tiny appends per document). When the configured Appendable
    // IS already a JsonOutputSink (a parent serializer such as JsonRecordSerializer sharing
    // its sink with inner per-record serializers), use it directly: wrapping it in another
    // CharOutputSink would double-buffer and, worse, decode the byte sink's raw-UTF-8 fast
    // path back through chars. Flushing the SHARED sink at the end of an inner call() merely
    // drains its buffer to the target mid-stream — safe, nothing closes the target.
    if (builder.byteStream != null) {
      out = new JsonOutputSink.Utf8OutputSink(builder.byteStream);
    } else if (builder.stream instanceof JsonOutputSink sink) {
      out = sink;
    } else {
      out = new JsonOutputSink.CharOutputSink(builder.stream);
    }
    indent = builder.indent;
    indentSpaces = builder.indentSpaces;
    withInitialIndent = builder.initialIndent;
    emitXQueryResultSequence = builder.emitXQueryResultSequence;
    serializeTimestamp = builder.serializeTimestamp;
    withMetaData = builder.withMetaData;
    withNodeKeyMetaData = builder.withNodeKey;
    withNodeKeyAndChildNodeKeyMetaData = builder.withNodeKeyAndChildCount;
    serializeStartNodeWithBrackets = builder.serializeStartNodeWithBrackets;
    // Store for delegation
    this.maxLevelLimit = builder.maxLevel;
    this.maxNodesLimit = builder.maxNodes;
    this.maxChildNodesLimit = builder.maxChildNodes;
    this.resourceSession = resourceMgr;
    // Reconstruct full revisions array: [version, versions...]
    if (builder.versions != null && builder.versions.length > 0) {
      this.revisions = new int[1 + builder.versions.length];
      this.revisions[0] = builder.version;
      System.arraycopy(builder.versions, 0, this.revisions, 1, builder.versions.length);
    } else {
      this.revisions = new int[] {builder.version};
    }
  }

  /**
   * Override call() to delegate to JsonLimitedSerializer when appropriate. Delegates when maxLevel,
   * maxChildNodes, or maxNodes are set.
   */
  @Override
  public Void call() {
    final Void result;
    try {
        // Delegate to JsonLimitedSerializer if any limit is set
        boolean hasLevelLimit = maxLevelLimit != Long.MAX_VALUE;
        boolean hasChildLimit = maxChildNodesLimit != Long.MAX_VALUE;
        boolean hasNodeLimit = maxNodesLimit != Long.MAX_VALUE;

        if (hasLevelLimit || hasChildLimit || hasNodeLimit) {
          // JsonLimitedSerializer has no wrapRevisionResultInObject step, so a LIMITED XQuery-result
          // serialization would emit invalid JSON for single fused named-member results. No caller
          // combines the two today — fail fast here (the delegation chokepoint) instead of silently
          // producing an invalid document if one ever does.
          if (emitXQueryResultSequence) {
            throw new IllegalStateException(
                "XQuery-result serialization with maxLevel/maxChildren/maxNodes limits is not supported: "
                    + "JsonLimitedSerializer lacks the named-member result wrap (see emitRevisionStartNode).");
          }
          // Use JsonLimitedSerializer for proper limit handling
          JsonLimitedSerializer.Builder limitedBuilder =
              new JsonLimitedSerializer.Builder(resourceSession, out, revisions).startNodeKey(startNodeKey)
                                                                                .maxLevel(hasLevelLimit
                                                                                    ? (int) maxLevelLimit
                                                                                    : 0)
                                                                                .maxChildren(hasChildLimit
                                                                                    ? (int) maxChildNodesLimit
                                                                                    : 0)
                                                                                .maxNodes(hasNodeLimit
                                                                                    ? maxNodesLimit
                                                                                    : 0)
                                                                                .prettyPrintIf(indent)
                                                                                .indentSpaces(indentSpaces)
                                                                                .withMetaData(withMetaData)
                                                                                .withNodeKeyMetaData(withNodeKeyMetaData)
                                                                                .withNodeKeyAndChildCountMetaData(
                                                                                    withNodeKeyAndChildNodeKeyMetaData)
                                                                                .serializeTimestamp(serializeTimestamp)
                                                                                .serializeStartNodeWithBrackets(
                                                                                    serializeStartNodeWithBrackets)
                                                                                .isXQueryResultSequenceIf(
                                                                                    emitXQueryResultSequence);

          result = limitedBuilder.build().call();
        } else {
          // Fall back to original AbstractSerializer behavior
          result = super.call();
        }
    } catch (final RuntimeException | Error e) {
      // Flush what was buffered, but never let a flush failure REPLACE the primary exception.
      try {
        out.flush();
      } catch (final java.io.IOException flushFailure) {
        e.addSuppressed(flushFailure);
      }
      throw e;
    }
    try {
      out.flush();
    } catch (final java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
    return result;
}

  /**
   * Emit node.
   *
   * @param rtx Sirix {@link JsonNodeReadOnlyTrx}
   */
  @Override
  public void emitNode(final JsonNodeReadOnlyTrx rtx) {
    try {
      final var hasChildren = rtx.hasChildren();

      switch (rtx.getKind()) {
        case JSON_DOCUMENT:
          break;
        case OBJECT:
          emitMetaData(rtx);

          if (withMetaDataField() && shouldEmitChildren(hasChildren)) {
            appendArrayStart(true);
          }

          appendObjectStart(shouldEmitChildren(hasChildren));

          if (!hasChildren || (visitor != null && ((!hasToSkipSiblings && currentLevel() + 1 > maxLevel())
              || (hasToSkipSiblings && currentLevel() > maxLevel())))) {
            appendObjectEnd(false);

            if (withMetaDataField()) {
              appendObjectEnd(true);
            }

            printCommaIfNeeded(rtx);
          }
          break;
        case ARRAY:
          emitMetaData(rtx);

          appendArrayStart(shouldEmitChildren(hasChildren));

          if (!hasChildren || (visitor != null && ((!hasToSkipSiblings && currentLevel() + 1 > maxLevel())
              || (hasToSkipSiblings && currentLevel() > maxLevel())))) {
            appendArrayEnd(false);

            if (withMetaDataField()) {
              appendObjectEnd(true);
            }

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
          printCommaIfNeeded(rtx);
          break;
        case NULL_VALUE:
          emitMetaData(rtx);
          appendObjectValue("null");
          if (withMetaDataField()) {
            appendObjectEnd(true);
          }
          printCommaIfNeeded(rtx);
          break;
        case NUMBER_VALUE:
          emitMetaData(rtx);
          appendObjectValue(rtx.getValue());
          if (withMetaDataField()) {
            appendObjectEnd(true);
          }
          printCommaIfNeeded(rtx);
          break;
        case STRING_VALUE:
          emitMetaData(rtx);
          appendStringValue(rtx);
          if (withMetaDataField()) {
            appendObjectEnd(true);
          }
          printCommaIfNeeded(rtx);
          break;
        case OBJECT_NAMED_OBJECT:
        case OBJECT_NAMED_ARRAY: {
          // P2 fused-structural emission: a single OBJECT_NAMED_OBJECT/ARRAY record carries both
          // the field name and the start of the structural value. Mirror the legacy two-emit path:
          //   - OBJECT_KEY pre-emit:   "<name>":
          //   - OBJECT pre-emit:       {  (or [ for ARRAY)
          // On emitEndNode this same record emits the corresponding `}` / `]`.
          final boolean innerHasChildren = rtx.hasChildren();
          final boolean innerEmitChildren = shouldEmitChildren(innerHasChildren);
          final boolean isNamedObject = rtx.getKind() == NodeKind.OBJECT_NAMED_OBJECT;

          if (startNodeKey != Fixed.NULL_NODE_KEY.getStandardProperty() && rtx.getNodeKey() == startNodeKey
              && serializeStartNodeWithBrackets && !wrapRevisionResultInObject) {
            // When the result IS a single fused named structural member, emitRevisionStartNode
            // already opened the wrapping `{` (see wrapRevisionResultInObject); opening another
            // here would double-brace (`"revision":{{…`). Otherwise emit the start-node wrapper.
            appendObjectStart(innerHasChildren);
            hadToAddBracket = true;
          }

          if (withMetaDataField()) {
            if (rtx.hasLeftSibling()
                && !(startNodeKey != Fixed.NULL_NODE_KEY.getStandardProperty()
                     && rtx.getNodeKey() == startNodeKey)) {
              appendObjectStart(true);
            }
            appendObjectKeyValue(quote("key"), quotedObjectKey(rtx))
                .appendSeparator()
                .appendObjectKey(quote("metadata"))
                .appendObjectStart(true);
            if (withNodeKeyMetaData || withNodeKeyAndChildNodeKeyMetaData) {
              appendObjectKeyValue(quote("nodeKey"), String.valueOf(rtx.getNodeKey()));
              // A fused structural node is object/array-like, so childCount always follows in
              // nodeKeyAndChildCount mode. Emit the separator right after nodeKey (mirroring
              // emitMetaData) instead of buggily gating it on withMetaData before childCount —
              // which left "nodeKey":N"childCount":M (missing comma) in nodeKeyAndChildCount mode.
              if (withMetaData || withNodeKeyAndChildNodeKeyMetaData) {
                appendSeparator();
              }
            }
            if (withMetaData) {
              if (rtx.getHash() != 0L) {
                appendObjectKeyValue(quote("hash"), quote(printHashValue(rtx)));
                appendSeparator();
              }
              appendObjectKeyValue(quote("type"), quote(rtx.getKind().toString()));
              if (rtx.getHash() != 0L) {
                appendSeparator().appendObjectKeyValue(quote("descendantCount"),
                    String.valueOf(rtx.getDescendantCount()));
              }
            }
            if (withNodeKeyAndChildNodeKeyMetaData) {
              if (withMetaData) {
                appendSeparator();
              }
              appendObjectKeyValue(quote("childCount"), String.valueOf(rtx.getChildCount()));
            }
            appendObjectEnd(innerHasChildren).appendSeparator();
            appendObjectKey(quote("value"));
            // iter#32 P2 metadata-mode shape — match legacy OBJECT/ARRAY body behavior:
            //   OBJECT_NAMED_OBJECT (parent of OBJECT_KEY-shaped children) emits `[{` so the
            //     first OBJECT_KEY-shaped child does NOT open its own wrapper (matches legacy OBJECT).
            //   OBJECT_NAMED_ARRAY  (parent of STRING_VALUE/etc array elements) emits `[`, and
            //     each child element opens its own `{` via emitMetaData (matches legacy ARRAY).
            appendArrayStart(innerEmitChildren);
            if (isNamedObject) {
              appendObjectStart(innerEmitChildren);
            }
          } else {
            appendObjectKey(quotedObjectKey(rtx));
            // Emit the OBJECT/ARRAY start in the no-metadata path.
            if (isNamedObject) {
              appendObjectStart(innerEmitChildren);
            } else {
              appendArrayStart(innerEmitChildren);
            }
          }

          if (!innerHasChildren || (visitor != null && ((!hasToSkipSiblings && currentLevel() + 1 > maxLevel())
              || (hasToSkipSiblings && currentLevel() > maxLevel())))) {
            if (withMetaDataField()) {
              if (isNamedObject) {
                // Close placeholder first-child `{`, then array-`]`, then outer wrapper `}`.
                appendObjectEnd(false).appendArrayEnd(false);
              } else {
                // Close array-`]`, then outer wrapper `}`.
                appendArrayEnd(false);
              }
              appendObjectEnd(true);
            } else {
              if (isNamedObject) {
                appendObjectEnd(false);
              } else {
                appendArrayEnd(false);
              }
              // Close the start-node wrapper `{` opened above — childless fused records are
              // leaves, so emitEndNode (which closes it for the with-children case) never runs.
              if (hadToAddBracket && rtx.getNodeKey() == startNodeKey) {
                appendObjectEnd(false);
              }
            }
            printCommaIfNeeded(rtx);
          }
          break;
        }
        case OBJECT_NAMED_BOOLEAN:
        case OBJECT_NAMED_NUMBER:
        case OBJECT_NAMED_STRING:
        case OBJECT_NAMED_NULL: {
          // Fused OBJECT_NAMED_* emits "name":value inline — there is no separate
          // primitive-value child record to walk to. Per-record `{` is opened by the parent
          // OBJECT body's `appendObjectStart` for the FIRST child and by this case for
          // subsequent siblings; close `}` here in emitNode (since fused records are leaves
          // and emitEndNode is not invoked for leaves), then emit the inter-sibling `,`.
          final boolean isStartNode =
              startNodeKey != Fixed.NULL_NODE_KEY.getStandardProperty() && rtx.getNodeKey() == startNodeKey;
          // A fused primitive serialized AS the start node has no parent context to brace it —
          // without a wrapper the output is a bare `"name":value` fragment (invalid JSON).
          // Mirrors the structural start-node wrapper above; suppressed for
          // JsonRecordSerializer (serializeStartNodeWithBrackets=false) and for the
          // XQuery-result wrap, which braces the member itself.
          final boolean wrapStartNode = isStartNode && serializeStartNodeWithBrackets && !wrapRevisionResultInObject;
          if (wrapStartNode) {
            appendObjectStart(true);
          }
          if (withMetaDataField()) {
            if (rtx.hasLeftSibling() && !isStartNode) {
              appendObjectStart(true);
            }
            appendObjectKeyValue(quote("key"), quotedObjectKey(rtx))
                .appendSeparator()
                .appendObjectKey(quote("metadata"))
                .appendObjectStart(true);
            if (withNodeKeyMetaData || withNodeKeyAndChildNodeKeyMetaData) {
              appendObjectKeyValue(quote("nodeKey"), String.valueOf(rtx.getNodeKey()));
            }
            if (withMetaData) {
              appendSeparator();
              if (rtx.getHash() != 0L) {
                appendObjectKeyValue(quote("hash"), quote(printHashValue(rtx)));
                appendSeparator();
              }
              // Emit the concrete fused kind name (OBJECT_NAMED_*) — JsonSerializer's existing
              // fixtures expect the precise kind, while JsonLimitedSerializer/JsonRecordSerializer
              // collapse the wire-name to OBJECT_KEY for the pagination-style payload.
              appendObjectKeyValue(quote("type"), quote(rtx.getKind().toString()));
            }
            appendObjectEnd(true).appendSeparator();
            appendObjectKey(quote("value"));
          } else {
            appendObjectKey(quotedObjectKey(rtx));
          }
          if (rtx.getKind() == NodeKind.OBJECT_NAMED_STRING) {
            appendStringValue(rtx);
          } else {
            // boolean, number, null — raw stringified form via getValue()
            appendObjectValue(rtx.getValue());
          }
          // Close the per-record `{` (whether opened above for siblings, by the parent OBJECT
          // body for first-child, or as the start-node wrapper) so subsequent commas land
          // outside the wrapper.
          if ((withMetaDataField() && !isStartNode) || wrapStartNode) {
            appendObjectEnd(true);
          }
          printCommaIfNeeded(rtx);
          break;
        }
        // $CASES-OMITTED$
        default:
          throw new IllegalStateException("Node kind not known!");
      }
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  /**
   * Object keys repeat massively in real-world JSON (every record shares the same field names).
   * Cache the quoted+escaped lexical form per dictionary nameKey. The mapping is only stable
   * WITHIN one revision: {@code Names.removeName} frees a dictionary slot at refcount 0 and a
   * later revision's {@code setName} may reassign it to a different (hash-colliding) name — so a
   * multi-revision serialization must clear the cache per revision (see
   * {@link #emitRevisionStartNode}) or it prints a prior revision's key text.
   */
  private final it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap<String> quotedKeyCache =
      new it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap<>();

  private String quotedObjectKey(final JsonNodeReadOnlyTrx rtx) {
    final int nameKey = rtx.getNameKey();
    if (nameKey == -1) {
      return quote(StringValue.escape(rtx.getName().stringValue()));
    }
    String cached = quotedKeyCache.get(nameKey);
    if (cached == null) {
      cached = quote(StringValue.escape(rtx.getName().stringValue()));
      quotedKeyCache.put(nameKey, cached);
    }
    return cached;
  }

  private String printHashValue(JsonNodeReadOnlyTrx rtx) {
    return String.format("%016x", rtx.getHash());
  }

  private boolean withMetaDataField() {
    return withMetaData || withNodeKeyMetaData || withNodeKeyAndChildNodeKeyMetaData;
  }

  private boolean shouldEmitChildren(boolean hasChildren) {
    return (visitor == null && hasChildren) || (visitor != null && hasChildren && currentLevel() + 1 <= maxLevel());
  }

  private void emitMetaData(JsonNodeReadOnlyTrx rtx) throws IOException {
    if (withMetaDataField()) {
      appendObjectStart(true).appendObjectKey(quote("metadata")).appendObjectStart(true);

      if (withNodeKeyMetaData || withNodeKeyAndChildNodeKeyMetaData) {
        appendObjectKeyValue(quote("nodeKey"), String.valueOf(rtx.getNodeKey()));
        if (withMetaData || withNodeKeyAndChildNodeKeyMetaData
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

      if (withNodeKeyAndChildNodeKeyMetaData && (rtx.getKind() == NodeKind.OBJECT || rtx.getKind() == NodeKind.ARRAY)) {
        if (withMetaData) {
          appendSeparator();
        }
        appendObjectKeyValue(quote("childCount"), String.valueOf(rtx.getChildCount()));
      }

      appendObjectEnd(true).appendSeparator().appendObjectKey(quote("value"));
    }
  }

  @Override
  protected void setTrxForVisitor(JsonNodeReadOnlyTrx rtx) {
    castVisitor().setTrx(rtx);
  }

  private long maxLevel() {
    return castVisitor().getMaxLevel();
  }

  private long maxChildNodes() {
    return castVisitor().getMaxChildNodes();
  }

  private long maxNumberOfNodes() {
    return castVisitor().getMaxNodes();
  }

  private JsonMaxLevelMaxNodesMaxChildNodesVisitor castVisitor() {
    return (JsonMaxLevelMaxNodesMaxChildNodesVisitor) visitor;
  }

  private long currentLevel() {
    return castVisitor().getCurrentLevel();
  }

  private long currentChildNodes() {
    return castVisitor().getCurrentChildNodes();
  }

  private long numberOfVisitedNodesPlusOne() {
    return castVisitor().getNumberOfVisitedNodesPlusOne();
  }

  @Override
  protected boolean isSubtreeGoingToBeVisited(final JsonNodeReadOnlyTrx rtx) {
    if (rtx.isObjectKey()) {
      return true;
    }

    return visitor == null || (!hasToSkipSiblings && currentLevel() + 1 <= maxLevel())
        || (hasToSkipSiblings && currentLevel() <= maxLevel());
  }

  @Override
  protected boolean areSiblingNodesGoingToBeSkipped(JsonNodeReadOnlyTrx rtx) {
    if (rtx.isObjectKey() || visitor == null) {
      return false;
    }
    return currentChildNodes() + 1 > maxChildNodes();
  }

  private void printCommaIfNeeded(final JsonNodeReadOnlyTrx rtx) throws IOException {
    final boolean hasRightSibling = rtx.hasRightSibling();

    if (hasRightSibling && rtx.getNodeKey() != startNodeKey && (visitor == null
        || currentChildNodes() < maxChildNodes() && numberOfVisitedNodesPlusOne() < maxNumberOfNodes())) {
      appendSeparator();
    }
  }

  @Override
  protected void emitEndNode(final JsonNodeReadOnlyTrx rtx, final boolean lastEndNode) {
    try {
      final var lastVisitResultType = visitor == null
          ? null
          : ((JsonMaxLevelMaxNodesMaxChildNodesVisitor) visitor).getLastVisitResultType();
      switch (rtx.getKind()) {
        case ARRAY -> {
          if (withMetaDataField()) {
            appendArrayEnd(true).appendObjectEnd(true);
          } else {
            appendArrayEnd(shouldEmitChildren(rtx.hasChildren()));
          }
          if (hasToAppendSeparator(rtx, lastVisitResultType, lastEndNode)) {
            appendSeparator();
          }
        }
        case OBJECT -> {
          if (withMetaDataField()) {
            appendArrayEnd(true).appendObjectEnd(true);
          } else {
            appendObjectEnd(shouldEmitChildren(rtx.hasChildren()));
          }
          if (hasToAppendSeparator(rtx, lastVisitResultType, lastEndNode)) {
            appendSeparator();
          }
        }
        case OBJECT_NAMED_OBJECT -> {
          // iter#32 P2 fused-structural close. Pre-emit shape (with metadata):
          //   {"key":"<n>","metadata":{...},"value":[{ <last-child-content> }
          // Match the close: `]}` (close last-child `}` already done by the child-leaf case
          // / nested OBJECT_NAMED_*-end; we must close the array `]` and outer wrapper `}`).
          // Bare:   "<n>":{ ... }   →  emit `}`.
          if (withMetaDataField()) {
            appendArrayEnd(true);
            if (!isSuppressedStartNodeWrapper(rtx)) {
              appendObjectEnd(true);
            }
          } else {
            appendObjectEnd(shouldEmitChildren(rtx.hasChildren()));
            // Close the start-node wrapper `{` from emitNode (metadata mode balances it via
            // the appendObjectEnd above; the bare path previously leaked it → `{"n":{…}`).
            if (hadToAddBracket && rtx.getNodeKey() == startNodeKey) {
              appendObjectEnd(true);
            }
          }
          if (hasToAppendSeparator(rtx, lastVisitResultType, lastEndNode)) {
            appendSeparator();
          }
        }
        case OBJECT_NAMED_ARRAY -> {
          // iter#32 P2 fused-structural close. Same wrapping as OBJECT_NAMED_OBJECT.
          if (withMetaDataField()) {
            appendArrayEnd(true);
            if (!isSuppressedStartNodeWrapper(rtx)) {
              appendObjectEnd(true);
            }
          } else {
            appendArrayEnd(shouldEmitChildren(rtx.hasChildren()));
            if (hadToAddBracket && rtx.getNodeKey() == startNodeKey) {
              appendObjectEnd(true);
            }
          }
          if (hasToAppendSeparator(rtx, lastVisitResultType, lastEndNode)) {
            appendSeparator();
          }
        }
        // iter#32 fusion: primitive OBJECT_NAMED_* records are leaves, so emitEndNode is not
        // invoked for them; the wrapper close + sibling separator are emitted in emitNode (above)
        // instead. The structural OBJECT_NAMED_OBJECT/ARRAY cases above DO close on emitEndNode.
        // $CASES-OMITTED$
        default -> {
        }
      }
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  private boolean hasToAppendSeparator(JsonNodeReadOnlyTrx rtx, VisitResultType lastVisitResultType,
      boolean lastEndNode) {
    return rtx.hasRightSibling() && rtx.getNodeKey() != startNodeKey && VisitResultType.TERMINATE != lastVisitResultType
        && (visitor == null || lastEndNode);
  }

  @Override
  protected void emitStartDocument() {
    try {
      final int length = (revisions.length == 1 && revisions[0] < 0)
          ? session.getMostRecentRevisionNumber()
          : revisions.length;

      if (length > 1) {
        appendObjectStart(true);

        if (indent) {
          // mOut.append(CharsForSerializing.NEWLINE.getBytes());
          stack.push(Constants.NULL_ID_LONG);
        }

        appendObjectKey(quote("sirix"));
        appendArrayStart(true);
      }
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  @Override
  protected void emitEndDocument() {
    try {
      final int length = (revisions.length == 1 && revisions[0] < 0)
          ? session.getMostRecentRevisionNumber()
          : revisions.length;

      if (length > 1) {
        if (indent) {
          stack.popLong();
        }

        appendArrayEnd(true).appendObjectEnd(true);
      }
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  @Override
  protected void emitRevisionStartNode(final JsonNodeReadOnlyTrx rtx) {
    // nameKey→text bindings are only stable within one revision (see quotedKeyCache javadoc).
    quotedKeyCache.clear();
    try {
      final int length = (revisions.length == 1 && revisions[0] < 0)
          ? session.getMostRecentRevisionNumber()
          : revisions.length;

      if (emitXQueryResultSequence || length > 1) {
        appendObjectStart(rtx.hasChildren())
                                            .appendObjectKeyValue(quote("revisionNumber"),
                                                Integer.toString(rtx.getRevisionNumber()))
                                            .appendSeparator();

        if (serializeTimestamp) {
          appendObjectKeyValue(quote("revisionTimestamp"), quote(DateTimeFormatter.ISO_INSTANT.withZone(
              ZoneOffset.UTC).format(rtx.getRevisionTimestamp()))).appendSeparator();
        }

        appendObjectKey(quote("revision"));

        // A query result that IS a single fused named member (e.g. `.products[1].id`, an
        // OBJECT_NAMED_STRING) serializes inline as a bare `"name":value` fragment. Emitted
        // straight after `"revision":` that produces invalid JSON (`"revision":"id":"…"`), so wrap
        // such a result in an object → `"revision":{"id":"…"}`. Only in the non-metadata path:
        // with metadata the named node already emits a full `{key,metadata,value}` object.
        //
        // The framework only moves rtx to the result node AFTER this callback (see
        // AbstractSerializer), so peek at the start node's kind here and restore the cursor.
        wrapRevisionResultInObject = false;
        if (!withMetaDataField() && startNodeKey != Fixed.NULL_NODE_KEY.getStandardProperty()) {
          final long cursorKey = rtx.getNodeKey();
          if (rtx.moveTo(startNodeKey)) {
            wrapRevisionResultInObject = isFusedNamedMember(rtx.getKind());
          }
          rtx.moveTo(cursorKey);
        }
        if (wrapRevisionResultInObject) {
          appendObjectStart(true);
        }

        if (rtx.hasFirstChild()) {
          stack.push(Constants.NULL_ID_LONG);
        }
      }
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  /**
   * Fused records that serialize as a bare {@code "name":value} (or {@code "name":{…}} /
   * {@code "name":[…]}) fragment with no enclosing object — these must be wrapped in {@code {}}
   * when they are a single query result, else the fragment is emitted straight after
   * {@code "revision":} and yields invalid JSON ({@code "revision":"id":"A"} — two colons — for a
   * leaf, or a bare member for a structural node). Wrapping is the SOLE wrap mechanism for these in
   * the XQuery-result path: the {@code serializeStartNodeWithBrackets} start-node bracket is
   * suppressed for them (see {@link #emitNode}) so structural nodes are not double-braced.
   */
  private static boolean isFusedNamedMember(final NodeKind kind) {
    return switch (kind) {
      case OBJECT_NAMED_BOOLEAN, OBJECT_NAMED_NUMBER, OBJECT_NAMED_STRING, OBJECT_NAMED_NULL,
           OBJECT_NAMED_OBJECT, OBJECT_NAMED_ARRAY -> true;
      default -> false;
    };
  }

  /**
   * Whether the fused-structural record-wrapper {@code {}} of the current start node was suppressed
   * at emit time. emitNode only opens the start node's {@code {} when
   * {@code serializeStartNodeWithBrackets} is set; when it is not (e.g. {@link JsonRecordSerializer}
   * wraps each top-level record itself), the opening {@code {} is suppressed, so emitEndNode must
   * likewise suppress the matching closing {@code }} — otherwise an extra {@code }} corrupts the
   * JSON (invalid output for {@code ?nextTopLevelNodes=N&withMetaData=…} with no maxLevel).
   */
  private boolean isSuppressedStartNodeWrapper(final JsonNodeReadOnlyTrx rtx) {
    return rtx.getNodeKey() == startNodeKey && !serializeStartNodeWithBrackets;
  }

  @Override
  protected void emitRevisionEndNode(final JsonNodeReadOnlyTrx rtx) {
    try {
      final int length = (revisions.length == 1 && revisions[0] < 0)
          ? session.getMostRecentRevisionNumber()
          : revisions.length;

      if (emitXQueryResultSequence || length > 1) {
        if (rtx.moveToDocumentRoot() && rtx.hasFirstChild() && !stack.isEmpty())
          stack.popLong();
        // Close the object we wrapped a single named-member result in (see emitRevisionStartNode),
        // before closing the outer revision-metadata object.
        if (wrapRevisionResultInObject) {
          appendObjectEnd(true);
          wrapRevisionResultInObject = false;
        }
        appendObjectEnd(rtx.hasChildren());

        if (hasMoreRevisionsToSerialize(rtx))
          appendSeparator();
      }
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  private boolean hasMoreRevisionsToSerialize(final JsonNodeReadOnlyTrx rtx) {
    return rtx.getRevisionNumber() < revisions[revisions.length - 1] || (revisions.length == 1 && revisions[0] == -1
        && rtx.getRevisionNumber() < rtx.getResourceSession().getMostRecentRevisionNumber());
  }

  /**
   * Indentation of output.
   *
   * @throws IOException if can't indent output
   */
  private void indent() throws IOException {
    if (indent) {
      for (int i = 0; i < currentIndent; i++) {
        out.ascii(' ');
      }
    }
  }

  private void newLine() throws IOException {
    if (indent) {
      out.ascii('\n');
      indent();
    }
  }

  private JsonSerializer appendObjectStart(final boolean hasChildren) throws IOException {
    out.ascii('{');
    if (hasChildren) {
      currentIndent += indentSpaces;
      newLine();
    }
    return this;
  }

  private JsonSerializer appendObjectEnd(final boolean hasChildren) throws IOException {
    if (hasChildren) {
      currentIndent -= indentSpaces;
      newLine();
    }
    out.ascii('}');
    return this;
  }

  private void appendArrayStart(final boolean hasChildren) throws IOException {
    out.ascii('[');
    if (hasChildren) {
      currentIndent += indentSpaces;
      newLine();
    }
  }

  private JsonSerializer appendArrayEnd(final boolean hasChildren) throws IOException {
    if (hasChildren) {
      currentIndent -= indentSpaces;
      newLine();
    }
    out.ascii(']');
    return this;
  }

  private JsonSerializer appendObjectKey(String key) throws IOException {
    out.text(key);
    if (indent) {
      out.ascii(':');
      out.ascii(' ');
    } else {
      out.ascii(':');
    }
    return this;
  }

  /**
   * Emit a string value. On the BYTE sink, an escape pre-scan over the stored UTF-8 bytes
   * (vectorized for long values) proves the overwhelmingly common escape-free case, which is
   * then bulk-copied verbatim — no String construction, no char→byte re-encoding.
   */
  private void appendStringValue(final JsonNodeReadOnlyTrx rtx) throws IOException {
    if (out.prefersRawUtf8()) {
      final byte[] raw = rtx.getValueBytes();
      if (raw != null && !JsonValueScan.mayNeedJsonEscape(raw)) {
        out.ascii('"');
        out.utf8(raw);
        out.ascii('"');
        return;
      }
    }
    appendQuotedObjectValue(StringValue.escape(rtx.getValue()));
  }

  /** Quoted string value without the intermediate "\"" + value + "\"" concatenation. */
  private void appendQuotedObjectValue(String value) throws IOException {
    out.ascii('"');
    out.text(value);
    out.ascii('"');
  }

  private void appendObjectValue(String value) throws IOException {
    out.text(value);
  }

  private JsonSerializer appendObjectKeyValue(String key, String value) throws IOException {
    out.text(key);
    if (indent) {
      out.ascii(':');
      out.ascii(' ');
    } else {
      out.ascii(':');
    }
    out.text(value);
    return this;
  }

  private JsonSerializer appendSeparator() throws IOException {
    out.ascii(',');
    newLine();
    return this;
  }

  private String quote(String value) {
    return "\"" + value + "\"";
  }

  /**
   * Main method.
   *
   * @param args args[0] specifies the input-TT file/folder; args[1] specifies the output XML file.
   * @throws Exception any exception
   */
  public static void main(final String... args) throws Exception {
    if (args.length < 2 || args.length > 3) {
      throw new IllegalArgumentException("Usage: JsonSerializer database output.json");
    }

    LOGWRAPPER.info("Serializing '" + args[0] + "' to '" + args[1] + "' ... ");
    final long time = System.nanoTime();
    final Path target = Paths.get(args[1]);
    SirixFiles.recursiveRemove(target);
    Files.createDirectories(target.getParent());
    Files.createFile(target);

    final Path databaseFile = Paths.get(args[0]);
    final DatabaseConfiguration config = new DatabaseConfiguration(databaseFile);
    Databases.createJsonDatabase(config);
    try (final var db = Databases.openJsonDatabase(databaseFile)) {
      db.createResource(new ResourceConfiguration.Builder("shredded").build());

      try (final JsonResourceSession resMgr = db.beginResourceSession("shredded");
          final FileWriter outputStream = new FileWriter(target.toFile())) {
        final JsonSerializer serializer = JsonSerializer.newBuilder(resMgr, outputStream).build();
        serializer.call();
      }
    }

    LOGWRAPPER.info(" done [" + (System.nanoTime() - time) / 1_000_000 + "ms].");
  }

  /**
   * Constructor, setting the necessary stuff.
   *
   * @param resMgr Sirix {@link ResourceSession}
   * @param stream {@link OutputStream} to write to
   * @param revisions revisions to serialize
   */
  /**
   * Byte-pipeline builder: serializes UTF-8 straight to the stream — stored string values that
   * need no escaping are bulk-copied without String construction or char→byte re-encoding.
   */
  public static Builder newBuilder(final JsonResourceSession resMgr, final java.io.OutputStream stream,
      final int... revisions) {
    return new Builder(resMgr, null, revisions).byteStream(stream);
  }

  public static Builder newBuilder(final JsonResourceSession resMgr, final Writer stream, final int... revisions) {
    return new Builder(resMgr, stream, revisions);
  }

  /**
   * Constructor.
   *
   * @param resMgr Sirix {@link ResourceSession}
   * @param nodeKey root node key of subtree to shredder
   * @param stream {@link OutputStream} to write to
   * @param properties {@link XmlSerializerProperties} to use
   * @param revisions revisions to serialize
   */
  public static Builder newBuilder(final JsonResourceSession resMgr, final long nodeKey,
      final Writer stream, final JsonSerializerProperties properties, final int... revisions) {
    return new Builder(resMgr, nodeKey, stream, properties, revisions);
  }

  /**
   * JsonSerializerBuilder to setup the JsonSerializer.
   */
  public static final class Builder {
    /**
     * Intermediate boolean for indendation, not necessary.
     */
    private boolean indent;

    /**
     * Intermediate number of spaces to indent, not necessary.
     */
    private int indentSpaces = 2;

    /**
     * Stream to pipe to.
     */
    private final Appendable stream;

    /** Byte-pipeline target — when set, the serializer emits UTF-8 bytes end-to-end. */
    private java.io.OutputStream byteStream;

    /**
     * Resource session to use.
     */
    private final JsonResourceSession resourceMgr;

    /**
     * Further revisions to serialize.
     */
    private int[] versions;

    /**
     * Revision to serialize.
     */
    private int version;

    /**
     * Node key of subtree to shredder.
     */
    private long startNodeKey;

    /**
     * Determines if an initial indent is needed or not.
     */
    private boolean initialIndent;

    /**
     * Determines if it's an XQuery result sequence.
     */
    private boolean emitXQueryResultSequence;

    /**
     * Determines if a timestamp should be serialized or not.
     */
    private boolean serializeTimestamp;

    /**
     * Determines if SirixDB meta data should be serialized for JSON object key nodes or not.
     */
    private boolean withMetaData;

    /**
     * Determines the maximum level to up to which to skip subtrees from serialization.
     */
    private long maxLevel;

    /**
     * Determines the maximum of nodes to serialize.
     */
    private long maxNodes;

    /**
     * Determines if nodeKey meta data should be serialized or not.
     */
    private boolean withNodeKey;

    /**
     * Determines if childCount meta data should be serialized or not.
     */
    private boolean withNodeKeyAndChildCount;

    private boolean serializeStartNodeWithBrackets;

    private long maxChildNodes;

    /**
     * Constructor, setting the necessary stuff.
     *
     * @param resourceMgr Sirix {@link ResourceSession}
     * @param stream {@link OutputStream} to write to
     * @param revisions revisions to serialize
     */
    public Builder(final JsonResourceSession resourceMgr, final Appendable stream, final int... revisions) {
      serializeStartNodeWithBrackets = true;
      maxLevel = Long.MAX_VALUE;
      startNodeKey = 0;
      this.resourceMgr = requireNonNull(resourceMgr);
      // stream may be null ONLY for the byte pipeline (newBuilder(…, OutputStream, …) sets
      // byteStream right after construction); the constructor wiring picks the sink.
      this.stream = stream;
      if (revisions == null || revisions.length == 0) {
        version = this.resourceMgr.getMostRecentRevisionNumber();
      } else {
        version = revisions[0];
        versions = new int[revisions.length - 1];
        System.arraycopy(revisions, 1, versions, 0, revisions.length - 1);
      }
      maxNodes = Long.MAX_VALUE;
      maxChildNodes = Long.MAX_VALUE;
    }

    /**
     * Constructor.
     *
     * @param resourceMgr Sirix {@link ResourceSession}
     * @param nodeKey root node key of subtree to shredder
     * @param stream {@link OutputStream} to write to
     * @param properties {@link JsonSerializerProperties} to use
     * @param revisions revisions to serialize
     */
    public Builder(final JsonResourceSession resourceMgr, final long nodeKey, final Writer stream,
        final JsonSerializerProperties properties, final int... revisions) {
      checkArgument(nodeKey >= 0, "nodeKey must be >= 0!");
      serializeStartNodeWithBrackets = true;
      maxLevel = -1;
      this.resourceMgr = requireNonNull(resourceMgr);
      this.startNodeKey = nodeKey;
      this.stream = requireNonNull(stream);
      if (revisions == null || revisions.length == 0) {
        version = this.resourceMgr.getMostRecentRevisionNumber();
      } else {
        version = revisions[0];
        versions = new int[revisions.length - 1];
        System.arraycopy(revisions, 1, versions, 0, revisions.length - 1);
      }
      final ConcurrentMap<?, ?> map = requireNonNull(properties.getProps());
      indent = requireNonNull((Boolean) map.get(S_INDENT[0]));
      indentSpaces = requireNonNull((Integer) map.get(S_INDENT_SPACES[0]));
    }

    /**
     * Specify the start node key.
     *
     * @param nodeKey node key to start serialization from (the root of the subtree to serialize)
     * @return this instance
     */
    Builder byteStream(final java.io.OutputStream target) {
      requireNonNull(target);
      this.byteStream = target;
      return this;
    }

    public Builder startNodeKey(final long nodeKey) {
      this.startNodeKey = nodeKey;
      return this;
    }

    /**
     * Specify the maximum of nodes.
     *
     * @param maxNodes max nodes to serialize
     * @return this XMLSerializerBuilder reference
     */
    public Builder numberOfNodes(final long maxNodes) {
      this.maxNodes = maxNodes;
      return this;
    }

    /**
     * If the {@code startNodeKey} denotes an object key node "{}" are added.
     *
     * @param serializeStartNodeWithBrackets {@code true}, if brackets should be serialized,
     *        {@code false otherwise}
     * @return this reference
     */
    public Builder serializeStartNodeWithBrackets(final boolean serializeStartNodeWithBrackets) {
      this.serializeStartNodeWithBrackets = serializeStartNodeWithBrackets;
      return this;
    }

    /**
     * Specify the maximum level.
     *
     * @param maxLevel the maximum level until which to serialize
     * @return this reference
     */
    public Builder maxLevel(final long maxLevel) {
      this.maxLevel = maxLevel;
      return this;
    }

    /**
     * Sets an initial indentation.
     *
     * @return this reference
     */
    public Builder withInitialIndent() {
      initialIndent = true;
      return this;
    }

    /**
     * Sets the max number of child nodes to serialize.
     *
     * @return this reference
     */
    public Builder maxChildren(final long maxChildren) {
      this.maxChildNodes = maxChildren;
      return this;
    }

    /**
     * Sets if the serialization is used for XQuery result sets.
     *
     * @return this reference
     */
    public Builder isXQueryResultSequence() {
      emitXQueryResultSequence = true;
      return this;
    }

    /**
     * Sets if the serialization of timestamps of the revision(s) is used or not.
     *
     * @return this reference
     */
    public Builder serializeTimestamp(boolean serializeTimestamp) {
      this.serializeTimestamp = serializeTimestamp;
      return this;
    }

    /**
     * Sets if metadata should be serialized or not.
     *
     * @return this reference
     */
    public Builder withMetaData(boolean withMetaData) {
      this.withMetaData = withMetaData;
      this.withNodeKey = true;
      this.withNodeKeyAndChildCount = true;
      return this;
    }

    /**
     * Sets if nodeKey metadata should be serialized or not.
     *
     * @return this reference
     */
    public Builder withNodeKeyMetaData(boolean withNodeKey) {
      this.withNodeKey = withNodeKey;
      return this;
    }

    /**
     * Sets if nodeKey and childCount metadata should be serialized or not.
     *
     * @return this reference
     */
    public Builder withNodeKeyAndChildCountMetaData(boolean withNodeKeyAndChildCount) {
      this.withNodeKeyAndChildCount = withNodeKeyAndChildCount;
      return this;
    }

    /**
     * Pretty prints the output.
     *
     * @return this reference
     */
    public Builder prettyPrint() {
      indent = true;
      return this;
    }

    /**
     * The versions to serialize.
     *
     * @param revisions the versions to serialize
     * @return this {@link Builder} instance
     */
    public Builder revisions(final int[] revisions) {
      requireNonNull(revisions);

      version = revisions[0];

      versions = new int[revisions.length - 1];
      System.arraycopy(revisions, 1, versions, 0, revisions.length - 1);

      return this;
    }

    /**
     * Building new {@link Serializer} instance.
     *
     * @return a new {@link Serializer} instance
     */
    public JsonSerializer build() {
      return new JsonSerializer(resourceMgr, this);
    }
  }
}
