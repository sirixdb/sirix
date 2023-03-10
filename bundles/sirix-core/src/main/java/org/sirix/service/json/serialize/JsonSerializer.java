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

package org.sirix.service.json.serialize;

import org.brackit.xquery.util.serialize.Serializer;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.sirix.access.DatabaseConfiguration;
import org.sirix.access.Databases;
import org.sirix.access.ResourceConfiguration;
import org.sirix.api.ResourceSession;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.api.json.JsonResourceSession;
import org.sirix.api.visitor.VisitResultType;
import org.sirix.axis.IncludeSelf;
import org.sirix.node.NodeKind;
import org.sirix.service.AbstractSerializer;
import org.sirix.service.xml.serialize.XmlSerializerProperties;
import org.sirix.settings.Constants;
import org.sirix.settings.Fixed;
import org.sirix.utils.LogWrapper;
import org.sirix.utils.SirixFiles;
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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.sirix.service.xml.serialize.XmlSerializerProperties.S_INDENT;
import static org.sirix.service.xml.serialize.XmlSerializerProperties.S_INDENT_SPACES;

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
  private final Appendable out;

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

  private final boolean withMetaData;

  private final boolean withNodeKeyMetaData;

  private final boolean withNodeKeyAndChildNodeKeyMetaData;

  private final boolean serializeStartNodeWithBrackets;

  private boolean hadToAddBracket;

  private int currentIndent;

  /**
   * Private constructor.
   *
   * @param resourceMgr resource manager to read the resource
   * @param builder     builder of the JSON serializer
   */
  private JsonSerializer(final JsonResourceSession resourceMgr, final Builder builder) {
    super(resourceMgr,
          builder.maxLevel == Long.MAX_VALUE && builder.maxNodes == Long.MAX_VALUE
              && builder.maxChildNodes == Long.MAX_VALUE
              ? null
              : new JsonMaxLevelMaxNodesMaxChildNodesVisitor(builder.startNodeKey,
                                                             IncludeSelf.YES,
                                                             builder.maxLevel,
                                                             builder.maxNodes,
                                                             builder.maxChildNodes),
          builder.startNodeKey,
          builder.version,
          builder.versions);
    out = builder.stream;
    indent = builder.indent;
    indentSpaces = builder.indentSpaces;
    withInitialIndent = builder.initialIndent;
    emitXQueryResultSequence = builder.emitXQueryResultSequence;
    serializeTimestamp = builder.serializeTimestamp;
    withMetaData = builder.withMetaData;
    withNodeKeyMetaData = builder.withNodeKey;
    withNodeKeyAndChildNodeKeyMetaData = builder.withNodeKeyAndChildCount;
    serializeStartNodeWithBrackets = builder.serializeStartNodeWithBrackets;
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

          if (!hasChildren || (visitor != null && ((!hasToSkipSiblings && currentLevel() + 1 > maxLevel()) || (
              hasToSkipSiblings && currentLevel() > maxLevel())))) {
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

          if (!hasChildren || (visitor != null && ((!hasToSkipSiblings && currentLevel() + 1 > maxLevel()) || (
              hasToSkipSiblings && currentLevel() > maxLevel())))) {
            appendArrayEnd(false);

            if (withMetaDataField()) {
              appendObjectEnd(true);
            }

            printCommaIfNeeded(rtx);
          }
          break;
        case OBJECT_KEY:
          if (startNodeKey != Fixed.NULL_NODE_KEY.getStandardProperty() && rtx.getNodeKey() == startNodeKey
              && serializeStartNodeWithBrackets) {
            appendObjectStart(hasChildren);
            hadToAddBracket = true;
          }

          if (withMetaDataField()) {
            if (rtx.hasLeftSibling() && !(startNodeKey != Fixed.NULL_NODE_KEY.getStandardProperty()
                && rtx.getNodeKey() == startNodeKey)) {
              appendObjectStart(true);
            }

            appendObjectKeyValue(quote("key"), quote(rtx.getName().stringValue())).appendSeparator()
                                                                                  .appendObjectKey(quote("metadata"))
                                                                                  .appendObjectStart(hasChildren);

            if (withNodeKeyMetaData || withNodeKeyAndChildNodeKeyMetaData) {
              appendObjectKeyValue(quote("nodeKey"), String.valueOf(rtx.getNodeKey()));
            }

            if (withMetaData) {
              appendSeparator();
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

            appendObjectEnd(hasChildren).appendSeparator();

            appendObjectKey(quote("value"));
          } else {
            appendObjectKey(quote(rtx.getName().stringValue()));
          }
          break;
        case BOOLEAN_VALUE:
        case OBJECT_BOOLEAN_VALUE:
          emitMetaData(rtx);
          appendObjectValue(Boolean.valueOf(rtx.getValue()).toString());
          if (withMetaDataField()) {
            appendObjectEnd(true);
          }
          printCommaIfNeeded(rtx);
          break;
        case NULL_VALUE:
        case OBJECT_NULL_VALUE:
          emitMetaData(rtx);
          appendObjectValue("null");
          if (withMetaDataField()) {
            appendObjectEnd(true);
          }
          printCommaIfNeeded(rtx);
          break;
        case NUMBER_VALUE:
        case OBJECT_NUMBER_VALUE:
          emitMetaData(rtx);
          appendObjectValue(rtx.getValue());
          if (withMetaDataField()) {
            appendObjectEnd(true);
          }
          printCommaIfNeeded(rtx);
          break;
        case STRING_VALUE:
        case OBJECT_STRING_VALUE:
          emitMetaData(rtx);
          appendObjectValue(quote(StringValue.escape(rtx.getValue())));
          if (withMetaDataField()) {
            appendObjectEnd(true);
          }
          printCommaIfNeeded(rtx);
          break;
        // $CASES-OMITTED$
        default:
          throw new IllegalStateException("Node kind not known!");
      }
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
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
        if (withMetaData || withNodeKeyAndChildNodeKeyMetaData && (rtx.getKind() == NodeKind.OBJECT
            || rtx.getKind() == NodeKind.ARRAY)) {
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

    return visitor == null || (!hasToSkipSiblings && currentLevel() + 1 <= maxLevel()) || (hasToSkipSiblings
        && currentLevel() <= maxLevel());
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
      final var lastVisitResultType =
          visitor == null ? null : ((JsonMaxLevelMaxNodesMaxChildNodesVisitor) visitor).getLastVisitResultType();
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
        case OBJECT_KEY -> {
          if ((withMetaDataField() && !(startNodeKey != Fixed.NULL_NODE_KEY.getStandardProperty()
              && rtx.getNodeKey() == startNodeKey)) || (hadToAddBracket && rtx.getNodeKey() == startNodeKey)) {
            appendObjectEnd(true);
          }
          if (hasToAppendSeparator(rtx, lastVisitResultType, lastEndNode)) {
            appendSeparator();
          }
        }
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
      final int length =
          (revisions.length == 1 && revisions[0] < 0) ? resMgr.getMostRecentRevisionNumber() : revisions.length;

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
      final int length =
          (revisions.length == 1 && revisions[0] < 0) ? resMgr.getMostRecentRevisionNumber() : revisions.length;

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
  protected void emitRevisionStartNode(final @NonNull JsonNodeReadOnlyTrx rtx) {
    try {
      final int length =
          (revisions.length == 1 && revisions[0] < 0) ? resMgr.getMostRecentRevisionNumber() : revisions.length;

      if (emitXQueryResultSequence || length > 1) {
        appendObjectStart(rtx.hasChildren()).appendObjectKeyValue(quote("revisionNumber"),
                                                                  Integer.toString(rtx.getRevisionNumber()))
                                            .appendSeparator();

        if (serializeTimestamp) {
          appendObjectKeyValue(quote("revisionTimestamp"),
                               quote(DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC)
                                                                  .format(rtx.getRevisionTimestamp()))).appendSeparator();
        }

        appendObjectKey(quote("revision"));

        if (rtx.hasFirstChild()) {
          stack.push(Constants.NULL_ID_LONG);
        }
      }
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  @Override
  protected void emitRevisionEndNode(final @NonNull JsonNodeReadOnlyTrx rtx) {
    try {
      final int length =
          (revisions.length == 1 && revisions[0] < 0) ? resMgr.getMostRecentRevisionNumber() : revisions.length;

      if (emitXQueryResultSequence || length > 1) {
        if (rtx.moveToDocumentRoot() && rtx.hasFirstChild())
          stack.popLong();
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

  private JsonSerializer appendObjectStart(final boolean hasChildren) throws IOException {
    out.append('{');
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
    out.append('}');
    return this;
  }

  private void appendArrayStart(final boolean hasChildren) throws IOException {
    out.append('[');
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
    out.append(']');
    return this;
  }

  private JsonSerializer appendObjectKey(String key) throws IOException {
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

  private JsonSerializer appendObjectKeyValue(String key, String value) throws IOException {
    out.append(key);
    if (indent) {
      out.append(": ");
    } else {
      out.append(":");
    }
    out.append(value);
    return this;
  }

  private JsonSerializer appendSeparator() throws IOException {
    out.append(',');
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
   * @param resMgr    Sirix {@link ResourceSession}
   * @param stream    {@link OutputStream} to write to
   * @param revisions revisions to serialize
   */
  public static Builder newBuilder(final JsonResourceSession resMgr, final Writer stream, final int... revisions) {
    return new Builder(resMgr, stream, revisions);
  }

  /**
   * Constructor.
   *
   * @param resMgr     Sirix {@link ResourceSession}
   * @param nodeKey    root node key of subtree to shredder
   * @param stream     {@link OutputStream} to write to
   * @param properties {@link XmlSerializerProperties} to use
   * @param revisions  revisions to serialize
   */
  public static Builder newBuilder(final JsonResourceSession resMgr, final @NonNegative long nodeKey,
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

    /**
     * Resource manager to use.
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
     * @param stream      {@link OutputStream} to write to
     * @param revisions   revisions to serialize
     */
    public Builder(final JsonResourceSession resourceMgr, final Appendable stream, final int... revisions) {
      serializeStartNodeWithBrackets = true;
      maxLevel = Long.MAX_VALUE;
      startNodeKey = 0;
      this.resourceMgr = requireNonNull(resourceMgr);
      this.stream = requireNonNull(stream);
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
     * @param nodeKey     root node key of subtree to shredder
     * @param stream      {@link OutputStream} to write to
     * @param properties  {@link JsonSerializerProperties} to use
     * @param revisions   revisions to serialize
     */
    public Builder(final JsonResourceSession resourceMgr, final @NonNegative long nodeKey, final Writer stream,
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
     * @param serializeStartNodeWithBrackets {@code true}, if brackets should be serialized, {@code false otherwise}
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
