/**
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
import org.sirix.access.DatabaseConfiguration;
import org.sirix.access.Databases;
import org.sirix.access.ResourceConfiguration;
import org.sirix.api.ResourceManager;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.node.NodeKind;
import org.sirix.service.AbstractSerializer;
import org.sirix.service.xml.serialize.XmlSerializerProperties;
import org.sirix.settings.Constants;
import org.sirix.settings.Fixed;
import org.sirix.utils.LogWrapper;
import org.sirix.utils.SirixFiles;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
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
import static com.google.common.base.Preconditions.checkNotNull;
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

  private boolean hadToAddBracket;

  private int currentIndent;

  /**
   * Initialize XMLStreamReader implementation with transaction. The cursor points to the node the
   * XMLStreamReader starts to read.
   *
   * @param resourceMgr resource manager to read the resource
   * @param nodeKey     start node key
   * @param builder     builder of XML Serializer
   * @param revision    revision to serialize
   * @param revsions    further revisions to serialize
   */
  private JsonSerializer(final JsonResourceManager resourceMgr, final @Nonnegative long nodeKey, final Builder builder,
      final boolean initialIndent, final @Nonnegative int revision, final int... revsions) {
    super(resourceMgr,
          builder.maxLevel == -1 ? null : new JsonMaxLevelVisitor(builder.maxLevel),
          nodeKey,
          revision,
          revsions);
    out = builder.stream;
    indent = builder.indent;
    indentSpaces = builder.indentSpaces;
    withInitialIndent = builder.tnitialIndent;
    emitXQueryResultSequence = builder.emitXQueryResultSequence;
    serializeTimestamp = builder.serializeTimestamp;
    withMetaData = builder.withMetaData;
    withNodeKeyMetaData = builder.withNodeKey;
  }

  /**
   * Emit node.
   *
   * @param rtx Sirix {@link JsonNodeReadOnlyTrx}
   */
  @Override
  protected void emitNode(final JsonNodeReadOnlyTrx rtx) {
    try {
      final var hasChildren = rtx.hasChildren();

      switch (rtx.getKind()) {
        case JSON_DOCUMENT:
          break;
        case OBJECT:
          emitMetaData(rtx);

          if ((withMetaData || withNodeKeyMetaData) && shouldEmitChildren(hasChildren)) {
            appendArrayStart(true);
          }

          appendObjectStart(shouldEmitChildren(hasChildren));

          if (!hasChildren || (visitor != null && currentLevel() + 1 >= maxLevel())) {
            appendObjectEnd(false);

            if (withMetaData || withNodeKeyMetaData) {
              appendObjectEnd(true);
            }

            if (rtx.hasRightSibling() && rtx.getNodeKey() != startNodeKey) {
              appendObjectSeparator();
            }
          }
          break;
        case ARRAY:
          emitMetaData(rtx);

          appendArrayStart(shouldEmitChildren(hasChildren));

          if (!hasChildren || (visitor != null && currentLevel() + 1 >= maxLevel())) {
            appendArrayEnd(false);

            if (withMetaData || withNodeKeyMetaData) {
              appendObjectEnd(true);
            }

            if (rtx.hasRightSibling() && rtx.getNodeKey() != startNodeKey) {
              appendObjectSeparator();
            }
          }
          break;
        case OBJECT_KEY:
          if (startNodeKey != Fixed.NULL_NODE_KEY.getStandardProperty() && rtx.getNodeKey() == startNodeKey) {
            appendObjectStart(hasChildren);
            hadToAddBracket = true;
          }

          if (withMetaData || withNodeKeyMetaData) {
            if (rtx.hasLeftSibling() && !(startNodeKey != Fixed.NULL_NODE_KEY.getStandardProperty()
                && rtx.getNodeKey() == startNodeKey)) {
              appendObjectStart(true);
            }

            appendObjectKeyValue(quote("key"), quote(rtx.getName().stringValue())).appendObjectSeparator()
                                                                                  .appendObjectKey(quote("metadata"))
                                                                                  .appendObjectStart(hasChildren);

            if (withNodeKeyMetaData) {
              appendObjectKeyValue(quote("nodeKey"), String.valueOf(rtx.getNodeKey()));

              if (withMetaData) {
                appendObjectSeparator();
                appendObjectKeyValue(quote("hash"), String.valueOf(rtx.getHash()));
                appendObjectSeparator().appendObjectKeyValue(quote("type"), quote(rtx.getKind().toString()))
                                       .appendObjectSeparator()
                                       .appendObjectKeyValue(quote("descendantCount"),
                                                             String.valueOf(rtx.getDescendantCount()));
              }

              appendObjectEnd(hasChildren).appendObjectSeparator();
            }

            appendObjectKey(quote("value"));
          } else {
            appendObjectKey(quote(rtx.getName().stringValue()));
          }
          break;
        case BOOLEAN_VALUE:
        case OBJECT_BOOLEAN_VALUE:
          emitMetaData(rtx);
          appendObjectValue(Boolean.valueOf(rtx.getValue()).toString());
          if (withMetaData || withNodeKeyMetaData) {
            appendObjectEnd(true);
          }
          printCommaIfNeeded(rtx);
          break;
        case NULL_VALUE:
        case OBJECT_NULL_VALUE:
          emitMetaData(rtx);
          appendObjectValue("null");
          if (withMetaData || withNodeKeyMetaData) {
            appendObjectEnd(true);
          }
          printCommaIfNeeded(rtx);
          break;
        case NUMBER_VALUE:
        case OBJECT_NUMBER_VALUE:
          emitMetaData(rtx);
          appendObjectValue(rtx.getValue());
          if (withMetaData || withNodeKeyMetaData) {
            appendObjectEnd(true);
          }
          printCommaIfNeeded(rtx);
          break;
        case STRING_VALUE:
        case OBJECT_STRING_VALUE:
          emitMetaData(rtx);
          appendObjectValue(quote(StringValue.escape(rtx.getValue())));
          if (withMetaData || withNodeKeyMetaData) {
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

  private boolean shouldEmitChildren(boolean hasChildren) {
    return (visitor == null && hasChildren) || (visitor != null && hasChildren && currentLevel() + 1 < maxLevel());
  }

  private void emitMetaData(JsonNodeReadOnlyTrx rtx) throws IOException {
    if (withMetaData || withNodeKeyMetaData) {
      appendObjectStart(true).appendObjectKey(quote("metadata")).appendObjectStart(true);

      if (withNodeKeyMetaData) {
        appendObjectKeyValue(quote("nodeKey"), String.valueOf(rtx.getNodeKey()));

        if (withMetaData) {
          appendObjectSeparator();
        }
      }

      if (withMetaData) {
        appendObjectKeyValue(quote("hash"), String.valueOf(rtx.getHash()));
        appendObjectSeparator().appendObjectKeyValue(quote("type"), quote(rtx.getKind().toString()));

        if (rtx.getKind() == NodeKind.OBJECT || rtx.getKind() == NodeKind.ARRAY) {
          appendObjectSeparator().appendObjectKeyValue(quote("descendantCount"),
                                                       String.valueOf(rtx.getDescendantCount()))
                                 .appendObjectSeparator()
                                 .appendObjectKeyValue(quote("childCount"), String.valueOf(rtx.getChildCount()));
        }
      }

      appendObjectEnd(true).appendObjectSeparator().appendObjectKey(quote("value"));
    }
  }

  @Override
  protected void setTrxForVisitor(JsonNodeReadOnlyTrx rtx) {
    castVisitor().setTrx(rtx);
  }

  private long maxLevel() {
    return castVisitor().getMaxLevel();
  }

  private JsonMaxLevelVisitor castVisitor() {
    return (JsonMaxLevelVisitor) visitor;
  }

  private long currentLevel() {
    return castVisitor().getCurrentLevel();
  }

  @Override
  protected boolean isSubtreeGoingToBeVisited(final JsonNodeReadOnlyTrx rtx) {
    if (rtx.isObjectKey())
      return true;
    return visitor == null || currentLevel() + 1 < maxLevel();
  }

  @Override
  protected boolean isSubtreeGoingToBePruned(final JsonNodeReadOnlyTrx rtx) {
    if (rtx.isObjectKey())
      return false;
    if (visitor == null) {
      return false;
    } else {
      return currentLevel() + 1 >= maxLevel();
    }
  }

  private void printCommaIfNeeded(final JsonNodeReadOnlyTrx rtx) throws IOException {
    final boolean hasRightSibling = rtx.hasRightSibling();

    if (hasRightSibling && rtx.getNodeKey() != startNodeKey)
      appendObjectSeparator();
  }

  /**
   * Emit end element.
   *
   * @param rtx Sirix {@link JsonNodeReadOnlyTrx}
   */
  @Override
  protected void emitEndNode(final JsonNodeReadOnlyTrx rtx) {
    try {
      switch (rtx.getKind()) {
        case ARRAY:
          if (withMetaData || withNodeKeyMetaData) {
            appendArrayEnd(true).appendObjectEnd(true);
          } else {
            appendArrayEnd(shouldEmitChildren(rtx.hasChildren()));
          }

          if (rtx.hasRightSibling() && rtx.getNodeKey() != startNodeKey) {
            appendObjectSeparator();
          }
          break;
        case OBJECT:
          if (withMetaData || withNodeKeyMetaData) {
            appendArrayEnd(true).appendObjectEnd(true);
          } else {
            appendObjectEnd(shouldEmitChildren(rtx.hasChildren()));
          }

          if (rtx.hasRightSibling() && rtx.getNodeKey() != startNodeKey) {
            appendObjectSeparator();
          }
          break;
        case OBJECT_KEY:
          if (withMetaData || withNodeKeyMetaData || (hadToAddBracket && rtx.getNodeKey() == startNodeKey)) {
            appendObjectEnd(true);
          }
          if (rtx.hasRightSibling() && rtx.getNodeKey() != startNodeKey) {
            appendObjectSeparator();
          }
          break;
        // $CASES-OMITTED$
        default:
      }
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  @Override
  protected void emitStartDocument() {
    try {
      final int length = (revisions.length == 1 && revisions[0] < 0)
          ? resMgr.getMostRecentRevisionNumber()
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
          ? resMgr.getMostRecentRevisionNumber()
          : revisions.length;

      if (length > 1) {
        if (indent) {
          stack.pop();
        }

        appendArrayEnd(true).appendObjectEnd(true);
      }
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  @Override
  protected void emitRevisionStartNode(final @Nonnull JsonNodeReadOnlyTrx rtx) {
    try {
      final int length = (revisions.length == 1 && revisions[0] < 0)
          ? resMgr.getMostRecentRevisionNumber()
          : revisions.length;

      if (emitXQueryResultSequence || length > 1) {
        appendObjectStart(rtx.hasChildren()).appendObjectKeyValue(quote("revisionNumber"),
                                                                  Integer.toString(rtx.getRevisionNumber()))
                                            .appendObjectSeparator();

        if (serializeTimestamp) {
          appendObjectKeyValue(quote("revisionTimestamp"),
                               quote(DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC)
                                                                  .format(rtx.getRevisionTimestamp()))).appendObjectSeparator();
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
  protected void emitRevisionEndNode(final @Nonnull JsonNodeReadOnlyTrx rtx) {
    try {
      final int length = (revisions.length == 1 && revisions[0] < 0)
          ? resMgr.getMostRecentRevisionNumber()
          : revisions.length;

      if (emitXQueryResultSequence || length > 1) {
        if (rtx.moveToDocumentRoot().trx().hasFirstChild())
          stack.pop();
        appendObjectEnd(rtx.hasChildren());

        if (hasMoreRevisionsToSerialize(rtx))
          appendObjectSeparator();
      }
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  private boolean hasMoreRevisionsToSerialize(final JsonNodeReadOnlyTrx rtx) {
    return rtx.getRevisionNumber() < revisions[revisions.length - 1] || (revisions.length == 1 && revisions[0] == -1
        && rtx.getRevisionNumber() < rtx.getResourceManager().getMostRecentRevisionNumber());
  }

  /**
   * Indentation of output.
   *
   * @throws IOException if can't indent output
   */
  private JsonSerializer indent() throws IOException {
    if (indent) {
      for (int i = 0; i < currentIndent; i++) {
        out.append(" ");
      }
    }
    return this;
  }

  private JsonSerializer newLine() throws IOException {
    if (indent) {
      out.append("\n");
      indent();
    }
    return this;
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

  private JsonSerializer appendArrayStart(final boolean hasChildren) throws IOException {
    out.append('[');
    if (hasChildren) {
      currentIndent += indentSpaces;
      newLine();
    }
    return this;
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

  private JsonSerializer appendObjectValue(String value) throws IOException {
    out.append(value);
    return this;
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

  private JsonSerializer appendObjectSeparator() throws IOException {
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

      try (final JsonResourceManager resMgr = db.openResourceManager("shredded");
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
   * @param resMgr    Sirix {@link ResourceManager}
   * @param stream    {@link OutputStream} to write to
   * @param revisions revisions to serialize
   */
  public static Builder newBuilder(final JsonResourceManager resMgr, final Writer stream, final int... revisions) {
    return new Builder(resMgr, stream, revisions);
  }

  /**
   * Constructor.
   *
   * @param resMgr     Sirix {@link ResourceManager}
   * @param nodeKey    root node key of subtree to shredder
   * @param stream     {@link OutputStream} to write to
   * @param properties {@link XmlSerializerProperties} to use
   * @param revisions  revisions to serialize
   */
  public static Builder newBuilder(final JsonResourceManager resMgr, final @Nonnegative long nodeKey,
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
    private final JsonResourceManager resourceMgr;

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
    private long nodeKey;

    /**
     * Determines if an initial indent is needed or not.
     */
    private boolean tnitialIndent;

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
     * Determines if nodeKey meta data should be serialized or not.
     */
    private boolean withNodeKey;

    /**
     * Constructor, setting the necessary stuff.
     *
     * @param resourceMgr Sirix {@link ResourceManager}
     * @param stream      {@link OutputStream} to write to
     * @param revisions   revisions to serialize
     */
    public Builder(final JsonResourceManager resourceMgr, final Appendable stream, final int... revisions) {
      maxLevel = -1;
      nodeKey = 0;
      this.resourceMgr = checkNotNull(resourceMgr);
      this.stream = checkNotNull(stream);
      if (revisions == null || revisions.length == 0) {
        version = this.resourceMgr.getMostRecentRevisionNumber();
      } else {
        version = revisions[0];
        versions = new int[revisions.length - 1];
        System.arraycopy(revisions, 1, versions, 0, revisions.length - 1);
      }
    }

    /**
     * Constructor.
     *
     * @param resourceMgr Sirix {@link ResourceManager}
     * @param nodeKey     root node key of subtree to shredder
     * @param stream      {@link OutputStream} to write to
     * @param properties  {@link XmlSerializerProperties} to use
     * @param revisions   revisions to serialize
     */
    public Builder(final JsonResourceManager resourceMgr, final @Nonnegative long nodeKey, final Writer stream,
        final JsonSerializerProperties properties, final int... revisions) {
      checkArgument(nodeKey >= 0, "nodeKey must be >= 0!");
      maxLevel = -1;
      this.resourceMgr = checkNotNull(resourceMgr);
      this.nodeKey = nodeKey;
      this.stream = checkNotNull(stream);
      if (revisions == null || revisions.length == 0) {
        version = this.resourceMgr.getMostRecentRevisionNumber();
      } else {
        version = revisions[0];
        versions = new int[revisions.length - 1];
        System.arraycopy(revisions, 1, versions, 0, revisions.length - 1);
      }
      final ConcurrentMap<?, ?> map = checkNotNull(properties.getProps());
      indent = checkNotNull((Boolean) map.get(S_INDENT[0]));
      indentSpaces = checkNotNull((Integer) map.get(S_INDENT_SPACES[0]));
    }

    /**
     * Specify the start node key.
     *
     * @param nodeKey node key to start serialization from (the root of the subtree to serialize)
     * @return this XMLSerializerBuilder reference
     */
    public Builder startNodeKey(final long nodeKey) {
      this.nodeKey = nodeKey;
      return this;
    }

    /**
     * Specify the maximum level.
     *
     * @param maxLevel the maximum level until which to serialize
     * @return this XMLSerializerBuilder reference
     */
    public Builder maxLevel(final long maxLevel) {
      this.maxLevel = maxLevel;
      return this;
    }

    /**
     * Sets an initial indentation.
     *
     * @return this {@link Builder} instance
     */
    public Builder withInitialIndent() {
      tnitialIndent = true;
      return this;
    }

    /**
     * Sets if the serialization is used for XQuery result sets.
     *
     * @return this {@link Builder} instance
     */
    public Builder isXQueryResultSequence() {
      emitXQueryResultSequence = true;
      return this;
    }

    /**
     * Sets if the serialization of timestamps of the revision(s) is used or not.
     *
     * @return this {@link Builder} instance
     */
    public Builder serializeTimestamp(boolean serializeTimestamp) {
      this.serializeTimestamp = serializeTimestamp;
      return this;
    }

    /**
     * Sets if metadata should be serialized or not.
     *
     * @return this {@link Builder} instance
     */
    public Builder withMetaData(boolean withMetaData) {
      this.withMetaData = withMetaData;
      this.withNodeKey = true;
      return this;
    }

    /**
     * Sets if nodeKey metadata should be serialized or not.
     *
     * @return this {@link Builder} instance
     */
    public Builder withNodeKeyMetaData(boolean withNodeKey) {
      this.withNodeKey = withNodeKey;
      return this;
    }

    /**
     * Pretty prints the output.
     *
     * @return this {@link Builder} instance
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
      checkNotNull(revisions);

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
      return new JsonSerializer(resourceMgr, nodeKey, this, tnitialIndent, version, versions);
    }
  }
}
