package org.sirix.service.json.serialize;

import org.brackit.xquery.util.serialize.Serializer;
import org.sirix.api.ResourceSession;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonResourceSession;
import org.sirix.service.xml.serialize.XmlSerializerProperties;

import org.checkerframework.checker.index.qual.NonNegative;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.sirix.service.xml.serialize.XmlSerializerProperties.S_INDENT;
import static org.sirix.service.xml.serialize.XmlSerializerProperties.S_INDENT_SPACES;

public final class JsonRecordSerializer implements Callable<Void> {

  private final long maxLevel;

  private final JsonResourceSession resourceMgr;

  private final int numberOfRecords;

  private final long numberOfNodes;

  private final long maxChildNodes;

  private enum State {
    IS_OBJECT,

    IS_ARRAY,

    IS_PRIMITIVE
  }

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

  private final boolean serializeTimestamp;

  private final boolean withMetaData;

  private final boolean withNodeKeyMetaData;

  private final boolean withNodeKeyAndChildNodeKeyMetaData;

  private boolean hadToAddBracket;

  private int currentIndent;

  /**
   * Array with versions to print.
   */
  private final int[] revisions;

  private long lastTopLevelNodeKey;

  /**
   * Initialize XMLStreamReader implementation with transaction. The cursor points to the node the
   * XMLStreamReader starts to read.
   *
   * @param resourceMgr resource manager to read the resource
   * @param builder     builder of XML Serializer
   * @param revision    revision to serialize
   * @param revisions   further revisions to serialize
   */
  private JsonRecordSerializer(final JsonResourceSession resourceMgr, final Builder builder,
      final @NonNegative int revision, final int... revisions) {
    this.numberOfRecords = builder.numberOfRecords;
    this.revisions = revisions == null ? new int[1] : new int[revisions.length + 1];
    this.resourceMgr = resourceMgr;
    initialize(revision, revisions);
    maxLevel = builder.maxLevel;
    maxChildNodes = builder.maxChildNodes;
    out = builder.stream;
    indent = builder.indent;
    indentSpaces = builder.indentSpaces;
    withInitialIndent = builder.initialIndent;
    serializeTimestamp = builder.serializeTimestamp;
    withMetaData = builder.withMetaData;
    withNodeKeyMetaData = builder.withNodeKey;
    withNodeKeyAndChildNodeKeyMetaData = builder.withNodeKeyAndChildCount;
    lastTopLevelNodeKey = builder.lastTopLevelNodeKey;
    numberOfNodes = builder.maxNodes;
  }

  /**
   * Initialize.
   *
   * @param revision  first revision to serialize
   * @param revisions revisions to serialize
   */
  private void initialize(final @NonNegative int revision, final int... revisions) {
    this.revisions[0] = revision;
    if (revisions != null) {
      System.arraycopy(revisions, 0, this.revisions, 1, revisions.length);
    }
  }

  /**
   * Constructor, setting the necessary stuff.
   *
   * @param resMgr          Sirix {@link ResourceSession}
   * @param numberOfRecords number of records to serialize
   * @param writer          {@link Writer} to write to
   * @param revisions       revisions to serialize
   */
  public static Builder newBuilder(final JsonResourceSession resMgr, final int numberOfRecords, final Writer writer,
      final int... revisions) {
    return new Builder(resMgr, numberOfRecords, writer, revisions);
  }

  /**
   * Constructor.
   *
   * @param resMgr          Sirix {@link ResourceSession}
   * @param numberOfRecords number of records to serialize
   * @param nodeKey         root node key of subtree to shredder
   * @param writer          {@link OutputStream} to write to
   * @param properties      {@link XmlSerializerProperties} to use
   * @param revisions       revisions to serialize
   */
  public static Builder newBuilder(final JsonResourceSession resMgr, final int numberOfRecords,
      final @NonNegative long nodeKey, final Writer writer, final JsonSerializerProperties properties,
      final int... revisions) {
    return new Builder(resMgr, numberOfRecords, nodeKey, writer, properties, revisions);
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

    private final int numberOfRecords;

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
    private boolean initialIndent;

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
    private long maxLevel = Long.MAX_VALUE;

    /**
     * Determines if nodeKey meta data should be serialized or not.
     */
    private boolean withNodeKey;

    /**
     * Determines if childCount meta data should be serialized or not.
     */
    private boolean withNodeKeyAndChildCount;

    private long lastTopLevelNodeKey;

    private long maxNodes = Long.MAX_VALUE;

    private long maxChildNodes = Long.MAX_VALUE;

    /**
     * Constructor, setting the necessary stuff.
     *
     * @param resourceMgr     Sirix {@link ResourceSession}
     * @param numberOfRecords number of records to serialize
     * @param stream          {@link OutputStream} to write to
     * @param revisions       revisions to serialize
     */
    public Builder(final JsonResourceSession resourceMgr, final int numberOfRecords, final Appendable stream,
        final int... revisions) {
      this.numberOfRecords = numberOfRecords;
      nodeKey = 0;
      this.resourceMgr = requireNonNull(resourceMgr);
      this.stream = requireNonNull(stream);
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
     * @param resourceMgr     Sirix {@link ResourceSession}
     * @param numberOfRecords number of records to serialize
     * @param nodeKey         root node key of subtree to shredder
     * @param stream          {@link OutputStream} to write to
     * @param properties      {@link XmlSerializerProperties} to use
     * @param revisions       revisions to serialize
     */
    public Builder(final JsonResourceSession resourceMgr, final int numberOfRecords, final @NonNegative long nodeKey,
        final Writer stream, final JsonSerializerProperties properties, final int... revisions) {
      checkArgument(nodeKey >= 0, "nodeKey must be >= 0!");
      this.numberOfRecords = numberOfRecords;
      maxLevel = -1;
      this.resourceMgr = requireNonNull(resourceMgr);
      this.nodeKey = nodeKey;
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
     * @return this XMLSerializerBuilder reference
     */
    public Builder startNodeKey(final long nodeKey) {
      this.nodeKey = nodeKey;
      return this;
    }

    /**
     * Specify the start node key.
     *
     * @param nodeKey node key to start serialization from (the root of the subtree to serialize)
     * @return this reference
     */
    public Builder lastTopLevelNodeKey(final long nodeKey) {
      this.lastTopLevelNodeKey = nodeKey;
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
     * Sets the max number of child nodes to serialize.
     *
     * @return this reference
     */
    public Builder maxChildren(final long maxChildren) {
      this.maxChildNodes = maxChildren;
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
     * @return this reference
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
    public JsonRecordSerializer build() {
      return new JsonRecordSerializer(resourceMgr, this, version, versions);
    }
  }

  /**
   * Serialize the first {@code numberOfRecords}, that is the first n-nodes of the 1st level.
   */
  public Void call() {
    var state = State.IS_PRIMITIVE;

    final int nrOfRevisions = revisions.length;
    final int length =
        (nrOfRevisions == 1 && revisions[0] < 0) ? resourceMgr.getMostRecentRevisionNumber() : nrOfRevisions;

    for (int i = 1; i <= length; i++) {
      try (final JsonNodeReadOnlyTrx rtx = resourceMgr.beginNodeReadOnlyTrx((nrOfRevisions == 1 && revisions[0] < 0)
                                                                                ? i
                                                                                : revisions[i - 1])) {
        rtx.moveToDocumentRoot();

        if (rtx.hasFirstChild()) {
          rtx.moveToFirstChild();

          var jsonSerializer =
              new JsonSerializer.Builder(rtx.getResourceSession(), out, revisions).startNodeKey(rtx.getNodeKey())
                                                                                  .serializeStartNodeWithBrackets(false)
                                                                                  .serializeTimestamp(serializeTimestamp)
                                                                                  .withMetaData(withMetaData)
                                                                                  .withNodeKeyAndChildCountMetaData(
                                                                                      withNodeKeyAndChildNodeKeyMetaData)
                                                                                  .withNodeKeyMetaData(
                                                                                      withNodeKeyMetaData)
                                                                                  .numberOfNodes(numberOfNodes)
                                                                                  .build();
          jsonSerializer.emitNode(rtx);

          if (rtx.isObject()) {
            state = State.IS_OBJECT;
          } else if (rtx.isArray()) {
            state = State.IS_ARRAY;
          }

          if (rtx.hasFirstChild()) {
            boolean hasMoved = false;
            if (lastTopLevelNodeKey != 0) {
              rtx.moveTo(lastTopLevelNodeKey);
              if (rtx.hasRightSibling()) {
                rtx.moveToRightSibling();
                hasMoved = true;
              }
            } else {
              rtx.moveToFirstChild();
              hasMoved = true;
            }

            if (hasMoved) {
              var nodeKey = rtx.getNodeKey();
              jsonSerializer =
                  new JsonSerializer.Builder(rtx.getResourceSession(), out, revisions).startNodeKey(nodeKey)
                                                                                      .serializeStartNodeWithBrackets(
                                                                                          false)
                                                                                      .maxLevel(maxLevel)
                                                                                      .maxChildren(maxChildNodes)
                                                                                      .serializeTimestamp(
                                                                                          serializeTimestamp)
                                                                                      .withMetaData(withMetaData)
                                                                                      .withNodeKeyAndChildCountMetaData(
                                                                                          withNodeKeyAndChildNodeKeyMetaData)
                                                                                      .withNodeKeyMetaData(
                                                                                          withNodeKeyMetaData)
                                                                                      .numberOfNodes(numberOfNodes)
                                                                                      .build();
              jsonSerializer.call();
              if (rtx.isObjectKey() && (withMetaData || withNodeKeyAndChildNodeKeyMetaData || withNodeKeyMetaData)) {
                out.append("}");
              }
              rtx.moveTo(nodeKey);

              if (rtx.hasRightSibling()) {
                for (int j = 1; j < numberOfRecords && rtx.hasRightSibling(); j++) {
                  rtx.moveToRightSibling();
                  nodeKey = rtx.getNodeKey();
                  out.append(",");
                  if (rtx.isObjectKey() && (withMetaData || withNodeKeyAndChildNodeKeyMetaData
                      || withNodeKeyMetaData)) {
                    out.append("{");
                  }
                  jsonSerializer =
                      new JsonSerializer.Builder(rtx.getResourceSession(), out, revisions).startNodeKey(nodeKey)
                                                                                          .serializeStartNodeWithBrackets(
                                                                                              false)
                                                                                          .maxLevel(maxLevel)
                                                                                          .maxChildren(maxChildNodes)
                                                                                          .serializeTimestamp(
                                                                                              serializeTimestamp)
                                                                                          .withMetaData(withMetaData)
                                                                                          .withNodeKeyAndChildCountMetaData(
                                                                                              withNodeKeyAndChildNodeKeyMetaData)
                                                                                          .withNodeKeyMetaData(
                                                                                              withNodeKeyMetaData)
                                                                                          .numberOfNodes(numberOfNodes)
                                                                                          .build();
                  jsonSerializer.call();
                  if (rtx.isObjectKey() && (withMetaData || withNodeKeyAndChildNodeKeyMetaData
                      || withNodeKeyMetaData)) {
                    out.append("}");
                  }
                  rtx.moveTo(nodeKey);
                }
              }
            }
          }

          if (state == State.IS_OBJECT) {
            if (withMetaData || withNodeKeyAndChildNodeKeyMetaData || withNodeKeyMetaData) {
              out.append("]");
            }
            out.append("}");
          } else if (state == State.IS_ARRAY) {
            if (withMetaData || withNodeKeyAndChildNodeKeyMetaData || withNodeKeyMetaData) {
              out.append("]}");
            } else {
              out.append("]");
            }
          }
        }

      } catch (final IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    return null;
  }
}
