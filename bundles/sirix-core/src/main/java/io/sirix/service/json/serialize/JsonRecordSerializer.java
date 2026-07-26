package io.sirix.service.json.serialize;

import io.sirix.api.ResourceSession;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.brackit.query.util.serialize.Serializer;
import io.sirix.service.xml.serialize.XmlSerializerProperties;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;

import static io.sirix.utils.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static io.sirix.service.xml.serialize.XmlSerializerProperties.S_INDENT;
import static io.sirix.service.xml.serialize.XmlSerializerProperties.S_INDENT_SPACES;

public final class JsonRecordSerializer implements Callable<Void> {

  private final long maxLevel;

  private final JsonResourceSession resourceMgr;

  private final int numberOfRecords;

  private final long numberOfNodes;

  private final long maxChildNodes;

  private final long startNodeKey;

  private enum State {
    IS_OBJECT,

    IS_ARRAY,

    IS_PRIMITIVE
  }

  /**
   * OutputStream to write to.
   */
  private final Appendable out;

  private final boolean serializeTimestamp;

  private final boolean withMetaData;

  private final boolean withNodeKeyMetaData;

  private final boolean withNodeKeyAndChildNodeKeyMetaData;

  /**
   * Array with versions to print.
   */
  private final int[] revisions;

  /**
   * Initialize XMLStreamReader implementation with transaction. The cursor points to the node the
   * XMLStreamReader starts to read.
   *
   * @param resourceMgr resource session to read the resource
   * @param builder builder of XML Serializer
   * @param revision revision to serialize
   * @param revisions further revisions to serialize
   */
  private JsonRecordSerializer(final JsonResourceSession resourceMgr, final Builder builder,
      final int revision, final int... revisions) {
    this.numberOfRecords = builder.numberOfRecords;
    this.revisions = revisions == null
        ? new int[1]
        : new int[revisions.length + 1];
    this.resourceMgr = resourceMgr;
    initialize(revision, revisions);
    maxLevel = builder.maxLevel;
    maxChildNodes = builder.maxChildNodes;
    out = builder.stream;
    serializeTimestamp = builder.serializeTimestamp;
    withMetaData = builder.withMetaData;
    withNodeKeyMetaData = builder.withNodeKey;
    withNodeKeyAndChildNodeKeyMetaData = builder.withNodeKeyAndChildCount;
    numberOfNodes = builder.maxNodes;
    startNodeKey = builder.nodeKey;
  }

  /**
   * Initialize.
   *
   * @param revision first revision to serialize
   * @param revisions revisions to serialize
   */
  private void initialize(final int revision, final int... revisions) {
    this.revisions[0] = revision;
    if (revisions != null) {
      System.arraycopy(revisions, 0, this.revisions, 1, revisions.length);
    }
  }

  /**
   * Constructor, setting the necessary stuff.
   *
   * @param resMgr Sirix {@link ResourceSession}
   * @param numberOfRecords number of records to serialize
   * @param writer {@link Writer} to write to
   * @param revisions revisions to serialize
   */
  public static Builder newBuilder(final JsonResourceSession resMgr, final int numberOfRecords, final Writer writer,
      final int... revisions) {
    return new Builder(resMgr, numberOfRecords, writer, revisions);
  }

  /**
   * Byte-pipeline builder: serializes UTF-8 straight to the stream. The record serializer's own
   * envelope/wrapper writes go through the sink's Appendable bridge (ASCII fast path), and the
   * inner per-record {@link JsonSerializer}s receive the SAME sink (see the sink pass-through in
   * {@code JsonSerializer}'s constructor), so escape-free stored string values bulk-copy to the
   * output without String construction or char→byte re-encoding.
   *
   * @param resMgr Sirix {@link ResourceSession}
   * @param numberOfRecords number of records to serialize
   * @param stream {@link OutputStream} to write UTF-8 bytes to
   * @param revisions revisions to serialize
   */
  public static Builder newBuilder(final JsonResourceSession resMgr, final int numberOfRecords,
      final OutputStream stream, final int... revisions) {
    return new Builder(resMgr, numberOfRecords, new JsonOutputSink.Utf8OutputSink(requireNonNull(stream)), revisions);
  }

  /**
   * Constructor.
   *
   * @param resMgr Sirix {@link ResourceSession}
   * @param numberOfRecords number of records to serialize
   * @param nodeKey root node key of subtree to shredder
   * @param writer {@link OutputStream} to write to
   * @param properties {@link XmlSerializerProperties} to use
   * @param revisions revisions to serialize
   */
  public static Builder newBuilder(final JsonResourceSession resMgr, final int numberOfRecords,
      final long nodeKey, final Writer writer, final JsonSerializerProperties properties,
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
     * Resource session to use.
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

    private long maxNodes = Long.MAX_VALUE;

    private long maxChildNodes = Long.MAX_VALUE;

    /**
     * Constructor, setting the necessary stuff.
     *
     * @param resourceMgr Sirix {@link ResourceSession}
     * @param numberOfRecords number of records to serialize
     * @param stream {@link OutputStream} to write to
     * @param revisions revisions to serialize
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
     * @param resourceMgr Sirix {@link ResourceSession}
     * @param numberOfRecords number of records to serialize
     * @param nodeKey root node key of subtree to shredder
     * @param stream {@link OutputStream} to write to
     * @param properties {@link XmlSerializerProperties} to use
     * @param revisions revisions to serialize
     */
    public Builder(final JsonResourceSession resourceMgr, final int numberOfRecords, final long nodeKey,
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
   * Serialize records. Two modes of operation:
   * <ul>
   * <li>Initial Load Mode (startNodeKey == 0): Serialize from document root with parent wrapper</li>
   * <li>Pagination Mode (startNodeKey > 0): Serialize right siblings of startNodeKey as array</li>
   * </ul>
   *
   * <p>A single revision serializes as the bare record payload (the wire shape paginating
   * clients consume). Multiple revisions get the same {@code {"sirix":[{"revisionNumber":N,
   * "revision":…},…]}} envelope {@link JsonSerializer} emits — the previous code concatenated
   * one full payload per revision with no separator (invalid JSON) and handed ALL revisions to
   * every inner per-record serializer (which then re-serialized every revision per record).
   */
  public Void call() {
    try {
      serializeRecords();
    } catch (final RuntimeException | Error e) {
      // Mirror JsonSerializer.call(): drain what was buffered in the shared sink, but never
      // let a flush failure REPLACE the primary exception.
      try {
        flushSink();
      } catch (final IOException flushFailure) {
        e.addSuppressed(flushFailure);
      }
      throw e;
    }
    try {
      flushSink();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    return null;
  }

  /**
   * Flushes the byte sink once after the final emit — covers every path out of
   * {@link #serializeRecords()}, including its early returns. Writer/Appendable-based builders
   * are unaffected: their writes were never buffered here (the inner serializers flush their own
   * private buffers at the end of each call).
   */
  private void flushSink() throws IOException {
    if (out instanceof JsonOutputSink sink) {
      sink.flush();
    }
  }

  private void serializeRecords() {
    final int nrOfRevisions = revisions.length;
    final int length = (nrOfRevisions == 1 && revisions[0] < 0)
        ? resourceMgr.getMostRecentRevisionNumber()
        : nrOfRevisions;

    try {
      final boolean envelope = length > 1;
      if (envelope) {
        out.append("{\"sirix\":[");
      }

      for (int i = 1; i <= length; i++) {
        final int currentRevision = (nrOfRevisions == 1 && revisions[0] < 0)
            ? i
            : revisions[i - 1];
        try (final JsonNodeReadOnlyTrx rtx = resourceMgr.beginNodeReadOnlyTrx(currentRevision)) {
          if (envelope) {
            if (i > 1) {
              out.append(',');
            }
            out.append("{\"revisionNumber\":").append(Integer.toString(rtx.getRevisionNumber()));
            if (serializeTimestamp) {
              out.append(",\"revisionTimestamp\":\"")
                 .append(DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC).format(rtx.getRevisionTimestamp()))
                 .append('"');
            }
            out.append(",\"revision\":");
          }

          if (startNodeKey > 0) {
            // PAGINATION MODE: Serialize right siblings of startNodeKey as array
            serializeSiblingsAfter(rtx, currentRevision);
          } else {
            // INITIAL LOAD MODE: Serialize from document root with parent wrapper
            serializeWithParentWrapper(rtx, currentRevision);
          }

          if (envelope) {
            out.append('}');
          }
        }
      }

      if (envelope) {
        out.append("]}");
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Pagination mode: Serialize right siblings of startNodeKey as a JSON array with parent metadata.
   * Output format: {"metadata":{parentNodeKey, childCount, ...}, "value":[{sibling1}, {sibling2},
   * ...]}
   * 
   * @param rtx the read-only transaction
   * @throws IOException if serialization fails
   */
  private void serializeSiblingsAfter(final JsonNodeReadOnlyTrx rtx, final int revision) throws IOException {
    // Move to the start node (last loaded child)
    if (!rtx.moveTo(startNodeKey)) {
      out.append("{\"value\":[]}");
      return;
    }

    // Pre-compute metadata flag for efficiency
    final boolean hasMetadata = withMetaData || withNodeKeyAndChildNodeKeyMetaData || withNodeKeyMetaData;

    // Emit parent metadata wrapper
    if (rtx.hasParent()) {
      rtx.moveToParent();
      if (hasMetadata) {
        emitParentMetadata(rtx);
      } else {
        out.append("{\"value\":[");
      }

      // Move back to start node
      rtx.moveTo(startNodeKey);
    } else {
      out.append("{\"value\":[");
    }

    // Move to right sibling (first new child to serialize)
    if (!rtx.hasRightSibling()) {
      out.append("]}");
      return;
    }
    rtx.moveToRightSibling();

    // Create builder ONCE with all common settings - only startNodeKey changes per sibling.
    // Inner serializers get ONLY the revision this payload is for — handing them the full
    // revisions array made every record re-serialize every requested revision.
    final JsonSerializer.Builder builder =
        new JsonSerializer.Builder(rtx.getResourceSession(), out, revision).serializeStartNodeWithBrackets(false)
                                                                            .maxLevel(maxLevel)
                                                                            .maxChildren(maxChildNodes)
                                                                            .serializeTimestamp(serializeTimestamp)
                                                                            .withMetaData(withMetaData)
                                                                            .withNodeKeyAndChildCountMetaData(
                                                                                withNodeKeyAndChildNodeKeyMetaData)
                                                                            .withNodeKeyMetaData(withNodeKeyMetaData)
                                                                            .numberOfNodes(numberOfNodes);

    int count = 0;
    do {
      if (count > 0) {
        out.append(',');
      }

      final long nodeKey = rtx.getNodeKey();
      final boolean isObjectKey = rtx.isObjectKey();

      // ObjectKey nodes ALWAYS need wrapper {} for valid JSON array elements
      // e.g., [{"key1": value1}, {"key2": value2}] instead of ["key1": value1, ...]
      if (isObjectKey) {
        out.append('{');
      }

      // Serialize this sibling's subtree - only update startNodeKey, reuse everything else
      builder.startNodeKey(nodeKey).build().call();

      // Close ObjectKey wrapper
      if (isObjectKey) {
        out.append('}');
      }

      // Restore cursor position to this sibling (serialization may have moved it)
      rtx.moveTo(nodeKey);
      count++;

      // Check if we should continue
      if (count >= numberOfRecords) {
        break;
      }
    } while (rtx.hasRightSibling() && rtx.moveToRightSibling());

    out.append("]}");
  }

  /**
   * Initial load mode: Serialize from document root with parent wrapper. Output format:
   * {"metadata":{...}, "value":[{child1}, {child2}, ...]}
   * 
   * @param rtx the read-only transaction
   * @throws IOException if serialization fails
   */
  private void serializeWithParentWrapper(final JsonNodeReadOnlyTrx rtx, final int revision) throws IOException {
    var state = State.IS_PRIMITIVE;

    rtx.moveToDocumentRoot();
    if (!rtx.hasFirstChild()) {
      return;
    }
    rtx.moveToFirstChild();

    if (rtx.getNodeKey() <= 0) {
      return;
    }

    // Pre-compute metadata flag for efficiency
    final boolean hasMetadata = withMetaData || withNodeKeyAndChildNodeKeyMetaData || withNodeKeyMetaData;

    if (rtx.isObject()) {
      state = State.IS_OBJECT;
    } else if (rtx.isArray()) {
      state = State.IS_ARRAY;
    }

    // Emit the parent wrapper manually to avoid the extra '{' that emitNode adds for OBJECT
    // which would conflict with separately serialized children
    if (hasMetadata) {
      emitParentMetadata(rtx);
    } else {
      // No metadata - just emit the opening bracket
      if (state == State.IS_OBJECT) {
        out.append('{');
      } else if (state == State.IS_ARRAY) {
        out.append('[');
      }
    }

    // Create builder for serialization (single revision — see serializeSiblingsAfter)
    final JsonSerializer.Builder builder =
        new JsonSerializer.Builder(rtx.getResourceSession(), out, revision).serializeStartNodeWithBrackets(false)
                                                                            .maxLevel(maxLevel)
                                                                            .maxChildren(maxChildNodes)
                                                                            .serializeTimestamp(serializeTimestamp)
                                                                            .withMetaData(withMetaData)
                                                                            .withNodeKeyAndChildCountMetaData(
                                                                                withNodeKeyAndChildNodeKeyMetaData)
                                                                            .withNodeKeyMetaData(withNodeKeyMetaData)
                                                                            .numberOfNodes(numberOfNodes);

    // For primitives, serialize the node itself (not children, which don't exist)
    if (state == State.IS_PRIMITIVE) {
      builder.startNodeKey(rtx.getNodeKey()).build().call();
    } else if (rtx.hasFirstChild()) {
      // Serialize children for objects/arrays
      rtx.moveToFirstChild();

      int count = 0;
      do {
        if (count > 0) {
          out.append(',');
        }

        final long nodeKey = rtx.getNodeKey();
        final boolean isObjectKey = rtx.isObjectKey();
        // When metadata is enabled, ObjectKey children need {} wrapping because:
        // 1. We set startNodeKey to each child, so serializer skips its auto-wrapping logic
        // 2. ObjectKey format with metadata is {"key":"...", "metadata":{...}, "value":...}
        final boolean needsWrapper = isObjectKey && hasMetadata;

        if (needsWrapper) {
          out.append('{');
        }

        builder.startNodeKey(nodeKey).build().call();

        if (needsWrapper) {
          out.append('}');
        }

        rtx.moveTo(nodeKey);
        count++;

        if (count >= numberOfRecords) {
          break;
        }
      } while (rtx.hasRightSibling() && rtx.moveToRightSibling());
    }

    // Close parent structure
    if (state == State.IS_OBJECT) {
      if (hasMetadata) {
        // Close {"metadata":{...},"value":[...]}
        out.append("]}");
      } else {
        out.append('}');
      }
    } else if (state == State.IS_ARRAY) {
      if (hasMetadata) {
        // Close {"metadata":{...},"value":[...]}
        out.append("]}");
      } else {
        out.append(']');
      }
    } else if (hasMetadata) {
      // IS_PRIMITIVE with metadata: close {"metadata":{...},"value":[...]}
      out.append("]}");
    }
  }

  /**
   * Emit parent node metadata wrapper. Output format: {"metadata":{nodeKey:X,...},"value":[
   * 
   * Always uses array wrapper for value to ensure consistency between initial load and pagination
   * responses.
   *
   * @param rtx the read-only transaction
   */
  private void emitParentMetadata(final JsonNodeReadOnlyTrx rtx) throws IOException {
    out.append("{\"metadata\":{");

    // Node key
    if (withNodeKeyMetaData || withNodeKeyAndChildNodeKeyMetaData) {
      out.append("\"nodeKey\":").append(String.valueOf(rtx.getNodeKey()));
    }

    // Hash and type (for full metadata)
    if (withMetaData) {
      if (withNodeKeyMetaData || withNodeKeyAndChildNodeKeyMetaData) {
        out.append(',');
      }
      if (rtx.getHash() != 0L) {
        out.append("\"hash\":\"").append(String.format("%016x", rtx.getHash())).append("\",");
      }
      out.append("\"type\":\"").append(rtx.getKind().toString()).append('"');
      if (rtx.getHash() != 0L) {
        out.append(",\"descendantCount\":").append(String.valueOf(rtx.getDescendantCount()));
      }
    }

    // Child count (for nodeKeyAndChildCount metadata)
    // When withNodeKeyAndChildNodeKeyMetaData is true, nodeKey was already output above,
    // so we always need a comma before childCount
    if (withNodeKeyAndChildNodeKeyMetaData) {
      out.append(",\"childCount\":").append(String.valueOf(rtx.getChildCount()));
    }

    out.append("},\"value\":[");
  }
}
