/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.service.xml.serialize;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.sirix.service.xml.serialize.XmlSerializerProperties.S_ID;
import static org.sirix.service.xml.serialize.XmlSerializerProperties.S_INDENT;
import static org.sirix.service.xml.serialize.XmlSerializerProperties.S_INDENT_SPACES;
import static org.sirix.service.xml.serialize.XmlSerializerProperties.S_REST;
import static org.sirix.service.xml.serialize.XmlSerializerProperties.S_XMLDECL;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ConcurrentMap;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.brackit.xquery.util.serialize.Serializer;
import org.sirix.access.DatabaseConfiguration;
import org.sirix.access.Databases;
import org.sirix.access.ResourceConfiguration;
import org.sirix.api.ResourceSession;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.api.xml.XmlResourceSession;
import org.sirix.node.NodeKind;
import org.sirix.settings.CharsForSerializing;
import org.sirix.settings.Constants;
import org.sirix.utils.LogWrapper;
import org.sirix.utils.SirixFiles;
import org.sirix.utils.XMLToken;
import org.slf4j.LoggerFactory;

/**
 *
 * <p>
 * Most efficient way to serialize a subtree into an OutputStream. The encoding always is UTF-8.
 * Note that the OutputStream internally is wrapped by a BufferedOutputStream. There is no need to
 * buffer it again outside of this class.
 * </p>
 */
public final class XmlSerializer extends org.sirix.service.AbstractSerializer<XmlNodeReadOnlyTrx, XmlNodeTrx> {

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory.getLogger(XmlSerializer.class));

  /** Offset that must be added to digit to make it ASCII. */
  private static final int ASCII_OFFSET = 48;

  /** Precalculated powers of each available long digit. */
  private static final long[] LONG_POWERS = {1L, 10L, 100L, 1000L, 10000L, 100000L, 1000000L, 10000000L, 100000000L,
      1000000000L, 10000000000L, 100000000000L, 1000000000000L, 10000000000000L, 100000000000000L, 1000000000000000L,
      10000000000000000L, 100000000000000000L, 1000000000000000000L};

  /** OutputStream to write to. */
  private final OutputStream out;

  /** Indent output. */
  private final boolean indent;

  /** Serialize XML declaration. */
  private final boolean serializeXMLDeclaration;

  /** Serialize rest header and closer and rest:id. */
  private final boolean serializeRest;

  /** Serialize a rest-sequence element for the start-document. */
  private final boolean serializeRestSequence;

  /** Serialize id. */
  private final boolean serializeId;

  /** Number of spaces to indent. */
  private final int indentSpaces;

  /** Determines if serializing with initial indentation. */
  private final boolean withInitialIndent;

  private final boolean emitXQueryResultSequence;

  private final boolean serializeTimestamp;

  private final boolean metaData;

  /**
   * Initialize XMLStreamReader implementation with transaction. The cursor points to the node the
   * XMLStreamReader starts to read.
   *
   * @param resourceMgr resource manager to read the resource
   * @param nodeKey start node key
   * @param builder builder of XML Serializer
   * @param revision revision to serialize
   * @param revsions further revisions to serialize
   */
  private XmlSerializer(final XmlResourceSession resourceMgr, final @NonNegative long nodeKey,
      final XmlSerializerBuilder builder, final boolean initialIndent, final @NonNegative int revision,
      final int... revsions) {
    super(resourceMgr, builder.maxLevel == -1
        ? null
        : new XmlMaxLevelVisitor(builder.maxLevel), nodeKey, revision, revsions);
    out = new BufferedOutputStream(builder.stream, 4096);
    indent = builder.indent;
    serializeXMLDeclaration = builder.declaration;
    serializeRest = builder.rest;
    serializeRestSequence = builder.restSequence;
    serializeId = builder.id;
    indentSpaces = builder.indentSpaces;
    withInitialIndent = builder.initialIndent;
    emitXQueryResultSequence = builder.emitXQueryResultSequence;
    serializeTimestamp = builder.serializeTimestamp;
    metaData = builder.metaData;
  }

  /**
   * Emit node (start element or characters).
   *
   * @param rtx Sirix {@link XmlNodeReadOnlyTrx}
   */
  @Override
  protected void emitNode(final XmlNodeReadOnlyTrx rtx) {
    try {
      switch (rtx.getKind()) {
        case XML_DOCUMENT:
          break;
        case ELEMENT:
          // Emit start element.
          indent();
          out.write(CharsForSerializing.OPEN.getBytes());
          writeQName(rtx);
          final long key = rtx.getNodeKey();
          // Emit namespace declarations.
          for (int index = 0, nspCount = rtx.getNamespaceCount(); index < nspCount; index++) {
            rtx.moveToNamespace(index);
            if (rtx.getPrefixKey() == -1) {
              out.write(CharsForSerializing.XMLNS.getBytes());
              write(rtx.nameForKey(rtx.getURIKey()));
              out.write(CharsForSerializing.QUOTE.getBytes());
            } else {
              out.write(CharsForSerializing.XMLNS_COLON.getBytes());
              write(rtx.nameForKey(rtx.getPrefixKey()));
              out.write(CharsForSerializing.EQUAL_QUOTE.getBytes());
              write(rtx.nameForKey(rtx.getURIKey()));
              out.write(CharsForSerializing.QUOTE.getBytes());
            }
            rtx.moveTo(key);
          }
          // Emit attributes.
          // Add virtual rest:id attribute.
          if (serializeId || metaData) {
            if (serializeRest) {
              out.write(CharsForSerializing.REST_PREFIX.getBytes());
            } else if (revisions.length > 1 || (revisions.length == 1 && revisions[0] == -1)) {
              out.write(CharsForSerializing.SID_PREFIX.getBytes());
            } else {
              out.write(CharsForSerializing.SPACE.getBytes());
            }
            out.write(CharsForSerializing.ID.getBytes());
            out.write(CharsForSerializing.EQUAL_QUOTE.getBytes());
            write(rtx.getNodeKey());
            out.write(CharsForSerializing.QUOTE.getBytes());
          }
          if (metaData) {
            if (serializeRest) {
              out.write(CharsForSerializing.REST_PREFIX.getBytes());
            } else if (revisions.length > 1 || (revisions.length == 1 && revisions[0] == -1)) {
              out.write(CharsForSerializing.SID_PREFIX.getBytes());
            } else {
              out.write(CharsForSerializing.SPACE.getBytes());
            }
            out.write(CharsForSerializing.ID.getBytes());
            out.write(CharsForSerializing.EQUAL_QUOTE.getBytes());
            write(rtx.getNodeKey());
            out.write(CharsForSerializing.QUOTE.getBytes());
          }

          // Iterate over all persistent attributes.
          for (int index = 0, attCount = rtx.getAttributeCount(); index < attCount; index++) {
            rtx.moveToAttribute(index);
            out.write(CharsForSerializing.SPACE.getBytes());
            writeQName(rtx);
            out.write(CharsForSerializing.EQUAL_QUOTE.getBytes());
            out.write(XMLToken.escapeAttribute(rtx.getValue()).getBytes(Constants.DEFAULT_ENCODING));
            out.write(CharsForSerializing.QUOTE.getBytes());
            rtx.moveTo(key);
          }
          if (rtx.hasFirstChild() && (visitor == null || currentLevel() + 1 < maxLevel())) {
            out.write(CharsForSerializing.CLOSE.getBytes());
          } else {
            out.write(CharsForSerializing.SLASH_CLOSE.getBytes());
          }
          if (indent && !(rtx.getFirstChildKind() == NodeKind.TEXT && rtx.getChildCount() == 1)) {
            out.write(CharsForSerializing.NEWLINE.getBytes());
          }
          break;
        case COMMENT:
          indent();
          out.write(CharsForSerializing.OPENCOMMENT.getBytes());
          out.write(XMLToken.escapeContent(rtx.getValue()).getBytes(Constants.DEFAULT_ENCODING));
          if (indent) {
            out.write(CharsForSerializing.NEWLINE.getBytes());
          }
          out.write(CharsForSerializing.CLOSECOMMENT.getBytes());
          break;
        case TEXT:
          if (rtx.hasRightSibling() || rtx.hasLeftSibling())
            indent();
          out.write(XMLToken.escapeContent(rtx.getValue()).getBytes(Constants.DEFAULT_ENCODING));
          if (indent && (rtx.hasRightSibling() || rtx.hasLeftSibling())) {
            out.write(CharsForSerializing.NEWLINE.getBytes());
          }
          break;
        case PROCESSING_INSTRUCTION:
          indent();
          out.write(CharsForSerializing.OPENPI.getBytes());
          writeQName(rtx);
          out.write(CharsForSerializing.SPACE.getBytes());
          out.write(XMLToken.escapeContent(rtx.getValue()).getBytes(Constants.DEFAULT_ENCODING));
          if (indent) {
            out.write(CharsForSerializing.NEWLINE.getBytes());
          }
          out.write(CharsForSerializing.CLOSEPI.getBytes());
          break;
        // $CASES-OMITTED$
        default:
          throw new IllegalStateException("Node kind not known!");
      }
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  @Override
  protected void emitEndNode(final XmlNodeReadOnlyTrx rtx, final boolean lastEndNode) {
    try {
      if (indent && !(rtx.getFirstChildKind() == NodeKind.TEXT && rtx.getChildCount() == 1))
        indent();
      out.write(CharsForSerializing.OPEN_SLASH.getBytes());
      writeQName(rtx);
      out.write(CharsForSerializing.CLOSE.getBytes());
      if (indent) {
        out.write(CharsForSerializing.NEWLINE.getBytes());
      }
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  // Write a QName.
  private void writeQName(final XmlNodeReadOnlyTrx rtx) throws IOException {
    if (rtx.getPrefixKey() != -1) {
      out.write(rtx.rawNameForKey(rtx.getPrefixKey()));
      out.write(CharsForSerializing.COLON.getBytes());
    }
    out.write(rtx.rawNameForKey(rtx.getLocalNameKey()));
  }

  @Override
  protected void emitStartDocument() {
    try {
      if (serializeXMLDeclaration) {
        write("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>");
        if (indent) {
          out.write(CharsForSerializing.NEWLINE.getBytes());
        }
      }

      final int length = (revisions.length == 1 && revisions[0] < 0)
          ? resMgr.getMostRecentRevisionNumber()
          : revisions.length;

      if (serializeRestSequence || length > 1) {
        if (serializeRestSequence) {
          write("<rest:sequence xmlns:rest=\"https://sirix.io/rest\">");
        } else {
          write("<sdb:sirix xmlns:sdb=\"https://sirix.io/rest\">");
        }

        if (indent) {
          out.write(CharsForSerializing.NEWLINE.getBytes());
          stack.push(Constants.NULL_ID_LONG);
        }
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

      if (serializeRestSequence || length > 1) {
        if (indent) {
          stack.popLong();
        }
        indent();

        if (serializeRestSequence) {
          write("</rest:sequence>");
        } else {
          write("</sdb:sirix>");
        }
      }

      out.flush();
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  @Override
  protected void emitRevisionStartNode(final @NonNull XmlNodeReadOnlyTrx rtx) {
    try {
      final int length = (revisions.length == 1 && revisions[0] < 0)
          ? resMgr.getMostRecentRevisionNumber()
          : revisions.length;

      if (serializeRest || length > 1) {
        indent();
        if (serializeRest) {
          write("<rest:item");
        } else {
          write("<sdb:sirix-item");
        }

        if (length > 1 || emitXQueryResultSequence) {
          if (serializeRest) {
            write(" rest:revision=\"");
          } else {
            write(" sdb:revision=\"");
          }
          write(Integer.toString(rtx.getRevisionNumber()));
          write("\"");

          if (serializeTimestamp) {
            if (serializeRest) {
              write(" rest:revisionTimestamp=\"");
            } else {
              write(" sdb:revisionTimestamp=\"");
            }

            write(DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC).format(rtx.getRevisionTimestamp()));
            write("\"");
          }

          write(">");
        } else if (serializeRest) {
          write(">");
        }

        if (rtx.hasFirstChild())
          stack.push(Constants.NULL_ID_LONG);

        if (indent) {
          out.write(CharsForSerializing.NEWLINE.getBytes());
        }
      }
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  @Override
  protected void emitRevisionEndNode(final @NonNull XmlNodeReadOnlyTrx rtx) {
    try {
      final int length = (revisions.length == 1 && revisions[0] < 0)
          ? resMgr.getMostRecentRevisionNumber()
          : revisions.length;

      if (serializeRest || length > 1) {
        if (rtx.moveToDocumentRoot() && rtx.hasFirstChild())
          stack.popLong();
        indent();
        if (serializeRest) {
          write("</rest:item>");
        } else {
          write("</sdb:sirix-item>");
        }
      }

      if (indent) {
        out.write(CharsForSerializing.NEWLINE.getBytes());
      }
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  @Override
  protected void setTrxForVisitor(XmlNodeReadOnlyTrx rtx) {
    castVisitor().setTrx(rtx);
  }

  @Override
  protected boolean areSiblingNodesGoingToBeSkipped(XmlNodeReadOnlyTrx rtx) {
    return false;
  }

  @Override
  protected boolean isSubtreeGoingToBeVisited(final XmlNodeReadOnlyTrx rtx) {
    return visitor == null || currentLevel() + 1 < maxLevel();
  }

  private long maxLevel() {
    return castVisitor().getMaxLevel();
  }

  private XmlMaxLevelVisitor castVisitor() {
    return (XmlMaxLevelVisitor) visitor;
  }

  private long currentLevel() {
    return castVisitor().getCurrentLevel();
  }

  /**
   * Indentation of output.
   *
   * @throws IOException if can't indent output
   */
  private void indent() throws IOException {
    if (indent) {
      final int indentSpaces = withInitialIndent
          ? (stack.size() + 1) * this.indentSpaces
          : stack.size() * this.indentSpaces;
      for (int i = 0; i < indentSpaces; i++) {
        out.write(" ".getBytes(Constants.DEFAULT_ENCODING));
      }
    }
  }

  /**
   * Write characters of string.
   *
   * @param value value to write
   * @throws IOException if can't write to string
   * @throws UnsupportedEncodingException if unsupport encoding
   */
  private void write(final String value) throws UnsupportedEncodingException, IOException {
    out.write(value.getBytes(Constants.DEFAULT_ENCODING));
  }

  /**
   * Write non-negative non-zero long as UTF-8 bytes.
   *
   * @param value value to write
   * @throws IOException if can't write to string
   */
  private void write(final long value) throws IOException {
    final int length = (int) Math.log10(value);
    int digit;
    long remainder = value;
    for (int i = length; i >= 0; i--) {
      digit = (byte) (remainder / LONG_POWERS[i]);
      out.write((byte) (digit + ASCII_OFFSET));
      remainder -= digit * LONG_POWERS[i];
    }
  }

  /**
   * Main method.
   *
   * @param args args[0] specifies the input-TT file/folder; args[1] specifies the output XML file.
   * @throws Exception any exception
   */
  public static void main(final String... args) throws Exception {
    if (args.length < 2 || args.length > 3) {
      throw new IllegalArgumentException("Usage: XMLSerializer input-TT output.xml");
    }

    LOGWRAPPER.info("Serializing '" + args[0] + "' to '" + args[1] + "' ... ");
    final long time = System.nanoTime();
    final Path target = Paths.get(args[1]);
    SirixFiles.recursiveRemove(target);
    Files.createDirectories(target.getParent());
    Files.createFile(target);

    final Path databaseFile = Paths.get(args[0]);
    final DatabaseConfiguration config = new DatabaseConfiguration(databaseFile);
    Databases.createXmlDatabase(config);
    try (final var db = Databases.openXmlDatabase(databaseFile)) {
      db.createResource(ResourceConfiguration.newBuilder("shredded").build());

      try (final XmlResourceSession resMgr = db.beginResourceSession("shredded");
           final FileOutputStream outputStream = new FileOutputStream(target.toFile())) {
        final XmlSerializer serializer = XmlSerializer.newBuilder(resMgr, outputStream).emitXMLDeclaration().build();
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
  public static XmlSerializerBuilder newBuilder(final XmlResourceSession resMgr, final OutputStream stream,
      final int... revisions) {
    return new XmlSerializerBuilder(resMgr, stream, revisions);
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
  public static XmlSerializerBuilder newBuilder(final XmlResourceSession resMgr, final @NonNegative long nodeKey,
      final OutputStream stream, final XmlSerializerProperties properties, final int... revisions) {
    return new XmlSerializerBuilder(resMgr, nodeKey, stream, properties, revisions);
  }

  /**
   * XMLSerializerBuilder to setup the XMLSerializer.
   */
  public static final class XmlSerializerBuilder {
    public boolean restSequence;

    /**
     * Intermediate boolean for indendation, not necessary.
     */
    private boolean indent;

    /**
     * Intermediate boolean for rest serialization, not necessary.
     */
    private boolean rest;

    /**
     * Intermediate boolean for XML-Decl serialization, not necessary.
     */
    private boolean declaration;

    /**
     * Intermediate boolean for ids, not necessary.
     */
    private boolean id;

    /**
     * Intermediate number of spaces to indent, not necessary.
     */
    private int indentSpaces = 2;

    /** Stream to pipe to. */
    private final OutputStream stream;

    /** Resource manager to use. */
    private final XmlResourceSession resourceMgr;

    /** Further revisions to serialize. */
    private int[] versions;

    /** Revision to serialize. */
    private int version;

    /** Node key of subtree to shredder. */
    private long nodeKey;

    private boolean initialIndent;

    private boolean emitXQueryResultSequence;

    private boolean serializeTimestamp;

    private boolean metaData;

    private long maxLevel;

    /**
     * Constructor, setting the necessary stuff.
     *
     * @param resourceMgr Sirix {@link ResourceSession}
     * @param stream {@link OutputStream} to write to
     * @param revisions revisions to serialize
     */
    public XmlSerializerBuilder(final XmlResourceSession resourceMgr, final OutputStream stream,
        final int... revisions) {
      maxLevel = -1;
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
     * @param nodeKey root node key of subtree to shredder
     * @param stream {@link OutputStream} to write to
     * @param properties {@link XmlSerializerProperties} to use
     * @param revisions revisions to serialize
     */
    public XmlSerializerBuilder(final XmlResourceSession resourceMgr, final @NonNegative long nodeKey,
        final OutputStream stream, final XmlSerializerProperties properties, final int... revisions) {
      checkArgument(nodeKey >= 0, "nodeKey must be >= 0!");
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
      rest = requireNonNull((Boolean) map.get(S_REST[0]));
      id = requireNonNull((Boolean) map.get(S_ID[0]));
      indentSpaces = requireNonNull((Integer) map.get(S_INDENT_SPACES[0]));
      declaration = requireNonNull((Boolean) map.get(S_XMLDECL[0]));
    }

    /**
     * Specify the start node key.
     *
     * @param nodeKey node key to start serialization from (the root of the subtree to serialize)
     * @return this XMLSerializerBuilder reference
     */
    public XmlSerializerBuilder startNodeKey(final long nodeKey) {
      this.nodeKey = nodeKey;
      return this;
    }

    /**
     * Sets an initial indentation.
     *
     * @return this {@link XmlSerializerBuilder} instance
     */
    public XmlSerializerBuilder withInitialIndent() {
      initialIndent = true;
      return this;
    }

    /**
     * Sets if the serialization is used for XQuery result sets.
     *
     * @return this {@link XmlSerializerBuilder} instance
     */
    public XmlSerializerBuilder isXQueryResultSequence() {
      emitXQueryResultSequence = true;
      return this;
    }

    /**
     * Sets if the serialization of timestamps of the revision(s) is used or not.
     *
     * @return this {@link XmlSerializerBuilder} instance
     */
    public XmlSerializerBuilder serializeTimestamp(boolean serializeTimestamp) {
      this.serializeTimestamp = serializeTimestamp;
      return this;
    }

    /**
     * Pretty prints the output.
     *
     * @return this {@link XmlSerializerBuilder} instance
     */
    public XmlSerializerBuilder prettyPrint() {
      indent = true;
      return this;
    }

    /**
     * Emit RESTful output.
     *
     * @return this {@link XmlSerializerBuilder} instance
     */
    public XmlSerializerBuilder emitRESTful() {
      rest = true;
      return this;
    }

    /**
     * Emit a rest-sequence start-tag/end-tag in startDocument()/endDocument() method.
     *
     * @return this {@link XmlSerializerBuilder} instance
     */
    public XmlSerializerBuilder emitRESTSequence() {
      restSequence = true;
      return this;
    }

    /**
     * Emit an XML declaration.
     *
     * @return this {@link XmlSerializerBuilder} instance
     */
    public XmlSerializerBuilder emitXMLDeclaration() {
      declaration = true;
      return this;
    }

    /**
     * Emit the unique nodeKeys / IDs of element-nodes.
     *
     * @return this {@link XmlSerializerBuilder} instance
     */
    public XmlSerializerBuilder emitIDs() {
      id = true;
      return this;
    }

    /**
     * Emit metadata of element-nodes.
     *
     * @return this {@link XmlSerializerBuilder} instance
     */
    public XmlSerializerBuilder emitMetaData() {
      metaData = true;
      return this;
    }

    /**
     * The maximum level.
     *
     * @param maxLevel the maximum level
     * @return this {@link XmlSerializerBuilder} instance
     */
    public XmlSerializerBuilder maxLevel(final long maxLevel) {
      this.maxLevel = maxLevel;
      return this;
    }

    /**
     * The versions to serialize.
     *
     * @param revisions the versions to serialize
     * @return this {@link XmlSerializerBuilder} instance
     */
    public XmlSerializerBuilder revisions(final int[] revisions) {
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
    public XmlSerializer build() {
      return new XmlSerializer(resourceMgr, nodeKey, this, initialIndent, version, versions);
    }
  }
}
