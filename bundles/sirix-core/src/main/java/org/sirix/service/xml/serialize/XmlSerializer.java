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
import static com.google.common.base.Preconditions.checkNotNull;
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
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.brackit.xquery.util.serialize.Serializer;
import org.sirix.access.DatabaseConfiguration;
import org.sirix.access.Databases;
import org.sirix.access.ResourceConfiguration;
import org.sirix.api.ResourceManager;
import org.sirix.api.xdm.XdmNodeReadOnlyTrx;
import org.sirix.api.xdm.XdmNodeTrx;
import org.sirix.api.xdm.XdmResourceManager;
import org.sirix.node.Kind;
import org.sirix.settings.CharsForSerializing;
import org.sirix.settings.Constants;
import org.sirix.utils.LogWrapper;
import org.sirix.utils.SirixFiles;
import org.sirix.utils.XMLToken;
import org.slf4j.LoggerFactory;

/**
 * <h1>XmlSerializer</h1>
 *
 * <p>
 * Most efficient way to serialize a subtree into an OutputStream. The encoding always is UTF-8.
 * Note that the OutputStream internally is wrapped by a BufferedOutputStream. There is no need to
 * buffer it again outside of this class.
 * </p>
 */
public final class XmlSerializer extends org.sirix.service.AbstractSerializer<XdmNodeReadOnlyTrx, XdmNodeTrx> {

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory.getLogger(XmlSerializer.class));

  /** Offset that must be added to digit to make it ASCII. */
  private static final int ASCII_OFFSET = 48;

  /** Precalculated powers of each available long digit. */
  private static final long[] LONG_POWERS = {1L, 10L, 100L, 1000L, 10000L, 100000L, 1000000L, 10000000L, 100000000L,
      1000000000L, 10000000000L, 100000000000L, 1000000000000L, 10000000000000L, 100000000000000L, 1000000000000000L,
      10000000000000000L, 100000000000000000L, 1000000000000000000L};

  /** OutputStream to write to. */
  private final OutputStream mOut;

  /** Indent output. */
  private final boolean mIndent;

  /** Serialize XML declaration. */
  private final boolean mSerializeXMLDeclaration;

  /** Serialize rest header and closer and rest:id. */
  private final boolean mSerializeRest;

  /** Serialize a rest-sequence element for the start-document. */
  private final boolean mSerializeRestSequence;

  /** Serialize id. */
  private final boolean mSerializeId;

  /** Number of spaces to indent. */
  private final int mIndentSpaces;

  /** Determines if serializing with initial indentation. */
  private final boolean mWithInitialIndent;

  private final boolean mEmitXQueryResultSequence;

  private final boolean mSerializeTimestamp;

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
  private XmlSerializer(final XdmResourceManager resourceMgr, final @Nonnegative long nodeKey,
      final XmlSerializerBuilder builder, final boolean initialIndent, final @Nonnegative int revision,
      final int... revsions) {
    super(resourceMgr, nodeKey, revision, revsions);
    mOut = new BufferedOutputStream(builder.mStream, 4096);
    mIndent = builder.mIndent;
    mSerializeXMLDeclaration = builder.mDeclaration;
    mSerializeRest = builder.mREST;
    mSerializeRestSequence = builder.mRESTSequence;
    mSerializeId = builder.mID;
    mIndentSpaces = builder.mIndentSpaces;
    mWithInitialIndent = builder.mInitialIndent;
    mEmitXQueryResultSequence = builder.mEmitXQueryResultSequence;
    mSerializeTimestamp = builder.mSerializeTimestamp;
  }

  /**
   * Emit node (start element or characters).
   *
   * @param rtx Sirix {@link XdmNodeReadOnlyTrx}
   */
  @Override
  protected void emitNode(final XdmNodeReadOnlyTrx rtx) {
    try {
      switch (rtx.getKind()) {
        case XDM_DOCUMENT:
          break;
        case ELEMENT:
          // Emit start element.
          indent();
          mOut.write(CharsForSerializing.OPEN.getBytes());
          writeQName(rtx);
          final long key = rtx.getNodeKey();
          // Emit namespace declarations.
          for (int index = 0, nspCount = rtx.getNamespaceCount(); index < nspCount; index++) {
            rtx.moveToNamespace(index);
            if (rtx.getPrefixKey() == -1) {
              mOut.write(CharsForSerializing.XMLNS.getBytes());
              write(rtx.nameForKey(rtx.getURIKey()));
              mOut.write(CharsForSerializing.QUOTE.getBytes());
            } else {
              mOut.write(CharsForSerializing.XMLNS_COLON.getBytes());
              write(rtx.nameForKey(rtx.getPrefixKey()));
              mOut.write(CharsForSerializing.EQUAL_QUOTE.getBytes());
              write(rtx.nameForKey(rtx.getURIKey()));
              mOut.write(CharsForSerializing.QUOTE.getBytes());
            }
            rtx.moveTo(key);
          }
          // Emit attributes.
          // Add virtual rest:id attribute.
          if (mSerializeId) {
            if (mSerializeRest) {
              mOut.write(CharsForSerializing.REST_PREFIX.getBytes());
            } else {
              mOut.write(CharsForSerializing.SPACE.getBytes());
            }
            mOut.write(CharsForSerializing.ID.getBytes());
            mOut.write(CharsForSerializing.EQUAL_QUOTE.getBytes());
            write(rtx.getNodeKey());
            mOut.write(CharsForSerializing.QUOTE.getBytes());
          }

          // Iterate over all persistent attributes.
          for (int index = 0, attCount = rtx.getAttributeCount(); index < attCount; index++) {
            rtx.moveToAttribute(index);
            mOut.write(CharsForSerializing.SPACE.getBytes());
            writeQName(rtx);
            mOut.write(CharsForSerializing.EQUAL_QUOTE.getBytes());
            mOut.write(XMLToken.escapeAttribute(rtx.getValue()).getBytes(Constants.DEFAULT_ENCODING));
            mOut.write(CharsForSerializing.QUOTE.getBytes());
            rtx.moveTo(key);
          }
          if (rtx.hasFirstChild()) {
            mOut.write(CharsForSerializing.CLOSE.getBytes());
          } else {
            mOut.write(CharsForSerializing.SLASH_CLOSE.getBytes());
          }
          if (mIndent && !(rtx.getFirstChildKind() == Kind.TEXT && rtx.getChildCount() == 1)) {
            mOut.write(CharsForSerializing.NEWLINE.getBytes());
          }
          break;
        case COMMENT:
          indent();
          mOut.write(CharsForSerializing.OPENCOMMENT.getBytes());
          mOut.write(XMLToken.escapeContent(rtx.getValue()).getBytes(Constants.DEFAULT_ENCODING));
          if (mIndent) {
            mOut.write(CharsForSerializing.NEWLINE.getBytes());
          }
          mOut.write(CharsForSerializing.CLOSECOMMENT.getBytes());
          break;
        case TEXT:
          if (rtx.hasRightSibling() || rtx.hasLeftSibling())
            indent();
          mOut.write(XMLToken.escapeContent(rtx.getValue()).getBytes(Constants.DEFAULT_ENCODING));
          if (mIndent && (rtx.hasRightSibling() || rtx.hasLeftSibling())) {
            mOut.write(CharsForSerializing.NEWLINE.getBytes());
          }
          break;
        case PROCESSING_INSTRUCTION:
          indent();
          mOut.write(CharsForSerializing.OPENPI.getBytes());
          writeQName(rtx);
          mOut.write(CharsForSerializing.SPACE.getBytes());
          mOut.write(XMLToken.escapeContent(rtx.getValue()).getBytes(Constants.DEFAULT_ENCODING));
          if (mIndent) {
            mOut.write(CharsForSerializing.NEWLINE.getBytes());
          }
          mOut.write(CharsForSerializing.CLOSEPI.getBytes());
          break;
        // $CASES-OMITTED$
        default:
          throw new IllegalStateException("Node kind not known!");
      }
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  /**
   * Emit end element.
   *
   * @param rtx Sirix {@link XdmNodeReadOnlyTrx}
   */
  @Override
  protected void emitEndNode(final XdmNodeReadOnlyTrx rtx) {
    try {
      if (mIndent && !(rtx.getFirstChildKind() == Kind.TEXT && rtx.getChildCount() == 1))
        indent();
      mOut.write(CharsForSerializing.OPEN_SLASH.getBytes());
      writeQName(rtx);
      mOut.write(CharsForSerializing.CLOSE.getBytes());
      if (mIndent) {
        mOut.write(CharsForSerializing.NEWLINE.getBytes());
      }
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  // Write a QName.
  private void writeQName(final XdmNodeReadOnlyTrx rtx) throws IOException {
    if (rtx.getPrefixKey() != -1) {
      mOut.write(rtx.rawNameForKey(rtx.getPrefixKey()));
      mOut.write(CharsForSerializing.COLON.getBytes());
    }
    mOut.write(rtx.rawNameForKey(rtx.getLocalNameKey()));
  }

  @Override
  protected void emitStartDocument() {
    try {
      if (mSerializeXMLDeclaration) {
        write("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>");
        if (mIndent) {
          mOut.write(CharsForSerializing.NEWLINE.getBytes());
        }
      }

      final int length = (mRevisions.length == 1 && mRevisions[0] < 0)
          ? (int) mResMgr.getMostRecentRevisionNumber()
          : mRevisions.length;

      if (mSerializeRestSequence || length > 1) {
        if (mSerializeRestSequence) {
          write("<rest:sequence xmlns:rest=\"https://sirix.io/rest\">");
        } else {
          write("<sdb:sirix xmlns:sdb=\"https://sirix.io/rest\">");
        }

        if (mIndent) {
          mOut.write(CharsForSerializing.NEWLINE.getBytes());
          mStack.push(Constants.NULL_ID_LONG);
        }
      }
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  @Override
  protected void emitEndDocument() {
    try {
      final int length = (mRevisions.length == 1 && mRevisions[0] < 0)
          ? (int) mResMgr.getMostRecentRevisionNumber()
          : mRevisions.length;

      if (mSerializeRestSequence || length > 1) {
        if (mIndent) {
          mStack.pop();
        }
        indent();

        if (mSerializeRestSequence) {
          write("</rest:sequence>");
        } else {
          write("</sdb:sirix>");
        }
      }

      mOut.flush();
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  @Override
  protected void emitRevisionStartNode(final @Nonnull XdmNodeReadOnlyTrx rtx) {
    try {
      final int length = (mRevisions.length == 1 && mRevisions[0] < 0)
          ? (int) mResMgr.getMostRecentRevisionNumber()
          : mRevisions.length;

      if (mSerializeRest || length > 1) {
        indent();
        if (mSerializeRest) {
          write("<rest:item");
        } else {
          write("<sdb:sirix-item");
        }

        if (length > 1 || mEmitXQueryResultSequence) {
          if (mSerializeRest) {
            write(" rest:revision=\"");
          } else {
            write(" sdb:revision=\"");
          }
          write(Integer.toString(rtx.getRevisionNumber()));
          write("\"");

          if (mSerializeTimestamp) {
            if (mSerializeRest) {
              write(" rest:revisionTimestamp=\"");
            } else {
              write(" sdb:revisionTimestamp=\"");
            }

            write(DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC).format(rtx.getRevisionTimestamp()));
            write("\"");
          }

          write(">");
        } else if (mSerializeRest) {
          write(">");
        }

        if (rtx.hasFirstChild())
          mStack.push(Constants.NULL_ID_LONG);

        if (mIndent) {
          mOut.write(CharsForSerializing.NEWLINE.getBytes());
        }
      }
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  @Override
  protected void emitRevisionEndNode(final @Nonnull XdmNodeReadOnlyTrx rtx) {
    try {
      final int length = (mRevisions.length == 1 && mRevisions[0] < 0)
          ? (int) mResMgr.getMostRecentRevisionNumber()
          : mRevisions.length;

      if (mSerializeRest || length > 1) {
        if (rtx.moveToDocumentRoot().getCursor().hasFirstChild())
          mStack.pop();
        indent();
        if (mSerializeRest) {
          write("</rest:item>");
        } else {
          write("</sdb:sirix-item>");
        }
      }

      if (mIndent) {
        mOut.write(CharsForSerializing.NEWLINE.getBytes());
      }
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  /**
   * Indentation of output.
   *
   * @throws IOException if can't indent output
   */
  private void indent() throws IOException {
    if (mIndent) {
      final int indentSpaces = mWithInitialIndent
          ? (mStack.size() + 1) * mIndentSpaces
          : mStack.size() * mIndentSpaces;
      for (int i = 0; i < indentSpaces; i++) {
        mOut.write(" ".getBytes(Constants.DEFAULT_ENCODING));
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
  protected void write(final String value) throws UnsupportedEncodingException, IOException {
    mOut.write(value.getBytes(Constants.DEFAULT_ENCODING));
  }

  /**
   * Write non-negative non-zero long as UTF-8 bytes.
   *
   * @param value value to write
   * @throws IOException if can't write to string
   */
  private void write(final long value) throws IOException {
    final int length = (int) Math.log10(value);
    int digit = 0;
    long remainder = value;
    for (int i = length; i >= 0; i--) {
      digit = (byte) (remainder / LONG_POWERS[i]);
      mOut.write((byte) (digit + ASCII_OFFSET));
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
    Databases.createXdmDatabase(config);
    try (final var db = Databases.openXdmDatabase(databaseFile)) {
      db.createResource(new ResourceConfiguration.Builder("shredded").build());

      try (final XdmResourceManager resMgr = db.openResourceManager("shredded");
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
   * @param resMgr Sirix {@link ResourceManager}
   * @param stream {@link OutputStream} to write to
   * @param revisions revisions to serialize
   */
  public static XmlSerializerBuilder newBuilder(final XdmResourceManager resMgr, final OutputStream stream,
      final int... revisions) {
    return new XmlSerializerBuilder(resMgr, stream, revisions);
  }

  /**
   * Constructor.
   *
   * @param resMgr Sirix {@link ResourceManager}
   * @param nodeKey root node key of subtree to shredder
   * @param stream {@link OutputStream} to write to
   * @param properties {@link XmlSerializerProperties} to use
   * @param revisions revisions to serialize
   */
  public static XmlSerializerBuilder newBuilder(final XdmResourceManager resMgr, final @Nonnegative long nodeKey,
      final OutputStream stream, final XmlSerializerProperties properties, final int... revisions) {
    return new XmlSerializerBuilder(resMgr, nodeKey, stream, properties, revisions);
  }

  /**
   * XMLSerializerBuilder to setup the XMLSerializer.
   */
  public static final class XmlSerializerBuilder {
    public boolean mRESTSequence;

    /**
     * Intermediate boolean for indendation, not necessary.
     */
    private boolean mIndent;

    /**
     * Intermediate boolean for rest serialization, not necessary.
     */
    private boolean mREST;

    /**
     * Intermediate boolean for XML-Decl serialization, not necessary.
     */
    private boolean mDeclaration;

    /**
     * Intermediate boolean for ids, not necessary.
     */
    private boolean mID;

    /**
     * Intermediate number of spaces to indent, not necessary.
     */
    private int mIndentSpaces = 2;

    /** Stream to pipe to. */
    private final OutputStream mStream;

    /** Resource manager to use. */
    private final XdmResourceManager mResourceMgr;

    /** Further revisions to serialize. */
    private int[] mVersions;

    /** Revision to serialize. */
    private int mVersion;

    /** Node key of subtree to shredder. */
    private long mNodeKey;

    private boolean mInitialIndent;

    private boolean mEmitXQueryResultSequence;

    private boolean mSerializeTimestamp;

    /**
     * Constructor, setting the necessary stuff.
     *
     * @param resourceMgr Sirix {@link ResourceManager}
     * @param stream {@link OutputStream} to write to
     * @param revisions revisions to serialize
     */
    public XmlSerializerBuilder(final XdmResourceManager resourceMgr, final OutputStream stream,
        final int... revisions) {
      mNodeKey = 0;
      mResourceMgr = checkNotNull(resourceMgr);
      mStream = checkNotNull(stream);
      if (revisions == null || revisions.length == 0) {
        mVersion = mResourceMgr.getMostRecentRevisionNumber();
      } else {
        mVersion = revisions[0];
        mVersions = new int[revisions.length - 1];
        for (int i = 0; i < revisions.length - 1; i++) {
          mVersions[i] = revisions[i + 1];
        }
      }
    }

    /**
     * Constructor.
     *
     * @param resourceMgr Sirix {@link ResourceManager}
     * @param nodeKey root node key of subtree to shredder
     * @param stream {@link OutputStream} to write to
     * @param properties {@link XmlSerializerProperties} to use
     * @param revisions revisions to serialize
     */
    public XmlSerializerBuilder(final XdmResourceManager resourceMgr, final @Nonnegative long nodeKey,
        final OutputStream stream, final XmlSerializerProperties properties, final int... revisions) {
      checkArgument(nodeKey >= 0, "pNodeKey must be >= 0!");
      mResourceMgr = checkNotNull(resourceMgr);
      mNodeKey = nodeKey;
      mStream = checkNotNull(stream);
      if (revisions == null || revisions.length == 0) {
        mVersion = mResourceMgr.getMostRecentRevisionNumber();
      } else {
        mVersion = revisions[0];
        mVersions = new int[revisions.length - 1];
        for (int i = 0; i < revisions.length - 1; i++) {
          mVersions[i] = revisions[i + 1];
        }
      }
      final ConcurrentMap<?, ?> map = checkNotNull(properties.getProps());
      mIndent = checkNotNull((Boolean) map.get(S_INDENT[0]));
      mREST = checkNotNull((Boolean) map.get(S_REST[0]));
      mID = checkNotNull((Boolean) map.get(S_ID[0]));
      mIndentSpaces = checkNotNull((Integer) map.get(S_INDENT_SPACES[0]));
      mDeclaration = checkNotNull((Boolean) map.get(S_XMLDECL[0]));
    }

    /**
     * Specify the start node key.
     *
     * @param nodeKey node key to start serialization from (the root of the subtree to serialize)
     * @return this XMLSerializerBuilder reference
     */
    public XmlSerializerBuilder startNodeKey(final long nodeKey) {
      mNodeKey = nodeKey;
      return this;
    }

    /**
     * Sets an initial indentation.
     *
     * @return this {@link XmlSerializerBuilder} instance
     */
    public XmlSerializerBuilder withInitialIndent() {
      mInitialIndent = true;
      return this;
    }

    /**
     * Sets if the serialization is used for XQuery result sets.
     *
     * @return this {@link XmlSerializerBuilder} instance
     */
    public XmlSerializerBuilder isXQueryResultSequence() {
      mEmitXQueryResultSequence = true;
      return this;
    }

    /**
     * Sets if the serialization of timestamps of the revision(s) is used or not.
     *
     * @return this {@link XmlSerializerBuilder} instance
     */
    public XmlSerializerBuilder serializeTimestamp(boolean serializeTimestamp) {
      mSerializeTimestamp = serializeTimestamp;
      return this;
    }

    /**
     * Pretty prints the output.
     *
     * @return this {@link XmlSerializerBuilder} instance
     */
    public XmlSerializerBuilder prettyPrint() {
      mIndent = true;
      return this;
    }

    /**
     * Emit RESTful output.
     *
     * @return this {@link XmlSerializerBuilder} instance
     */
    public XmlSerializerBuilder emitRESTful() {
      mREST = true;
      return this;
    }

    /**
     * Emit a rest-sequence start-tag/end-tag in startDocument()/endDocument() method.
     *
     * @return this {@link XmlSerializerBuilder} instance
     */
    public XmlSerializerBuilder emitRESTSequence() {
      mRESTSequence = true;
      return this;
    }

    /**
     * Emit an XML declaration.
     *
     * @return this {@link XmlSerializerBuilder} instance
     */
    public XmlSerializerBuilder emitXMLDeclaration() {
      mDeclaration = true;
      return this;
    }

    /**
     * Emit the unique nodeKeys / IDs of nodes.
     *
     * @return this {@link XmlSerializerBuilder} instance
     */
    public XmlSerializerBuilder emitIDs() {
      mID = true;
      return this;
    }

    /**
     * The versions to serialize.
     *
     * @param revisions the versions to serialize
     * @return this {@link XmlSerializerBuilder} instance
     */
    public XmlSerializerBuilder revisions(final int[] revisions) {
      checkNotNull(revisions);

      mVersion = revisions[0];

      mVersions = new int[revisions.length - 1];
      for (int i = 0; i < revisions.length - 1; i++) {
        mVersions[i] = revisions[i + 1];
      }

      return this;
    }

    /**
     * Building new {@link Serializer} instance.
     *
     * @return a new {@link Serializer} instance
     */
    public XmlSerializer build() {
      return new XmlSerializer(mResourceMgr, mNodeKey, this, mInitialIndent, mVersion, mVersions);
    }
  }
}
