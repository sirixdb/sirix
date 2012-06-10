/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.treetank.service.xml.serialize;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import static org.treetank.service.xml.serialize.XMLSerializerProperties.S_ID;
import static org.treetank.service.xml.serialize.XMLSerializerProperties.S_INDENT;
import static org.treetank.service.xml.serialize.XMLSerializerProperties.S_INDENT_SPACES;
import static org.treetank.service.xml.serialize.XMLSerializerProperties.S_REST;
import static org.treetank.service.xml.serialize.XMLSerializerProperties.S_XMLDECL;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.LoggerFactory;
import org.treetank.access.Database;
import org.treetank.access.conf.DatabaseConfiguration;
import org.treetank.access.conf.ResourceConfiguration;
import org.treetank.access.conf.SessionConfiguration;
import org.treetank.api.IDatabase;
import org.treetank.api.INodeReadTrx;
import org.treetank.api.ISession;
import org.treetank.node.ElementNode;
import org.treetank.node.interfaces.INameNode;
import org.treetank.node.interfaces.IStructNode;
import org.treetank.settings.ECharsForSerializing;
import org.treetank.utils.Files;
import org.treetank.utils.IConstants;
import org.treetank.utils.LogWrapper;
import org.treetank.utils.XMLToken;

/**
 * <h1>XMLSerializer</h1>
 * 
 * <p>
 * Most efficient way to serialize a subtree into an OutputStream. The encoding always is UTF-8. Note that the
 * OutputStream internally is wrapped by a BufferedOutputStream. There is no need to buffer it again outside
 * of this class.
 * </p>
 */
public final class XMLSerializer extends AbsSerializer {

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory.getLogger(XMLSerializer.class));

  /** Offset that must be added to digit to make it ASCII. */
  private static final int ASCII_OFFSET = 48;

  /** Precalculated powers of each available long digit. */
  private static final long[] LONG_POWERS = {
    1L, 10L, 100L, 1000L, 10000L, 100000L, 1000000L, 10000000L, 100000000L, 1000000000L, 10000000000L,
    100000000000L, 1000000000000L, 10000000000000L, 100000000000000L, 1000000000000000L, 10000000000000000L,
    100000000000000000L, 1000000000000000000L
  };

  /** OutputStream to write to. */
  private final OutputStream mOut;

  /** Indent output. */
  private final boolean mIndent;

  /** Serialize XML declaration. */
  private final boolean mSerializeXMLDeclaration;

  /** Serialize rest header and closer and rest:id. */
  private final boolean mSerializeRest;

  /** Serialize id. */
  private final boolean mSerializeId;

  /** Number of spaces to indent. */
  private final int mIndentSpaces;

  /**
   * Initialize XMLStreamReader implementation with transaction. The cursor
   * points to the node the XMLStreamReader starts to read.
   * 
   * @param paramSession
   *          Session for read XML
   * @param paramNodeKey
   *          Node Key
   * @param paramBuilder
   *          Builder of XML Serializer
   * @param paramVersions
   *          Version to serailze
   */
  private XMLSerializer(final ISession paramSession, final long paramNodeKey,
    final XMLSerializerBuilder paramBuilder, final long... paramVersions) {
    super(paramSession, paramNodeKey, paramVersions);
    mOut = new BufferedOutputStream(paramBuilder.mStream, 4096);
    mIndent = paramBuilder.mIndent;
    mSerializeXMLDeclaration = paramBuilder.mDeclaration;
    mSerializeRest = paramBuilder.mREST;
    mSerializeId = paramBuilder.mID;
    mIndentSpaces = paramBuilder.mIndentSpaces;
  }

  /**
   * Emit node (start element or characters).
   */
  @Override
  protected void emitStartElement(final INodeReadTrx pRtx) {
    try {
      switch (pRtx.getNode().getKind()) {
      case ROOT_KIND:
        if (mIndent) {
          mOut.write(ECharsForSerializing.NEWLINE.getBytes());
        }
        break;
      case ELEMENT_KIND:
        // Emit start element.
        indent();
        mOut.write(ECharsForSerializing.OPEN.getBytes());
        mOut.write(pRtx.rawNameForKey(((INameNode)pRtx.getNode()).getNameKey()));
        final long key = pRtx.getNode().getNodeKey();
        // Emit namespace declarations.
        for (int index = 0, length = ((ElementNode)pRtx.getNode()).getNamespaceCount(); index < length; index++) {
          pRtx.moveToNamespace(index);
          if (pRtx.nameForKey(((INameNode)pRtx.getNode()).getNameKey()).isEmpty()) {
            mOut.write(ECharsForSerializing.XMLNS.getBytes());
            write(pRtx.nameForKey(((INameNode)pRtx.getNode()).getURIKey()));
            mOut.write(ECharsForSerializing.QUOTE.getBytes());
          } else {
            mOut.write(ECharsForSerializing.XMLNS_COLON.getBytes());
            write(pRtx.nameForKey(((INameNode)pRtx.getNode()).getNameKey()));
            mOut.write(ECharsForSerializing.EQUAL_QUOTE.getBytes());
            write(pRtx.nameForKey(((INameNode)pRtx.getNode()).getURIKey()));
            mOut.write(ECharsForSerializing.QUOTE.getBytes());
          }
          pRtx.moveTo(key);
        }
        // Emit attributes.
        // Add virtual rest:id attribute.
        if (mSerializeId) {
          if (mSerializeRest) {
            mOut.write(ECharsForSerializing.REST_PREFIX.getBytes());
          } else {
            mOut.write(ECharsForSerializing.SPACE.getBytes());
          }
          mOut.write(ECharsForSerializing.ID.getBytes());
          mOut.write(ECharsForSerializing.EQUAL_QUOTE.getBytes());
          write(pRtx.getNode().getNodeKey());
          mOut.write(ECharsForSerializing.QUOTE.getBytes());
        }

        // Iterate over all persistent attributes.
        for (int index = 0; index < ((ElementNode)pRtx.getNode()).getAttributeCount(); index++) {
          pRtx.moveToAttribute(index);
          mOut.write(ECharsForSerializing.SPACE.getBytes());
          mOut.write(pRtx.rawNameForKey(((INameNode)pRtx.getNode()).getNameKey()));
          mOut.write(ECharsForSerializing.EQUAL_QUOTE.getBytes());
          // XmlEscaper.xmlEscaper().escape(pRtx.getValueOfCurrentNode()).toBytes();
          mOut.write(XMLToken.escape(pRtx.getValueOfCurrentNode()).getBytes(IConstants.DEFAULT_ENCODING));// pRtx.getItem().getRawValue());
          mOut.write(ECharsForSerializing.QUOTE.getBytes());
          pRtx.moveTo(key);
        }
        if (((IStructNode)pRtx.getNode()).hasFirstChild()) {
          mOut.write(ECharsForSerializing.CLOSE.getBytes());
        } else {
          mOut.write(ECharsForSerializing.SLASH_CLOSE.getBytes());
        }
        if (mIndent) {
          mOut.write(ECharsForSerializing.NEWLINE.getBytes());
        }
        break;
      case TEXT_KIND:
        indent();
        // Guava 11 (not released)
        // XmlEscaper.xmlContentEscaper().escape(pRtx.getValueOfCurrentNode()).toBytes();
        mOut.write(XMLToken.escape(pRtx.getValueOfCurrentNode()).getBytes(IConstants.DEFAULT_ENCODING)); // pRtx.getItem().getRawValue());
        if (mIndent) {
          mOut.write(ECharsForSerializing.NEWLINE.getBytes());
        }
        break;
      }
    } catch (final IOException exc) {
      exc.printStackTrace();
    }
  }

  /**
   * Emit end element.
   * 
   * @param pRtx
   *          Read Transaction
   */
  @Override
  protected void emitEndElement(final INodeReadTrx pRtx) {
    try {
      indent();
      mOut.write(ECharsForSerializing.OPEN_SLASH.getBytes());
      mOut.write(pRtx.rawNameForKey(((INameNode)pRtx.getNode()).getNameKey()));
      mOut.write(ECharsForSerializing.CLOSE.getBytes());
      if (mIndent) {
        mOut.write(ECharsForSerializing.NEWLINE.getBytes());
      }
    } catch (final IOException exc) {
      exc.printStackTrace();
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void emitStartDocument() {
    try {
      if (mSerializeXMLDeclaration) {
        write("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>");
      }
      if (mSerializeRest) {
        write("<rest:sequence xmlns:rest=\"REST\"><rest:item>");
      }
    } catch (final IOException exc) {
      exc.printStackTrace();
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void emitEndDocument() {
    try {
      if (mSerializeRest) {
        write("</rest:item></rest:sequence>");
      }
      mOut.flush();
    } catch (final IOException exc) {
      exc.printStackTrace();
    }

  }

  /** {@inheritDoc} */
  @Override
  protected void emitStartManualElement(final long mVersion) {
    try {
      write("<tt revision=\"");
      write(Long.toString(mVersion));
      write("\">");
    } catch (final IOException exc) {
      exc.printStackTrace();
    }

  }

  /** {@inheritDoc} */
  @Override
  protected void emitEndManualElement(final long mVersion) {
    try {
      write("</tt>");
    } catch (final IOException exc) {
      exc.printStackTrace();
    }
  }

  /**
   * Indentation of output.
   * 
   * @throws IOException
   *           if can't indent output
   */
  private void indent() throws IOException {
    if (mIndent) {
      for (int i = 0; i < mStack.size() * mIndentSpaces; i++) {
        mOut.write(" ".getBytes());
      }
    }
  }

  /**
   * Write characters of string.
   * 
   * @param mString
   *          String to write
   * @throws IOException
   *           if can't write to string
   * @throws UnsupportedEncodingException
   *           if unsupport encoding
   */
  protected void write(final String mString) throws UnsupportedEncodingException, IOException {
    mOut.write(mString.getBytes(IConstants.DEFAULT_ENCODING));
  }

  /**
   * Write non-negative non-zero long as UTF-8 bytes.
   * 
   * @param mValue
   *          Value to write
   * @throws IOException
   *           if can't write to string
   */
  private void write(final long mValue) throws IOException {
    final int length = (int)Math.log10(mValue);
    int digit = 0;
    long remainder = mValue;
    for (int i = length; i >= 0; i--) {
      digit = (byte)(remainder / LONG_POWERS[i]);
      mOut.write((byte)(digit + ASCII_OFFSET));
      remainder -= digit * LONG_POWERS[i];
    }
  }

  /**
   * Main method.
   * 
   * @param args
   *          args[0] specifies the input-TT file/folder; args[1] specifies
   *          the output XML file.
   * @throws Exception
   *           Any exception.
   */
  public static void main(final String... args) throws Exception {
    if (args.length < 2 || args.length > 3) {
      throw new IllegalArgumentException("Usage: XMLSerializer input-TT output.xml");
    }

    LOGWRAPPER.info("Serializing '" + args[0] + "' to '" + args[1] + "' ... ");
    final long time = System.nanoTime();
    final File target = new File(args[1]);
    Files.recursiveRemove(target.toPath());
    target.getParentFile().mkdirs();
    target.createNewFile();
    final FileOutputStream outputStream = new FileOutputStream(target);

    final DatabaseConfiguration config = new DatabaseConfiguration(new File(args[0]));
    Database.createDatabase(config);
    final IDatabase db = Database.openDatabase(new File(args[0]));
    db.createResource(new ResourceConfiguration.Builder("shredded", config).build());
    final ISession session = db.getSession(new SessionConfiguration.Builder("shredded").build());

    final XMLSerializer serializer = new XMLSerializerBuilder(session, outputStream).build();
    serializer.call();

    session.close();
    outputStream.close();
    db.close();

    LOGWRAPPER.info(" done [" + (System.nanoTime() - time) / 1_000_000 + "ms].");
  }

  /**
   * XMLSerializerBuilder to setup the XMLSerializer.
   */
  public static final class XMLSerializerBuilder {
    /**
     * Intermediate boolean for indendation, not necessary.
     */
    private transient boolean mIndent;

    /**
     * Intermediate boolean for rest serialization, not necessary.
     */
    private transient boolean mREST;

    /**
     * Intermediate boolean for XML-Decl serialization, not necessary.
     */
    private transient boolean mDeclaration = true;

    /**
     * Intermediate boolean for ids, not necessary.
     */
    private transient boolean mID;

    /**
     * Intermediate number of spaces to indent, not necessary.
     */
    private transient int mIndentSpaces = 2;

    /** Stream to pipe to. */
    private final OutputStream mStream;

    /** Session to use. */
    private final ISession mSession;

    /** Versions to use. */
    private transient long[] mVersions;

    /** Node key of subtree to shredder. */
    private final long mNodeKey;

    /**
     * Constructor, setting the necessary stuff.
     * 
     * @param paramSession
     *          {@link ISession} to Serialize
     * @param paramStream
     *          {@link OutputStream}
     * @param paramVersions
     *          version(s) to Serialize
     */
    public XMLSerializerBuilder(final ISession paramSession, final OutputStream paramStream,
      final long... paramVersions) {
      mNodeKey = 0;
      mStream = checkNotNull(paramStream);
      mSession = checkNotNull(paramSession);
      mVersions = checkNotNull(paramVersions);
    }

    /**
     * Constructor.
     * 
     * @param paramSession
     *          {@link ISession}
     * @param paramNodeKey
     *          root node key of subtree to shredder
     * @param paramStream
     *          {@link OutputStream}
     * @param paramProperties
     *          {@link XMLSerializerProperties}
     * @param paramVersions
     *          version(s) to serialize
     */
    public XMLSerializerBuilder(final ISession paramSession, final long paramNodeKey,
      final OutputStream paramStream, final XMLSerializerProperties paramProperties,
      final long... paramVersions) {
      checkArgument(paramNodeKey >= 0, "paramNodeKey must be >= 0!");
      mSession = checkNotNull(paramSession);
      mNodeKey = paramNodeKey;
      mStream = checkNotNull(paramStream);
      mVersions = checkNotNull(paramVersions);
      final ConcurrentMap<?, ?> map = checkNotNull(paramProperties.getProps());
      mIndent = checkNotNull((Boolean)map.get(S_INDENT[0]));
      mREST = checkNotNull((Boolean)map.get(S_REST[0]));
      mID = checkNotNull((Boolean)map.get(S_ID[0]));
      mIndentSpaces = checkNotNull((Integer)map.get(S_INDENT_SPACES[0]));
      mDeclaration = checkNotNull((Boolean)map.get(S_XMLDECL[0]));
    }

    /**
     * Setting the indention.
     * 
     * @param paramIndent
     *          determines if it should be indented
     * @return XMLSerializerBuilder reference
     */
    public XMLSerializerBuilder setIndend(final boolean paramIndent) {
      mIndent = paramIndent;
      return this;
    }

    /**
     * Setting the RESTful output.
     * 
     * @param paramREST
     *          set RESTful
     * @return XMLSerializerBuilder reference
     */
    public XMLSerializerBuilder setREST(final boolean paramREST) {
      mREST = paramREST;
      return this;
    }

    /**
     * Setting the declaration.
     * 
     * @param paramDeclaration
     *          determines if the XML declaration should be emitted
     * @return XMLSerializerBuilder reference.
     */
    public XMLSerializerBuilder setDeclaration(final boolean paramDeclaration) {
      mDeclaration = paramDeclaration;
      return this;
    }

    /**
     * Setting the IDs on nodes.
     * 
     * @param paramID
     *          determines if IDs should be set for each node
     * @return XMLSerializerBuilder reference
     */
    public XMLSerializerBuilder setID(final boolean paramID) {
      mID = paramID;
      return this;
    }

    /**
     * Setting the ids on nodes.
     * 
     * @param paramVersions
     *          to set
     * @return XMLSerializerBuilder reference
     */
    public XMLSerializerBuilder setVersions(final long[] paramVersions) {
      mVersions = checkNotNull(paramVersions);
      return this;
    }

    /**
     * Building new Serializer.
     * 
     * @return a new instance
     */
    public XMLSerializer build() {
      return new XMLSerializer(mSession, mNodeKey, this, mVersions);
    }
  }

}
