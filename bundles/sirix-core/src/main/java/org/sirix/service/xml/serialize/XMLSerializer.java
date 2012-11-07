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

package org.sirix.service.xml.serialize;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.sirix.service.xml.serialize.XMLSerializerProperties.S_ID;
import static org.sirix.service.xml.serialize.XMLSerializerProperties.S_INDENT;
import static org.sirix.service.xml.serialize.XMLSerializerProperties.S_INDENT_SPACES;
import static org.sirix.service.xml.serialize.XMLSerializerProperties.S_REST;
import static org.sirix.service.xml.serialize.XMLSerializerProperties.S_XMLDECL;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.access.Databases;
import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.Database;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.Session;
import org.sirix.settings.CharsForSerializing;
import org.sirix.settings.Constants;
import org.sirix.utils.Files;
import org.sirix.utils.LogWrapper;
import org.sirix.utils.XMLToken;
import org.slf4j.LoggerFactory;

/**
 * <h1>XMLSerializer</h1>
 * 
 * <p>
 * Most efficient way to serialize a subtree into an OutputStream. The encoding
 * always is UTF-8. Note that the OutputStream internally is wrapped by a
 * BufferedOutputStream. There is no need to buffer it again outside of this
 * class.
 * </p>
 */
public final class XMLSerializer extends AbstractSerializer {

	/** {@link LogWrapper} reference. */
	private static final LogWrapper LOGWRAPPER = new LogWrapper(
			LoggerFactory.getLogger(XMLSerializer.class));

	/** Offset that must be added to digit to make it ASCII. */
	private static final int ASCII_OFFSET = 48;

	/** Precalculated powers of each available long digit. */
	private static final long[] LONG_POWERS = { 1L, 10L, 100L, 1000L, 10000L,
			100000L, 1000000L, 10000000L, 100000000L, 1000000000L, 10000000000L,
			100000000000L, 1000000000000L, 10000000000000L, 100000000000000L,
			1000000000000000L, 10000000000000000L, 100000000000000000L,
			1000000000000000000L };

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
	 * @param pSession
	 *          session for read XML
	 * @param pNodeKey
	 *          start node key
	 * @param pBuilder
	 *          builder of XML Serializer
	 * @param pRevision
	 *          revision to serialize
	 * @param pRevisions
	 *          further revisions to serialize
	 */
	private XMLSerializer(final @Nonnull Session pSession,
			final @Nonnegative long pNodeKey,
			final @Nonnull XMLSerializerBuilder pBuilder,
			final @Nonnegative int pRevision, final @Nonnull int... pRevisions) {
		super(pSession, pNodeKey, pRevision, pRevisions);
		mOut = new BufferedOutputStream(pBuilder.mStream, 4096);
		mIndent = pBuilder.mIndent;
		mSerializeXMLDeclaration = pBuilder.mDeclaration;
		mSerializeRest = pBuilder.mREST;
		mSerializeId = pBuilder.mID;
		mIndentSpaces = pBuilder.mIndentSpaces;
	}

	/**
	 * Emit node (start element or characters).
	 * 
	 * @param rtx
	 *          Sirix {@link NodeReadTrx}
	 */
	@Override
	protected void emitStartElement(final @Nonnull NodeReadTrx rtx) {
		try {
			switch (rtx.getKind()) {
			case DOCUMENT:
				if (mIndent) {
					mOut.write(CharsForSerializing.NEWLINE.getBytes());
				}
				break;
			case ELEMENT:
				// Emit start element.
				indent();
				mOut.write(CharsForSerializing.OPEN.getBytes());
				mOut.write(rtx.rawNameForKey(rtx.getNameKey()));
				final long key = rtx.getNodeKey();
				// Emit namespace declarations.
				for (int index = 0, nspCount = rtx.getNamespaceCount(); index < nspCount; index++) {
					rtx.moveToNamespace(index);
					if (rtx.nameForKey(rtx.getNameKey()).isEmpty()) {
						mOut.write(CharsForSerializing.XMLNS.getBytes());
						write(rtx.nameForKey(rtx.getURIKey()));
						mOut.write(CharsForSerializing.QUOTE.getBytes());
					} else {
						mOut.write(CharsForSerializing.XMLNS_COLON.getBytes());
						write(rtx.nameForKey(rtx.getNameKey()));
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
					mOut.write(rtx.rawNameForKey(rtx.getNameKey()));
					mOut.write(CharsForSerializing.EQUAL_QUOTE.getBytes());
					mOut.write(XMLToken.escapeAttribute(rtx.getValue()).getBytes(
							Constants.DEFAULT_ENCODING));// pRtx.getItem().getRawValue());
					mOut.write(CharsForSerializing.QUOTE.getBytes());
					rtx.moveTo(key);
				}
				if (rtx.hasFirstChild()) {
					mOut.write(CharsForSerializing.CLOSE.getBytes());
				} else {
					mOut.write(CharsForSerializing.SLASH_CLOSE.getBytes());
				}
				if (mIndent) {
					mOut.write(CharsForSerializing.NEWLINE.getBytes());
				}
				break;
			case COMMENT:
				indent();
				mOut.write(CharsForSerializing.OPENCOMMENT.getBytes());
				mOut.write(XMLToken.escapeContent(rtx.getValue()).getBytes(
						Constants.DEFAULT_ENCODING));
				if (mIndent) {
					mOut.write(CharsForSerializing.NEWLINE.getBytes());
				}
				mOut.write(CharsForSerializing.CLOSECOMMENT.getBytes());
				break;
			case TEXT:
				indent();
				mOut.write(XMLToken.escapeContent(rtx.getValue()).getBytes(
						Constants.DEFAULT_ENCODING));
				if (mIndent) {
					mOut.write(CharsForSerializing.NEWLINE.getBytes());
				}
				break;
			case PROCESSING_INSTRUCTION:
				indent();
				mOut.write(CharsForSerializing.OPENPI.getBytes());
				mOut.write(rtx.rawNameForKey(rtx.getNameKey()));
				mOut.write(CharsForSerializing.SPACE.getBytes());
				mOut.write(XMLToken.escapeContent(rtx.getValue()).getBytes(
						Constants.DEFAULT_ENCODING));
				if (mIndent) {
					mOut.write(CharsForSerializing.NEWLINE.getBytes());
				}
				mOut.write(CharsForSerializing.CLOSEPI.getBytes());
				break;
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
	 * @param rtx
	 *          Sirix {@link NodeReadTrx}
	 */
	@Override
	protected void emitEndElement(final @Nonnull NodeReadTrx rtx) {
		try {
			indent();
			mOut.write(CharsForSerializing.OPEN_SLASH.getBytes());
			mOut.write(rtx.rawNameForKey(rtx.getNameKey()));
			mOut.write(CharsForSerializing.CLOSE.getBytes());
			if (mIndent) {
				mOut.write(CharsForSerializing.NEWLINE.getBytes());
			}
		} catch (final IOException e) {
			LOGWRAPPER.error(e.getMessage(), e);
		}
	}

	@Override
	protected void emitStartDocument() {
		try {
			if (mSerializeXMLDeclaration) {
				write("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>");
			}
			if (mSerializeRest) {
				write("<rest:sequence xmlns:rest=\"REST\"><rest:item>");
			}
		} catch (final IOException e) {
			LOGWRAPPER.error(e.getMessage(), e);
		}
	}

	@Override
	protected void emitEndDocument() {
		try {
			if (mSerializeRest) {
				write("</rest:item></rest:sequence>");
			}
			mOut.flush();
		} catch (final IOException e) {
			LOGWRAPPER.error(e.getMessage(), e);
		}
	}

	@Override
	protected void emitStartManualElement(final @Nonnegative long version) {
		try {
			write("<tt revision=\"");
			write(Long.toString(version));
			write("\">");
		} catch (final IOException e) {
			LOGWRAPPER.error(e.getMessage(), e);
		}
	}

	@Override
	protected void emitEndManualElement(final @Nonnegative long version) {
		try {
			write("</tt>");
		} catch (final IOException e) {
			LOGWRAPPER.error(e.getMessage(), e);
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
				mOut.write(" ".getBytes(Constants.DEFAULT_ENCODING));
			}
		}
	}

	/**
	 * Write characters of string.
	 * 
	 * @param pString
	 *          String to write
	 * @throws IOException
	 *           if can't write to string
	 * @throws UnsupportedEncodingException
	 *           if unsupport encoding
	 */
	protected void write(final @Nonnull String pString)
			throws UnsupportedEncodingException, IOException {
		mOut.write(pString.getBytes(Constants.DEFAULT_ENCODING));
	}

	/**
	 * Write non-negative non-zero long as UTF-8 bytes.
	 * 
	 * @param value
	 *          value to write
	 * @throws IOException
	 *           if can't write to string
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
	 * @param args
	 *          args[0] specifies the input-TT file/folder; args[1] specifies the
	 *          output XML file.
	 * @throws Exception
	 *           any exception
	 */
	public static void main(final String... args) throws Exception {
		if (args.length < 2 || args.length > 3) {
			throw new IllegalArgumentException(
					"Usage: XMLSerializer input-TT output.xml");
		}

		LOGWRAPPER.info("Serializing '" + args[0] + "' to '" + args[1] + "' ... ");
		final long time = System.nanoTime();
		final File target = new File(args[1]);
		Files.recursiveRemove(target.toPath());
		target.getParentFile().mkdirs();
		target.createNewFile();
		try (final FileOutputStream outputStream = new FileOutputStream(target)) {
			final DatabaseConfiguration config = new DatabaseConfiguration(new File(
					args[0]));
			Databases.createDatabase(config);
			try (final Database db = Databases.openDatabase(new File(args[0]))) {
				db.createResource(new ResourceConfiguration.Builder("shredded", config)
						.build());
				final Session session = db.getSession(new SessionConfiguration.Builder(
						"shredded").build());

				final XMLSerializer serializer = XMLSerializer.builder(session,
						outputStream).build();
				serializer.call();
			}
		}

		LOGWRAPPER
				.info(" done [" + (System.nanoTime() - time) / 1_000_000 + "ms].");
	}
	
	/**
	 * Constructor, setting the necessary stuff.
	 * 
	 * @param session
	 *          Sirix {@link Session}
	 * @param stream
	 *          {@link OutputStream} to write to
	 * @param revisions
	 *          revisions to serialize
	 */
	public static XMLSerializerBuilder builder(final @Nonnull Session session,
			final @Nonnull OutputStream stream, final int... revisions) {
		return new XMLSerializerBuilder(session, stream, revisions);
	}
	
	/**
	 * Constructor.
	 * 
	 * @param session
	 *          Sirix {@link Session}
	 * @param nodeKey
	 *          root node key of subtree to shredder
	 * @param stream
	 *          {@link OutputStream} to write to
	 * @param properties
	 *          {@link XMLSerializerProperties} to use
	 * @param revisions
	 *          revisions to serialize
	 */
	public static XMLSerializerBuilder builder(final @Nonnull Session session,
			final @Nonnegative long nodeKey, final @Nonnull OutputStream stream,
			final @Nonnull XMLSerializerProperties properties,
			final int... revisions) {
		return new XMLSerializerBuilder(session, nodeKey, stream, properties, revisions);
	}

	/**
	 * XMLSerializerBuilder to setup the XMLSerializer.
	 */
	public static final class XMLSerializerBuilder {
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
		private boolean mDeclaration = true;

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

		/** Session to use. */
		private final Session mSession;

		/** Further revisions to serialize. */
		private int[] mVersions;

		/** Revision to serialize. */
		private int mVersion;

		/** Node key of subtree to shredder. */
		private long mNodeKey;

		/**
		 * Constructor, setting the necessary stuff.
		 * 
		 * @param session
		 *          Sirix {@link Session}
		 * @param stream
		 *          {@link OutputStream} to write to
		 * @param revisions
		 *          revisions to serialize
		 */
		public XMLSerializerBuilder(final @Nonnull Session session,
				final @Nonnull OutputStream stream, final int... revisions) {
			mNodeKey = 0;
			mSession = checkNotNull(session);
			mStream = checkNotNull(stream);
			if (revisions == null || revisions.length == 0) {
				mVersion = mSession.getLastRevisionNumber();
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
		 * @param session
		 *          Sirix {@link Session}
		 * @param nodeKey
		 *          root node key of subtree to shredder
		 * @param stream
		 *          {@link OutputStream} to write to
		 * @param properties
		 *          {@link XMLSerializerProperties} to use
		 * @param revisions
		 *          revisions to serialize
		 */
		public XMLSerializerBuilder(final @Nonnull Session session,
				final @Nonnegative long nodeKey, final @Nonnull OutputStream stream,
				final @Nonnull XMLSerializerProperties properties,
				final int... revisions) {
			checkArgument(nodeKey >= 0, "pNodeKey must be >= 0!");
			mSession = checkNotNull(session);
			mNodeKey = nodeKey;
			mStream = checkNotNull(stream);
			if (revisions == null || revisions.length == 0) {
				mVersion = mSession.getLastRevisionNumber();
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
		 * Setting the start node key.
		 * 
		 * @param nodeKey
		 *          node key to start serialization from
		 * @return XMLSerializerBuilder reference
		 */
		public XMLSerializerBuilder startNodeKey(final long nodeKey) {
			mNodeKey = nodeKey;
			return this;
		}

		/**
		 * Setting the indendation.
		 * 
		 * @param indent
		 *          determines if it should be indented
		 * @return XMLSerializerBuilder reference
		 */
		public XMLSerializerBuilder doIndend(final boolean indent) {
			mIndent = indent;
			return this;
		}

		/**
		 * Setting the RESTful output.
		 * 
		 * @param isRESTful
		 *          set RESTful
		 * @return XMLSerializerBuilder reference
		 */
		public XMLSerializerBuilder isRESTful(final boolean isRESTful) {
			mREST = isRESTful;
			return this;
		}

		/**
		 * Setting the declaration.
		 * 
		 * @param declaration
		 *          determines if the XML declaration should be emitted
		 * @return {@link XMLSerializerBuilder} reference
		 */
		public XMLSerializerBuilder setDeclaration(final boolean declaration) {
			mDeclaration = declaration;
			return this;
		}

		/**
		 * Setting the IDs on nodes.
		 * 
		 * @param id
		 *          determines if IDs should be set for each node
		 * @return XMLSerializerBuilder reference
		 */
		public XMLSerializerBuilder setID(final boolean id) {
			mID = id;
			return this;
		}

		/**
		 * Setting the versions to serialize.
		 * 
		 * @param versions
		 *          versions to serialize
		 * @return XMLSerializerBuilder reference
		 */
		public XMLSerializerBuilder versions(final int[] versions) {
			mVersions = checkNotNull(versions);
			return this;
		}

		/**
		 * Building new {@link Serializer} instance.
		 * 
		 * @return a new {@link Serializer} instance
		 */
		public XMLSerializer build() {
			return new XMLSerializer(mSession, mNodeKey, this, mVersion, mVersions);
		}
	}

}
