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

package org.sirix.saxon.wrapper;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.Iterator;

import javax.annotation.Nonnegative;

import net.sf.saxon.Configuration;
import net.sf.saxon.event.Receiver;
import net.sf.saxon.om.DocumentInfo;
import net.sf.saxon.om.NamePool;
import net.sf.saxon.om.NodeInfo;
import net.sf.saxon.om.SequenceIterator;
import net.sf.saxon.pattern.NodeTest;
import net.sf.saxon.trans.XPathException;
import net.sf.saxon.tree.iter.AxisIterator;
import net.sf.saxon.tree.util.FastStringBuffer;
import net.sf.saxon.value.Value;

import org.sirix.api.Axis;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.Session;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.IncludeSelf;
import org.sirix.exception.SirixException;
import org.sirix.node.Kind;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

/**
 * <h1>DocumentWrapper</h1>
 * 
 * <p>
 * Wraps a sirix document and represents a document node. Therefore it
 * implements Saxon's DocumentInfo core interface and also represents a Node in
 * Saxon's internal node implementation. Thus it extends <tt>NodeWrapper</tt>.
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public final class DocumentWrapper implements DocumentInfo {

	/** {@link LogWrapper} instance. */
	private static final LogWrapper LOGWRAPPER = new LogWrapper(
			LoggerFactory.getLogger(DocumentWrapper.class));

	/** sirix database. */
	final Session mSession;

	/** The revision. */
	final int mRevision;

	/** Base URI of the document. */
	String mBaseURI;

	/** Saxon configuration. */
	Configuration mConfig;

	/** Unique document number. */
	long mDocumentNumber;

	/**
	 * Instance of {@link NodeWrapper}-implementation
	 */
	private final NodeWrapper mNodeWrapper;

	/**
	 * Wrap a sirix document.
	 * 
	 * @param session
	 *          sirix {@link Session}
	 * @param revision
	 *          the revision to open
	 * @param config
	 *          Saxon {@link Configuration} instance
	 * @throws SirixException
	 *           if sirix encounters an error
	 */
	public DocumentWrapper(final Session session,
			@Nonnegative final int revision, final Configuration config)
			throws SirixException {
		checkArgument(revision >= 0, "revision must be >= 0!");
		mSession = checkNotNull(session);
		mRevision = revision;
		mBaseURI = session.getResourceConfig().getResource().getAbsolutePath();
		mConfig = checkNotNull(config);
		mNodeWrapper = new NodeWrapper(this, 0);
	}

	/**
	 * Wrap a sirix document.
	 * 
	 * @param session
	 *          Sirix {@link Session}
	 * @param config
	 *          Saxon {@link Configuration} instance
	 * @throws SirixException
	 *           if Sirix encounters an error
	 */
	public DocumentWrapper(final Session session,
			final Configuration config) throws SirixException {
		this(session, session.getLastRevisionNumber(), config);
	}

	@Override
	public String[] getUnparsedEntity(final String name) {
		throw new UnsupportedOperationException("Currently not supported by sirix!");
	}

	/**
	 * Get the unparsed entity with a given name.
	 * 
	 * @return null: sirix does not provide access to unparsed entities.
	 */
	@SuppressWarnings("unchecked")
	public Iterator<String> getUnparsedEntityNames() {
		return (Iterator<String>) Collections.EMPTY_LIST.iterator();
	}

	@Override
	public NodeInfo selectID(final String ID, final boolean getParent) {
		try {
			final NodeReadTrx rtx = mSession.beginNodeReadTrx();
			final Axis axis = new DescendantAxis(rtx, IncludeSelf.YES);
			while (axis.hasNext()) {
				if (rtx.getKind() == Kind.ELEMENT) {
					final int attCount = rtx.getAttributeCount();

					if (attCount > 0) {
						final long nodeKey = rtx.getNodeKey();

						for (int index = 0; index < attCount; index++) {
							rtx.moveToAttribute(index);

							if ("xml:id".equalsIgnoreCase(rtx.getName().getLocalName())
									&& ID.equals(rtx.getValue())) {
								if (getParent) {
									rtx.moveToParent();
								}
								return new NodeWrapper(this, rtx.getNodeKey());
							}
							rtx.moveTo(nodeKey);
						}
					}
				}
				axis.next();
			}
			rtx.close();
		} catch (final SirixException e) {
			LOGWRAPPER.error(e.getMessage(), e);
		}
		return null;
	}

	@Override
	public NamePool getNamePool() {
		return mConfig.getNamePool();
	}

	/**
	 * Set the configuration (containing the name pool used for all names in this
	 * document). Calling this method allocates a unique number to the document
	 * (unique within the Configuration); this will form the basis for testing
	 * node identity.
	 * 
	 * @param config
	 *          Saxon {@link Configuration} instance
	 */
	public void setConfiguration(final Configuration config) {
		mConfig = config;
		mDocumentNumber = config.getDocumentNumberAllocator()
				.allocateDocumentNumber();
	}

	@Override
	public Configuration getConfiguration() {
		return mConfig;
	}

	@Override
	public String getBaseURI() {
		return mBaseURI;
	}

	/**
	 * Set the baseURI of the current document.
	 * 
	 * @param baseURI
	 *          usually the absolute path of the document
	 */
	void setBaseURI(final String baseURI) {
		mBaseURI = checkNotNull(baseURI);
	}

	@Override
	public Object getUserData(String arg0) {
		return null;
	}

	@Override
	public void setUserData(String arg0, Object arg1) {
	}

	@Override
	public Value atomize() throws XPathException {
		return getNodeWrapper().atomize();
	}

	@Override
	public int compareOrder(NodeInfo arg0) {
		return getNodeWrapper().compareOrder(arg0);
	}

	@Override
	public void copy(Receiver arg0, int arg1, int arg2) throws XPathException {
		getNodeWrapper().copy(arg0, arg1, arg2);

	}

	@Override
	public void generateId(FastStringBuffer arg0) {
		getNodeWrapper().generateId(arg0);
	}

	@Override
	public String getAttributeValue(int arg0) {
		return getNodeWrapper().getAttributeValue(arg0);
	}

	@Override
	public int getColumnNumber() {
		return getNodeWrapper().getColumnNumber();
	}

	@Override
	public int[] getDeclaredNamespaces(int[] arg0) {
		return getNodeWrapper().getDeclaredNamespaces(arg0);
	}

	@Override
	public String getDisplayName() {
		return getNodeWrapper().getDisplayName();
	}

	@Override
	public long getDocumentNumber() {
		return getNodeWrapper().getDocumentNumber();
	}

	@Override
	public DocumentInfo getDocumentRoot() {
		return getNodeWrapper().getDocumentRoot();
	}

	@Override
	public int getFingerprint() {
		return getNodeWrapper().getFingerprint();
	}

	@Override
	public int getLineNumber() {
		return getNodeWrapper().getLineNumber();
	}

	@Override
	public String getLocalPart() {
		return getNodeWrapper().getLocalPart();
	}

	@Override
	public int getNameCode() {
		return getNodeWrapper().getNameCode();
	}

	@Override
	public int getNodeKind() {
		return getNodeWrapper().getNodeKind();
	}

	@Override
	public NodeInfo getParent() {
		return getNodeWrapper().getParent();
	}

	@Override
	public String getPrefix() {
		return getNodeWrapper().getPrefix();
	}

	@Override
	public NodeInfo getRoot() {
		return getNodeWrapper().getRoot();
	}

	@Override
	public String getStringValue() {
		return getNodeWrapper().getStringValue();
	}

	@Override
	public String getSystemId() {
		return getNodeWrapper().getSystemId();
	}

	@Override
	public int getTypeAnnotation() {
		return getNodeWrapper().getTypeAnnotation();
	}

	@Override
	public String getURI() {
		return getNodeWrapper().getURI();
	}

	@Override
	public boolean hasChildNodes() {
		return getNodeWrapper().hasChildNodes();
	}

	@Override
	public boolean isId() {
		return getNodeWrapper().isId();
	}

	@Override
	public boolean isIdref() {
		return getNodeWrapper().isIdref();
	}

	@Override
	public boolean isNilled() {
		return getNodeWrapper().isNilled();
	}

	@Override
	public boolean isSameNodeInfo(NodeInfo arg0) {
		return getNodeWrapper().isSameNodeInfo(arg0);
	}

	@Override
	public AxisIterator iterateAxis(byte arg0) {
		return getNodeWrapper().iterateAxis(arg0);
	}

	@Override
	public AxisIterator iterateAxis(byte arg0, NodeTest arg1) {
		return getNodeWrapper().iterateAxis(arg0, arg1);
	}

	@Override
	public void setSystemId(String arg0) {
		getNodeWrapper().setSystemId(arg0);
	}

	@Override
	public CharSequence getStringValueCS() {
		return getNodeWrapper().getStringValueCS();
	}

	@Override
	public SequenceIterator getTypedValue() throws XPathException {
		return getNodeWrapper().getTypedValue();
	}

	public NodeWrapper getNodeWrapper() {
		return mNodeWrapper;
	}
}
