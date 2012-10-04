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

import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

import net.sf.saxon.Configuration;
import net.sf.saxon.event.Receiver;
import net.sf.saxon.om.Axis;
import net.sf.saxon.om.DocumentInfo;
import net.sf.saxon.om.NamePool;
import net.sf.saxon.om.NodeInfo;
import net.sf.saxon.om.SequenceIterator;
import net.sf.saxon.om.StandardNames;
import net.sf.saxon.pattern.AnyNodeTest;
import net.sf.saxon.pattern.NameTest;
import net.sf.saxon.pattern.NodeTest;
import net.sf.saxon.trans.XPathException;
import net.sf.saxon.tree.iter.AxisIterator;
import net.sf.saxon.tree.iter.EmptyIterator;
import net.sf.saxon.tree.iter.NamespaceIterator;
import net.sf.saxon.tree.iter.SingletonIterator;
import net.sf.saxon.tree.util.FastStringBuffer;
import net.sf.saxon.tree.util.Navigator;
import net.sf.saxon.tree.wrapper.SiblingCountingNode;
import net.sf.saxon.type.Type;
import net.sf.saxon.value.AtomicValue;
import net.sf.saxon.value.StringValue;
import net.sf.saxon.value.UntypedAtomicValue;
import net.sf.saxon.value.Value;

import org.sirix.api.IAxis;
import org.sirix.api.INodeReadTrx;
import org.sirix.axis.AncestorAxis;
import org.sirix.axis.AttributeAxis;
import org.sirix.axis.ChildAxis;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.EIncludeSelf;
import org.sirix.axis.FollowingAxis;
import org.sirix.axis.FollowingSiblingAxis;
import org.sirix.axis.ParentAxis;
import org.sirix.axis.PrecedingAxis;
import org.sirix.axis.PrecedingSiblingAxis;
import org.sirix.axis.filter.FilterAxis;
import org.sirix.axis.filter.TextFilter;
import org.sirix.exception.SirixException;
import org.sirix.node.EKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <h1>NodeWrapper</h1>
 * 
 * <p>
 * Wraps a sirix node into Saxon's internal representation of a node. It
 * therefore implements Saxon's core interface NodeInfo as well as two others:
 * </p>
 * 
 * <dl>
 * <dt>NodeInfo</dt>
 * <dd>The NodeInfo interface represents a node in Saxon's implementation of the
 * XPath 2.0 data model.</dd>
 * <dt>VirtualNode</dt>
 * <dd>This interface is implemented by NodeInfo implementations that act as
 * wrappers on some underlying tree. It provides a method to access the real
 * node underlying the virtual node, for use by applications that need to drill
 * down to the underlying data.</dd>
 * <dt>SiblingCountingNode</dt>
 * <dd>Interface that extends NodeInfo by providing a method to get the position
 * of a node relative to its siblings.</dd>
 * </dl>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class NodeWrapper implements SiblingCountingNode {

	/** Kind of current node. */
	private final EKind mNodeKind;

	/** Document wrapper. */
	private final DocumentWrapper mDocWrapper;

	/**
	 * Log wrapper for better output.
	 */
	private static final Logger LOGGER = LoggerFactory
			.getLogger(NodeWrapper.class);

	/** Key of node. */
	private final long mKey;

	/** QName of current node. */
	private final QName mQName;

	/** The revision to open. */
	private final int mRevision;

	/**
	 * A node in the XML parse tree. Wrap a sirix node.
	 * 
	 * @param database
	 *          sirix database.
	 * @param pNodeKeyToStart
	 *          start noeKey to move to
	 * @throws SirixException
	 */
	NodeWrapper(final DocumentWrapper pDocWrapper, final long pNodeKeyToStart)
			throws SirixException {
		mDocWrapper = checkNotNull(pDocWrapper);
		checkArgument(pNodeKeyToStart >= 0, "pNodeKeyToStart must be >= 0!");
		final INodeReadTrx rtx = mDocWrapper.mSession
				.beginNodeReadTrx(pDocWrapper.mRevision);
		rtx.moveTo(pNodeKeyToStart);
		mNodeKind = rtx.getKind();
		mKey = rtx.getNodeKey();
		mRevision = pDocWrapper.mRevision;

		if (mNodeKind == EKind.ELEMENT || mNodeKind == EKind.ATTRIBUTE) {
			mQName = rtx.getName();
		} else {
			mQName = null;
		}
		rtx.close();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Value atomize() throws XPathException {
		Value value = null;

		switch (mNodeKind) {
		case COMMENT:
		case PROCESSING:
			// The content as an instance of the xs:string data type.
			value = new StringValue(getStringValueCS());
			break;
		default:
			// The content as an instance of the xdt:untypedAtomic data type.
			value = new UntypedAtomicValue(getStringValueCS());
		}

		return value;
	}

	@Override
	public int compareOrder(final NodeInfo node) {
		if (getDocumentNumber() != node.getDocumentNumber()) {
			throw new IllegalStateException("May not be called on different trees!");
		}
		return Navigator.compareOrder(this, (NodeWrapper) checkNotNull(node));
	}

	/**
	 * Copy this node to a given outputter (deep copy).
	 * 
	 * @see net.sf.saxon.om.NodeInfo#copy(Receiver, int, int)
	 */
	public void copy(final Receiver out, final int copyOption,
			final int locationId) throws XPathException {
		Navigator
				.copy(this, out, mDocWrapper.getNamePool(), copyOption, locationId);
	}

	@Override
	public void generateId(final FastStringBuffer buf) {
		buf.append(String.valueOf(mKey));
	}

	@Override
	public String getAttributeValue(final int fingerprint) {
		String attVal = null;

		final NameTest test = new NameTest(Type.ATTRIBUTE, fingerprint,
				getNamePool());
		final AxisIterator iterator = iterateAxis(Axis.ATTRIBUTE, test);
		final NodeInfo attribute = (NodeInfo) iterator.next();

		if (attribute != null) {
			attVal = attribute.getStringValue();
		}

		return attVal;
	}

	@Override
	public String getBaseURI() {
		String baseURI = null;

		NodeInfo node = this;

		while (node != null) {
			baseURI = node.getAttributeValue(StandardNames.XML_BASE);

			if (baseURI == null) {
				// Search for baseURI in parent node (xml:base="").
				node = node.getParent();
			} else {
				break;
			}
		}

		if (baseURI == null) {
			baseURI = mDocWrapper.getBaseURI();
		}

		return baseURI;
	}

	@Override
	public int getColumnNumber() {
		throw new UnsupportedOperationException("Not supported by sirix.");
	}

	@Override
	public Configuration getConfiguration() {
		return mDocWrapper.getConfiguration();
	}

	@Override
	public int[] getDeclaredNamespaces(final int[] buffer) {
		int[] retVal = null;
		if (mNodeKind == EKind.ELEMENT) {
			try {
				final INodeReadTrx rtx = createRtxAndMove();
				final int count = rtx.getNamespaceCount();

				if (count == 0) {
					retVal = EMPTY_NAMESPACE_LIST;
				} else {
					retVal = (buffer == null || count > buffer.length ? new int[count]
							: buffer);
					final NamePool pool = getNamePool();
					int n = 0;
					try {
						for (int i = 0; i < count; i++) {
							rtx.moveTo(i);
							final String prefix = getPrefix();
							final String uri = getURI();
							rtx.moveTo(mKey);

							retVal[n++] = pool.allocateNamespaceCode(prefix, uri);
						}
						rtx.close();
					} catch (final SirixException exc) {
						LOGGER.error(exc.toString());
					}
					/*
					 * If the supplied array is larger than required, then the first
					 * unused entry will be set to -1.
					 */
					if (count < retVal.length) {
						retVal[count] = -1;
					}
				}
			} catch (final SirixException e) {
				throw new IllegalStateException(e.getCause());
			}
		}

		return retVal;
	}

	@Override
	public String getDisplayName() {
		String dName = "";

		switch (mNodeKind) {
		case ELEMENT:
		case ATTRIBUTE:
			dName = new StringBuilder(getPrefix()).append(":").append(getLocalPart())
					.toString();
			break;
		case NAMESPACE:
		case PROCESSING:
			dName = getLocalPart();
			break;
		default:
			// Do nothing.
		}

		return dName;
	}

	@Override
	public long getDocumentNumber() {
		return mDocWrapper.getBaseURI().hashCode();
	}

	@Override
	public DocumentInfo getDocumentRoot() {
		return mDocWrapper;
	}

	@Override
	public int getFingerprint() {
		int retVal;

		final int nameCount = getNameCode();
		if (nameCount == -1) {
			retVal = -1;
		} else {
			retVal = nameCount & 0xfffff;
		}

		return retVal;
	}

	@Override
	public int getLineNumber() {
		throw new UnsupportedOperationException("Not supported by sirix.");
	}

	@Override
	public String getLocalPart() {
		String localPart = "";

		switch (mNodeKind) {
		case ELEMENT:
		case ATTRIBUTE:
			localPart = mQName.getLocalPart();
			break;
		default:
			// Do nothing.
		}

		return localPart;
	}

	@Override
	public int getNameCode() {
		int nameCode = -1;

		switch (mNodeKind) {
		case ELEMENT:
		case ATTRIBUTE:
		case PROCESSING:
			// case NAMESPACE_KIND:
			nameCode = mDocWrapper.getNamePool().allocate(getPrefix(), getURI(),
					getLocalPart());
			break;
		default:
			// text, comment, document and namespace nodes.
		}

		return nameCode;
	}

	@Override
	public NamePool getNamePool() {
		return mDocWrapper.getNamePool();
	}

	@Override
	public int getNodeKind() {
		return mNodeKind.getId();
	}

	@Override
	public NodeInfo getParent() {
		try {
			NodeInfo parent = null;
			final INodeReadTrx rtx = createRtxAndMove();
			if (rtx.hasParent()) {
				// Parent transaction.
				parent = new NodeWrapper(mDocWrapper, rtx.getParentKey());
			}
			rtx.close();
			return parent;
		} catch (final SirixException e) {
			LOGGER.error(e.getMessage(), e);
			return null;
		}

	}

	@Override
	public String getPrefix() {
		String prefix = "";

		switch (mNodeKind) {
		case ELEMENT:
		case ATTRIBUTE:
			prefix = mQName.getPrefix();
			break;
		default:
			/*
			 * Change nothing, return empty String in case of a node which isn't an
			 * element or attribute.
			 */
		}

		return prefix;
	}

	@Override
	public NodeInfo getRoot() {
		return (NodeInfo) mDocWrapper;
	}

	/**
	 * getStringValue() just calls getStringValueCS().
	 * 
	 */
	@Override
	public final String getStringValue() {
		return getStringValueCS().toString();
	}

	@Override
	public final CharSequence getStringValueCS() {
		String mValue = "";
		try {
			final INodeReadTrx rtx = createRtxAndMove();

			switch (mNodeKind) {
			case DOCUMENT_ROOT:
			case ELEMENT:
				mValue = expandString();
				break;
			case ATTRIBUTE:
				mValue = emptyIfNull(rtx.getValue());
				break;
			case TEXT:
				mValue = rtx.getValue();
				break;
			case COMMENT:
			case PROCESSING:
				mValue = emptyIfNull(rtx.getValue());
				break;
			default:
				mValue = "";
			}

			rtx.close();
		} catch (final SirixException exc) {
			LOGGER.error(exc.toString());
		}

		return mValue;
	}

	/**
	 * Filter text nodes.
	 * 
	 * @return concatenated String of text node values
	 */
	private String expandString() {
		final FastStringBuffer fsb = new FastStringBuffer(FastStringBuffer.SMALL);
		try {
			final INodeReadTrx rtx = createRtxAndMove();
			final FilterAxis axis = new FilterAxis(new DescendantAxis(rtx),
					new TextFilter(rtx));

			while (axis.hasNext()) {
				if (rtx.getKind() == EKind.TEXT) {
					fsb.append(rtx.getValue());
				}
				axis.next();
			}
			rtx.close();
		} catch (final SirixException exc) {
			LOGGER.error(exc.toString());
		}
		return fsb.condense().toString();
	}

	@Override
	public String getSystemId() {
		return mDocWrapper.getBaseURI();
	}

	/**
	 * Get the type annotation.
	 * 
	 * @return UNTYPED or UNTYPED_ATOMIC.
	 */
	public int getTypeAnnotation() {
		int type = 0;
		if (mNodeKind == EKind.ATTRIBUTE) {
			type = StandardNames.XS_UNTYPED_ATOMIC;
		} else {
			type = StandardNames.XS_UNTYPED;
		}
		return type;
	}

	@Override
	public String getURI() {
		String URI = "";

		switch (mNodeKind) {
		case ELEMENT:
		case ATTRIBUTE:
		case NAMESPACE:
			if (!"".equals(mQName.getPrefix())) {
				URI = mQName.getNamespaceURI();
			}
			break;
		default:
			// Do nothing.
		}

		// Return URI or empty string.
		return URI;
	}

	@Override
	public boolean hasChildNodes() {
		boolean hasChildNodes = false;
		try {
			final INodeReadTrx rtx = createRtxAndMove();
			if (rtx.getChildCount() > 0) {
				hasChildNodes = true;
			}
			rtx.close();
		} catch (final SirixException exc) {
			LOGGER.error(exc.toString());
		}
		return hasChildNodes;
	}

	/**
	 * Not supported.
	 */
	@Override
	public boolean isId() {
		return false;
	}

	@Override
	public boolean isIdref() {
		throw new UnsupportedOperationException("Currently not supported by Sirix!");
	}

	@Override
	public boolean isNilled() {
		throw new UnsupportedOperationException("Currently not supported by Sirix!");
	}

	@Override
	public boolean isSameNodeInfo(final NodeInfo pOther) {
		if (pOther == null) {
			return false;
		}
		if (!(pOther instanceof NodeInfo)) {
			return false;
		} else {
			return ((NodeWrapper) pOther).mKey == mKey;
		}
	}

	@Override
	public AxisIterator iterateAxis(final byte axisNumber) {
		return iterateAxis(axisNumber, AnyNodeTest.getInstance());
	}

	@Override
	public AxisIterator iterateAxis(final byte axisNumber, final NodeTest nodeTest) {
		AxisIterator returnVal = null;
		try {
			final INodeReadTrx rtx = createRtxAndMove();

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("NODE TEST: " + nodeTest);
			}

			switch (axisNumber) {
			case Axis.ANCESTOR:
				if (getNodeKind() == EKind.DOCUMENT_ROOT.getId()) {
					returnVal = EmptyIterator.getInstance();
				} else {
					returnVal = new Navigator.AxisFilter(new SaxonEnumeration(
							new AncestorAxis(rtx)), nodeTest);
				}
				break;
			case Axis.ANCESTOR_OR_SELF:
				if (getNodeKind() == EKind.DOCUMENT_ROOT.getId()) {
					returnVal = Navigator.filteredSingleton(this, nodeTest);
				} else {
					returnVal = new Navigator.AxisFilter(new SaxonEnumeration(
							new AncestorAxis(rtx, EIncludeSelf.YES)), nodeTest);
				}
				break;
			case Axis.ATTRIBUTE:
				if (getNodeKind() != EKind.ELEMENT.getId()) {
					returnVal = EmptyIterator.getInstance();
				} else {
					returnVal = new Navigator.AxisFilter(new SaxonEnumeration(
							new AttributeAxis(rtx)), nodeTest);
				}
				break;
			case Axis.CHILD:
				if (rtx.hasFirstChild()) {
					returnVal = new Navigator.AxisFilter(new SaxonEnumeration(
							new ChildAxis(rtx)), nodeTest);
				} else {
					returnVal = EmptyIterator.getInstance();
				}
				break;
			case Axis.DESCENDANT:
				if (hasChildNodes()) {
					returnVal = new Navigator.AxisFilter(new SaxonEnumeration(
							new DescendantAxis(rtx)), nodeTest);
				} else {
					returnVal = EmptyIterator.getInstance();
				}
				break;
			case Axis.DESCENDANT_OR_SELF:
				returnVal = new Navigator.AxisFilter(new SaxonEnumeration(
						new DescendantAxis(rtx, EIncludeSelf.YES)), nodeTest);
				break;
			case Axis.FOLLOWING:
				returnVal = new Navigator.AxisFilter(new SaxonEnumeration(
						new FollowingAxis(rtx)), nodeTest);
				break;
			case Axis.FOLLOWING_SIBLING:
				switch (mNodeKind) {
				case DOCUMENT_ROOT:
				case ATTRIBUTE:
				case NAMESPACE:
					returnVal = EmptyIterator.getInstance();
					break;
				default:
					returnVal = new Navigator.AxisFilter(new SaxonEnumeration(
							new FollowingSiblingAxis(rtx)), nodeTest);
					break;
				}

			case Axis.NAMESPACE:
				if (getNodeKind() != EKind.ELEMENT.getId()) {
					returnVal = EmptyIterator.getInstance();
				} else {
					returnVal = NamespaceIterator.makeIterator(this, nodeTest);
				}
				break;
			case Axis.PARENT:
				if (rtx.getParentKey() == EKind.DOCUMENT_ROOT.getId()) {
					returnVal = EmptyIterator.getInstance();
				} else {
					returnVal = new Navigator.AxisFilter(new SaxonEnumeration(
							new ParentAxis(rtx)), nodeTest);
				}
			case Axis.PRECEDING:
				returnVal = new Navigator.AxisFilter(new SaxonEnumeration(
						new PrecedingAxis(rtx)), nodeTest);
				break;
			case Axis.PRECEDING_SIBLING:
				switch (mNodeKind) {
				case DOCUMENT_ROOT:
				case ATTRIBUTE:
				case NAMESPACE:
					returnVal = EmptyIterator.getInstance();
					break;
				default:
					returnVal = new Navigator.AxisFilter(new SaxonEnumeration(
							new PrecedingSiblingAxis(rtx)), nodeTest);
					break;
				}

			case Axis.SELF:
				returnVal = Navigator.filteredSingleton(this, nodeTest);
				break;

			case Axis.PRECEDING_OR_ANCESTOR:
				returnVal = new Navigator.AxisFilter(
						new Navigator.PrecedingEnumeration(this, true), nodeTest);
				break;
			default:
				throw new IllegalArgumentException("Unknown axis number " + axisNumber);
			}
		} catch (final SirixException exc) {
			LOGGER.error(exc.toString());
		}
		return returnVal;
	}

	@Override
	public void setSystemId(final String systemId) {
		mDocWrapper.setBaseURI(systemId);
	}

	@Override
	public SequenceIterator getTypedValue() throws XPathException {
		return SingletonIterator.makeIterator((AtomicValue) atomize());
	}

	@Override
	public int getSiblingPosition() {
		int index = 0;
		try {
			final INodeReadTrx rtx = createRtxAndMove();
			while (rtx.hasLeftSibling()) {
				rtx.moveToLeftSibling();
				index++;
			}
			rtx.close();
		} catch (final SirixException exc) {
			LOGGER.error(exc.toString());
		}
		return index;
	}

	/**
	 * Create a new {@link IReadTransaction} and move to {@link mKey}.
	 * 
	 * @return new read transaction instance which is moved to {@link mKey}
	 * @throws SirixException
	 *           if sirix fails to setup new transaction
	 */
	private final INodeReadTrx createRtxAndMove() throws SirixException {
		final INodeReadTrx rtx = mDocWrapper.mSession.beginNodeReadTrx(mRevision);
		rtx.moveTo(mKey);
		return rtx;
	}

	/**
	 * Treat a node value of null as an empty string.
	 * 
	 * @param s
	 *          The node value.
	 * @return a zero-length string if s is null, otherwise s.
	 */
	private static String emptyIfNull(final String s) {
		return (s == null ? "" : s);
	}

	/**
	 * <h1>SaxonEnumeration</h1>
	 * 
	 * <p>
	 * Saxon adaptor for axis iterations.
	 * </p>
	 */
	public final class SaxonEnumeration extends Navigator.BaseEnumeration {

		/** Sirix {@link IAxis} iterator. */
		private final IAxis mAxis;

		/**
		 * Constructor.
		 * 
		 * @param pAxis
		 *          Sirix {@link IAxis}
		 */
		public SaxonEnumeration(@Nonnull final IAxis pAxis) {
			mAxis = checkNotNull(pAxis);
		}

		@Override
		public void advance() {
			if (mAxis.hasNext()) {
				final long nextKey = mAxis.next();
				try {
					current = new NodeWrapper(mDocWrapper, nextKey);
				} catch (final SirixException e) {
					current = null;
					try {
						mAxis.getTrx().close();
					} catch (final SirixException exc) {
						LOGGER.error(exc.toString());
					}
				}
			} else {
				current = null;
				try {
					mAxis.getTrx().close();
				} catch (SirixException exc) {
					LOGGER.error(exc.toString());
				}
			}
		}

		@Override
		public SequenceIterator getAnother() {
			return new SaxonEnumeration(mAxis);
		}
	}

	/**
	 * Get current nodeKey.
	 * 
	 * @return current nodeKey
	 */
	public long getKey() {
		return mKey;
	}
}
