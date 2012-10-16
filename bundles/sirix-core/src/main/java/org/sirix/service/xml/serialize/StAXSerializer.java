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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.annotation.Nonnull;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Namespace;
import javax.xml.stream.events.XMLEvent;

import org.sirix.api.Axis;
import org.sirix.api.NodeReadTrx;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.IncludeSelf;
import org.sirix.axis.filter.FilterAxis;
import org.sirix.axis.filter.TextFilter;
import org.sirix.exception.SirixException;
import org.sirix.node.Kind;
import org.sirix.utils.XMLToken;

/**
 * <h1>StAXSerializer</h1>
 * 
 * <p>
 * Provides a StAX implementation (event API) for retrieving a sirix database.
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class StAXSerializer implements XMLEventReader {

	/**
	 * Determines if start tags have to be closed, thus if end tags have to be
	 * emitted.
	 */
	private transient boolean mCloseElements;

	/** {@link XMLEvent}. */
	private transient XMLEvent mEvent;

	/** {@link XMLEventFactory} to create events. */
	private final XMLEventFactory mFac = XMLEventFactory.newFactory();

	/** Current node key. */
	private transient long mKey;

	/** Determines if all end tags have been emitted. */
	private transient boolean mCloseElementsEmitted;

	/** Determines if nextTag() method has been called. */
	private transient boolean mNextTag;

	/** {@link Axis} for iteration. */
	private final Axis mAxis;

	/** Stack for reading end element. */
	private final Deque<Long> mStack;

	/**
	 * Determines if the cursor has to move back after empty elements or go up in
	 * the tree (used in getElementText().
	 */
	private transient boolean mToLastKey;

	/**
	 * Last emitted key (start tags, text... except end tags; used in
	 * getElementText()).
	 */
	private transient long mLastKey;

	/** Determines if {@link NodeReadTrx} should be closed afterwards. */
	private transient boolean mCloseRtx;

	/** First call. */
	private transient boolean mFirst;

	/** Determines if end document event should be emitted or not. */
	private transient boolean mEmitEndDocument;

	/**
	 * Determines if the serializer must emit a new element in the next call to
	 * nextEvent.
	 */
	private boolean mHasNext;

	private long mStartRightSibling;

	private long mStartParent;

	private boolean mCalledHasNext;

	/**
	 * Initialize XMLStreamReader implementation with transaction. The cursor
	 * points to the node the XMLStreamReader starts to read. Do not serialize the
	 * tank ids.
	 * 
	 * @param pAxis
	 *          {@link NodeReadTrx} which is used to iterate over and generate
	 *          StAX events
	 */
	public StAXSerializer(@Nonnull final NodeReadTrx pRtx) {
		this(pRtx, true);
	}

	/**
	 * Initialize XMLStreamReader implementation with transaction. The cursor
	 * points to the node the XMLStreamReader starts to read. Do not serialize the
	 * tank ids.
	 * 
	 * @param pAxis
	 *          {@link pRtx} which is used to iterate over and generate StAX
	 *          events
	 * @param pCloseRtx
	 *          Determines if rtx should be closed afterwards.
	 */
	public StAXSerializer(final @Nonnull NodeReadTrx pRtx,
			final boolean pCloseRtx) {
		mNextTag = false;
		mAxis = new DescendantAxis(checkNotNull(pRtx), IncludeSelf.YES);
		mCloseRtx = pCloseRtx;
		mStack = new ArrayDeque<Long>();
		mFirst = true;
		mEmitEndDocument = true;
		mHasNext = true;
		mStartParent = pRtx.getParentKey();
		mStartRightSibling = pRtx.getRightSiblingKey();
	}

	/**
	 * Emit end tag.
	 * 
	 * @param pRTX
	 *          sirix reading transaction {@link NodeReadTrx}.
	 */
	private void emitEndTag(final @Nonnull NodeReadTrx pRTX) {
		final long nodeKey = pRTX.getNodeKey();
		mEvent = mFac
				.createEndElement(pRTX.getName(), new NamespaceIterator(pRTX));
		pRTX.moveTo(nodeKey);
	}

	/**
	 * Emit a node.
	 * 
	 * @param pRTX
	 *          sirix reading transaction {@link NodeReadTrx}.
	 */
	private void emitNode(final @Nonnull NodeReadTrx pRTX) {
		switch (pRTX.getKind()) {
		case DOCUMENT_ROOT:
			mEvent = mFac.createStartDocument();
			break;
		case ELEMENT:
			final long key = pRTX.getNodeKey();
			final QName qName = pRTX.getName();
			mEvent = mFac.createStartElement(qName, new AttributeIterator(pRTX),
					new NamespaceIterator(pRTX));
			pRTX.moveTo(key);
			break;
		case TEXT:
			mEvent = mFac.createCharacters(XMLToken.escapeContent(pRTX.getValue()));
			break;
		case COMMENT:
			mEvent = mFac.createComment(XMLToken.escapeContent(pRTX.getValue()));
			break;
		case PROCESSING:
			mEvent = mFac.createProcessingInstruction(pRTX.getName().getLocalPart(),
					pRTX.getValue());
			break;
		default:
			throw new IllegalStateException("Kind not known!");
		}
	}

	@Override
	public void close() throws XMLStreamException {
		if (mCloseRtx) {
			try {
				mAxis.getTrx().close();
			} catch (final SirixException e) {
				throw new XMLStreamException(e);
			}
		}
	}

	@Override
	public String getElementText() throws XMLStreamException {
		final NodeReadTrx rtx = mAxis.getTrx();
		final long nodeKey = rtx.getNodeKey();

		/*
		 * The cursor has to move back (once) after determining, that a closing tag
		 * would be the next event (precond: closeElement and either goBack or goUp
		 * is true).
		 */
		if (mCloseElements && mToLastKey) {
			rtx.moveTo(mLastKey);
		}

		if (mEvent.getEventType() != XMLStreamConstants.START_ELEMENT) {
			rtx.moveTo(nodeKey);
			throw new XMLStreamException(
					"getElementText() only can be called on a start element");
		}
		final FilterAxis textFilterAxis = new FilterAxis(new DescendantAxis(rtx),
				new TextFilter(rtx));
		final StringBuilder strBuilder = new StringBuilder();

		while (textFilterAxis.hasNext()) {
			textFilterAxis.next();
			strBuilder.append(rtx.getValue());
		}

		rtx.moveTo(nodeKey);
		return XMLToken.escapeContent(strBuilder.toString());
	}

	@Override
	public Object getProperty(final String pName) throws IllegalArgumentException {
		throw new UnsupportedOperationException("Not supported by sirix!");
	}

	@Override
	public boolean hasNext() {
		boolean retVal = false;

		if (!mStack.isEmpty() && (mCloseElements || mCloseElementsEmitted)) {
			/*
			 * mAxis.hasNext() can't be used in this case, because it would iterate to
			 * the next node but at first all end-tags have to be emitted.
			 */
			retVal = true;
		} else {
			retVal = mAxis.hasNext();
		}

		if (!retVal && mEmitEndDocument) {
			mHasNext = false;
			retVal = !retVal;
		}

		return retVal;
	}

	@Override
	public XMLEvent nextEvent() throws XMLStreamException {
		if (!mCalledHasNext) {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
		}
		try {
			if (mHasNext && !mCloseElements && !mCloseElementsEmitted) {
				mKey = mAxis.next();

				if (mNextTag) {
					if (mAxis.getTrx().getKind() != Kind.ELEMENT) {
						throw new XMLStreamException(
								"The next tag isn't a start- or end-tag!");
					}
					mNextTag = false;
				}
			}
			if (!mHasNext && mEmitEndDocument) {
				mEmitEndDocument = false;
				mEvent = mFac.createEndDocument();
			} else {
				emit(mAxis.getTrx());
			}
		} catch (final IOException e) {
			throw new IllegalStateException(e);
		}

		mFirst = false;
		return mEvent;
	}

	@Override
	public XMLEvent nextTag() throws XMLStreamException {
		mNextTag = true;
		return nextEvent();
	}

	@Override
	public XMLEvent peek() throws XMLStreamException {
		final long currNodeKey = mAxis.getTrx().getNodeKey();
		final NodeReadTrx rtx = mAxis.getTrx();
		try {
			if (!mHasNext && mEmitEndDocument) {
				mEvent = mFac.createEndDocument();
			} else if (!mHasNext) {
				return null;
			} else {
				if (mCloseElements && !mCloseElementsEmitted && !mStack.isEmpty()) {
					rtx.moveTo(mStack.peek());
					emitEndTag(rtx);
				} else {
					if (mFirst && mAxis.isSelfIncluded() == IncludeSelf.YES) {
						emitNode(rtx);
					} else {
						if (rtx.hasFirstChild()) {
							rtx.moveToFirstChild();
							emitNode(rtx);
						} else if (rtx.hasRightSibling()) {
							if (rtx.getRightSiblingKey() == mStartRightSibling) {
								mEvent = mFac.createEndDocument();
							} else {
								rtx.moveToRightSibling();
								final Kind nodeKind = rtx.getKind();
								processNode(nodeKind);
							}
						} else if (rtx.hasParent()) {
							if (rtx.getParentKey() == mStartParent) {
								mEvent = mFac.createEndDocument();
							} else {
								rtx.moveToParent();
								emitEndTag(rtx);
							}
						}
					}
				}
			}
		} catch (final IOException e) {
			throw new XMLStreamException(e);
		}

		rtx.moveTo(currNodeKey);
		mFirst = false;
		return mEvent;
	}

	/**
	 * Just calls {@link #nextEvent()}.
	 * 
	 * @return next event
	 */
	@Override
	public Object next() {
		try {
			mEvent = nextEvent();
		} catch (final XMLStreamException e) {
			throw new IllegalStateException(e);
		}

		return mEvent;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("Not supported!");
	}

	/**
	 * Determines if a node or an end element has to be emitted.
	 * 
	 * @param pNodeKind
	 *          the node kind
	 * @throws IOException
	 *           if any I/O error occurred
	 */
	private void processNode(@Nonnull final Kind pNodeKind) throws IOException {
		assert pNodeKind != null;
		switch (pNodeKind) {
		case ELEMENT:
			emitEndTag(mAxis.getTrx());
			break;
		case PROCESSING:
		case COMMENT:
		case TEXT:
			emitNode(mAxis.getTrx());
			break;
		default:
			// Do nothing.
		}
	}

	/**
	 * Move to node and emit it.
	 * 
	 * @param pRtx
	 *          Read Transaction.
	 * @throws IOException
	 *           if any I/O error occurred
	 */
	private void emit(@Nonnull final NodeReadTrx pRtx) throws IOException {
		assert pRtx != null;
		// Emit pending end elements.
		if (mCloseElements) {
			if (!mStack.isEmpty() && mStack.peek() != pRtx.getLeftSiblingKey()) {
				pRtx.moveTo(mStack.pop());
				emitEndTag(pRtx);
				pRtx.moveTo(mKey);
			} else if (!mStack.isEmpty()) {
				pRtx.moveTo(mStack.pop());
				emitEndTag(pRtx);
				pRtx.moveTo(mKey);
				mCloseElementsEmitted = true;
				mCloseElements = false;
			}
		} else {
			mCloseElementsEmitted = false;

			// Emit node.
			emitNode(pRtx);

			mLastKey = pRtx.getNodeKey();

			// Push end element to stack if we are a start element.
			if (pRtx.getKind() == Kind.ELEMENT) {
				mStack.push(mLastKey);
			}

			// Remember to emit all pending end elements from stack if
			// required.
			if ((!pRtx.hasFirstChild() && !pRtx.hasRightSibling())
					|| (pRtx.getKind() == Kind.ELEMENT && !pRtx.hasFirstChild())) {
				moveToNextNode();
			}
		}
	}

	/**
	 * Move to next node in tree either in case of a right sibling of an empty
	 * element or if no further child and no right sibling can be found, so that
	 * the next node is in the following axis.
	 */
	private void moveToNextNode() {
		mToLastKey = true;
		if (mAxis.hasNext()) {
			mKey = mAxis.next();
		}
		mCloseElements = true;
	}

	/**
	 * Implementation of an attribute-iterator.
	 */
	private final static class AttributeIterator implements Iterator<Attribute> {

		/**
		 * {@link NodeReadTrx} implementation.
		 */
		private final NodeReadTrx mRTX;

		/** Number of attribute nodes. */
		private final int mAttCount;

		/** Index of attribute node. */
		private int mIndex;

		/** Node key. */
		private final long mNodeKey;

		/** Factory to create nodes {@link XMLEventFactory}. */
		private final transient XMLEventFactory mFac = XMLEventFactory.newFactory();

		/**
		 * Constructor.
		 * 
		 * @param pRtx
		 *          reference implementing the {@link NodeReadTrx} interface
		 */
		public AttributeIterator(final NodeReadTrx pRtx) {
			mRTX = checkNotNull(pRtx);
			mNodeKey = mRTX.getNodeKey();
			mIndex = 0;

			if (mRTX.getKind() == Kind.ELEMENT) {
				mAttCount = mRTX.getAttributeCount();
			} else {
				mAttCount = 0;
			}
		}

		@Override
		public boolean hasNext() {
			boolean retVal = false;

			if (mIndex < mAttCount) {
				retVal = true;
			}

			return retVal;
		}

		@Override
		public Attribute next() {
			mRTX.moveTo(mNodeKey);
			mRTX.moveToAttribute(mIndex++);
			assert mRTX.getKind() == Kind.ATTRIBUTE;
			final QName qName = mRTX.getName();
			final String value = XMLToken.escapeAttribute(mRTX.getValue());
			mRTX.moveTo(mNodeKey);
			return mFac.createAttribute(qName, value);
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Not supported!");
		}
	}

	/**
	 * Implementation of a namespace iterator.
	 */
	private final static class NamespaceIterator implements Iterator<Namespace> {

		/**
		 * sirix {@link NodeReadTrx}.
		 */
		private final NodeReadTrx mRTX;

		/** Number of namespace nodes. */
		private final int mNamespCount;

		/** Index of namespace node. */
		private int mIndex;

		/** Node key. */
		private final long mNodeKey;

		/** Factory to create nodes {@link XMLEventFactory}. */
		private final transient XMLEventFactory mFac = XMLEventFactory
				.newInstance();

		/**
		 * Constructor.
		 * 
		 * @param pRtx
		 *          reference implementing the {@link NodeReadTrx} interface
		 */
		public NamespaceIterator(final NodeReadTrx pRtx) {
			mRTX = checkNotNull(pRtx);
			mNodeKey = mRTX.getNodeKey();
			mIndex = 0;

			if (mRTX.getKind() == Kind.ELEMENT) {
				mNamespCount = mRTX.getNamespaceCount();
			} else {
				mNamespCount = 0;
			}
		}

		@Override
		public boolean hasNext() {
			boolean retVal = false;

			if (mIndex < mNamespCount) {
				retVal = true;
			}

			return retVal;
		}

		@Override
		public Namespace next() {
			mRTX.moveTo(mNodeKey);
			mRTX.moveToNamespace(mIndex++);
			assert mRTX.getKind() == Kind.NAMESPACE;
			final QName qName = mRTX.getName();
			mRTX.moveTo(mNodeKey);
			return mFac
					.createNamespace(qName.getLocalPart(), qName.getNamespaceURI());
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Not supported!");
		}
	}
}
