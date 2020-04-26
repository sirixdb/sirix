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

package org.sirix.gui.view.text;

import static com.google.common.base.Preconditions.checkNotNull;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Namespace;
import javax.xml.stream.events.XMLEvent;
import org.brackit.xquery.atomic.QNm;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.IncludeSelf;
import org.sirix.axis.filter.FilterAxis;
import org.sirix.axis.filter.xml.TextFilter;
import org.sirix.diff.DiffFactory.DiffType;
import org.sirix.exception.SirixException;
import org.sirix.gui.view.DiffAxis;
import org.sirix.gui.view.TransactionTuple;
import org.sirix.gui.view.VisualItemAxis;
import org.sirix.node.Kind;
import org.sirix.settings.Fixed;
import org.sirix.utils.XMLToken;

/**
 *
 * <p>
 * Provides a StAX implementation (event API) for retrieving a sirix database (based on diffs).
 * </p>
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class StAXDiffSerializer implements XMLEventReader {

  /**
   * Determines if start tags have to be closed, thus if end tags have to be emitted.
   */
  private transient boolean mCloseElements;

  /** {@link XMLEvent}. */
  private transient XMLEvent mEvent;

  /** {@link XMLEventFactory} to create events. */
  private final XMLEventFactory mFac = XMLEventFactory.newFactory();

  /** Determines if all end tags have been emitted. */
  private transient boolean mCloseElementsEmitted;

  /** Determines if nextTag() method has been called. */
  private transient boolean mNextTag;

  /** {@link DiffAxis} for iteration. */
  private final DiffAxis mAxis;

  /** Stack for reading end element. */
  private final Deque<TransactionTuple> mStack;

  private final Deque<TransactionTuple> mUpdatedStack;

  /**
   * Determines if the cursor has to move back after empty elements or go up in the tree (used in
   * getElementText().
   */
  private transient boolean mToLastKey;

  /**
   * Last emitted key (start tags, text... except end tags; used in getElementText()).
   */
  private transient long mLastKey;

  /** Determines if {@link NodeReadTrx} should be closed afterwards. */
  private transient boolean mCloseRtx;

  /** First call. */
  private transient boolean mFirst;

  /**
   * Determines if the serializer must emit a new element in the next call to nextEvent.
   */
  private boolean mHasNext;

  /** Diff type. */
  private DiffType mDiff;

  /** Current depth. */
  private int mDepth;

  /** Determines if end document event should be emitted or not. */
  private transient boolean mEmitEndDocument;

  private boolean mFirstUpdate = true;

  /**
   * Initialize XMLStreamReader implementation with transaction. The cursor points to the node the
   * XMLStreamReader starts to read. Do not serialize the tank ids.
   *
   * @param pItems {@link VisualItemAxis} which is used to iterate over and generate StAX events
   */
  public StAXDiffSerializer(final DiffAxis pItems) {
    this(pItems, true);
  }

  /**
   * Initialize XMLStreamReader implementation with transaction. The cursor points to the node the
   * XMLStreamReader starts to read. Do not serialize the tank ids.
   *
   * @param pItems {@link VisualItemAxis} which is used to iterate over and generate StAX events
   * @param pCloseRtx Determines if rtx should be closed afterwards.
   */
  public StAXDiffSerializer(final DiffAxis pItems, final boolean pCloseRtx) {
    mNextTag = false;
    mAxis = checkNotNull(pItems);
    mCloseRtx = pCloseRtx;
    mStack = new ArrayDeque<>();
    mUpdatedStack = new ArrayDeque<>();
    mFirst = true;
    mHasNext = true;
    mDepth = 0;
    mEmitEndDocument = true;
  }

  /**
   * Emit end tag.
   *
   * @param rtx read-only transaction
   */
  private void emitEndTag(final XmlNodeReadOnlyTrx rtx) {
    assert rtx != null;
    final long nodeKey = rtx.getNodeKey();
    final QNm name = rtx.getName();
    mEvent = mFac.createEndElement(new QName(name.getNamespaceURI(), name.getLocalName(), name.getPrefix()),
        new NamespaceIterator(rtx));
    rtx.moveTo(nodeKey);
  }

  /**
   * Emit a node.
   *
   * @param rtx read-only transaction
   */
  private void emitNode(final XmlNodeReadOnlyTrx rtx) {
    assert rtx != null;
    switch (rtx.getKind()) {
      case XDM_DOCUMENT:
        mEvent = mFac.createStartDocument();
        break;
      case ELEMENT:
        final long key = rtx.getNodeKey();
        final QNm qName = rtx.getName();
        mEvent = mFac.createStartElement(new QName(qName.getNamespaceURI(), qName.getLocalName(), qName.getPrefix()),
            new AttributeIterator(rtx), new NamespaceIterator(rtx));
        rtx.moveTo(key);
        break;
      case TEXT:
        mEvent = mFac.createCharacters(XMLToken.escapeContent(rtx.getValue()));
        break;
      default:
        throw new IllegalStateException("Kind not known!");
    }
  }

  @Override
  public void close() throws XMLStreamException {
    if (mCloseRtx) {
      try {
        mAxis.getTransaction().close();
      } catch (final SirixException e) {
        throw new XMLStreamException(e);
      }
    }
  }

  @Override
  public String getElementText() throws XMLStreamException {
    final XmlNodeReadOnlyTrx rtx = mAxis.getTransaction();
    final long nodeKey = rtx.getNodeKey();

    /*
     * The cursor has to move back (once) after determining, that a closing tag would be the next event
     * (precond: closeElement and either goBack or goUp is true).
     */
    if (mCloseElements && mToLastKey) {
      rtx.moveTo(mLastKey);
    }

    if (mEvent.getEventType() != XMLStreamConstants.START_ELEMENT) {
      rtx.moveTo(nodeKey);
      throw new XMLStreamException("getElementText() only can be called on a start element");
    }
    final FilterAxis<XmlNodeReadOnlyTrx> textFilterAxis =
        new FilterAxis<XmlNodeReadOnlyTrx>(new DescendantAxis(rtx), new TextFilter(rtx));
    final StringBuilder strBuilder = new StringBuilder();

    while (textFilterAxis.hasNext()) {
      textFilterAxis.next();
      strBuilder.append(mAxis.getTransaction().getValue());
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

    if (!mUpdatedStack.isEmpty()) {
      retVal = true;
    } else {
      if (!mStack.isEmpty() && (mCloseElements || mCloseElementsEmitted)) {
        /*
         * mAxis.hasNext() can't be used in this case, because it would iterate to the next node but at
         * first all end-tags have to be emitted.
         */
        retVal = true;
      } else {
        retVal = mAxis.hasNext();
      }

      if (!retVal && mEmitEndDocument) {
        mHasNext = false;
        retVal = !retVal;
      }
    }

    return retVal;
  }

  @Override
  public XMLEvent nextEvent() throws XMLStreamException {
    try {
      if (!mUpdatedStack.isEmpty()) {
        emit(mAxis.getTransaction());
      } else {
        if (!mCloseElements && !mCloseElementsEmitted) {
          mAxis.next();

          if (mNextTag) {
            if (mAxis.getTransaction().getKind() != Kind.ELEMENT) {
              throw new XMLStreamException("The next tag isn't a start- or end-tag!");
            }
            mNextTag = false;
          }
        }
        if (!mHasNext && mEmitEndDocument) {
          mEmitEndDocument = false;
          mEvent = mFac.createEndDocument();
        } else {
          emit(mAxis.getTransaction());
        }
      }
    } catch (final IOException e) {
      throw new IllegalStateException(e);
    }

    return mEvent;
  }

  @Override
  public XMLEvent nextTag() throws XMLStreamException {
    mNextTag = true;
    return nextEvent();
  }

  @Override
  public XMLEvent peek() throws XMLStreamException {
    final long currNodeKey = mAxis.getTransaction().getNodeKey();
    final NodeReadTrx rtx = mAxis.getTransaction();
    try {
      if (!mHasNext && mEmitEndDocument) {
        mEvent = mFac.createEndDocument();
      } else if (!mHasNext) {
        return null;
      } else {
        if (mCloseElements && !mCloseElementsEmitted) {
          final TransactionTuple trxTuple = mStack.peek();
          final NodeReadTrx trx = trxTuple.getRtx();
          final long nodeKey = trx.getNodeKey();
          trx.moveTo(trxTuple.getKey());
          emitEndTag(trxTuple.getRtx());
          trx.moveTo(nodeKey);
        } else {
          if (mFirst && mAxis.isSelfIncluded() == IncludeSelf.YES) {
            emitNode(rtx);
          } else {
            if (rtx.hasFirstChild()) {
              rtx.moveToFirstChild();
              emitNode(rtx);
            } else if (rtx.hasRightSibling()) {
              rtx.moveToRightSibling();
              final Kind nodeKind = rtx.getKind();
              processNode(nodeKind);
            } else if (rtx.hasParent()) {
              rtx.moveToParent();
              emitEndTag(rtx);
            }
          }
        }
      }
    } catch (final IOException e) {
      throw new XMLStreamException(e);
    }

    rtx.moveTo(currNodeKey);
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
   * Get diff type.
   *
   * @return {@link DiffType} value
   */
  public DiffType getDiff() {
    return mDiff;
  }

  /**
   * Determines if a node or an end element has to be emitted.
   *
   * @param pNodeKind the node kind
   * @throws IOException if any I/O error occurred
   */
  private void processNode(final Kind pNodeKind) throws IOException {
    assert pNodeKind != null;
    switch (pNodeKind) {
      case ELEMENT:
        emitEndTag(mAxis.getTransaction());
        break;
      case TEXT:
        emitNode(mAxis.getTransaction());
        break;
      default:
        // Do nothing.
    }
  }

  /**
   * Move to node and emit it.
   *
   * @param pRtx Read Transaction.
   * @throws IOException if any I/O error occurred
   */
  private void emit(final NodeReadTrx pRtx) throws IOException {
    assert pRtx != null;
    // Emit pending update elements.
    if (!mUpdatedStack.isEmpty()) {
      mDiff = DiffType.UPDATED;
      final TransactionTuple tuple = mUpdatedStack.peek();
      final NodeReadTrx trx = tuple.getRtx();
      final long nodeKey = trx.getNodeKey();
      trx.moveTo(tuple.getKey());
      if (mFirstUpdate && mDepth <= tuple.getDepth() && trx.getKind() == Kind.ELEMENT) {
        mFirstUpdate = false;
        mStack.pop();
        emitEndTag(trx);
      } else {
        mUpdatedStack.pop();
        emitNode(trx);
        if (trx.getKind() == Kind.ELEMENT) {
          mStack.push(tuple);
        }
        trx.moveTo(nodeKey);
        assert mUpdatedStack.isEmpty();
      }
    } else // Emit pending end elements.
    if (mCloseElements) {
      final long pNodeKey = pRtx.getNodeKey();
      final TransactionTuple tuple = mStack.peek();
      NodeReadTrx rtx = tuple.getRtx();
      final long nodeKey = rtx.getNodeKey();
      int depth = tuple.getDepth();
      mDiff = tuple.getDiff();
      if (mDepth < depth || (mDiff == DiffType.UPDATED && mFirstUpdate)) {
        if (mDiff == DiffType.UPDATED) {
          mFirstUpdate = false;
        }
        rtx.moveTo(mStack.pop().getKey());
        emitEndTag(rtx);
        rtx.moveTo(nodeKey);
      } else {
        mFirstUpdate = true;
        rtx.moveTo(mStack.pop().getKey());
        emitEndTag(rtx);
        rtx.moveTo(nodeKey);
        mCloseElementsEmitted = true;
        mCloseElements = false;
      }
      pRtx.moveTo(pNodeKey);
    } else {
      mCloseElementsEmitted = false;

      // Emit node.
      emitNode(pRtx);

      mLastKey = pRtx.getNodeKey();

      if (mLastKey == 2878) {
        System.out.println();
      }
      mDiff = mAxis.getDiff();
      final int depth = mAxis.getDepth();

      if (mDiff == DiffType.UPDATED) {
        mFirstUpdate = true;
        mUpdatedStack.push(
            new TransactionTuple(mAxis.getOldRtx().getNodeKey(), mAxis.getOldRtx(), mAxis.getDiff(), mAxis.getDepth()));
      }

      // Push end element to stack if we are a start element.
      if (pRtx.getKind() == Kind.ELEMENT) {
        mStack.push(new TransactionTuple(mLastKey, pRtx, mAxis.getDiff(), mAxis.getDepth()));
      }

      final Kind nodeKind = pRtx.getKind();

      // Remember to emit all pending end elements from stack if
      // required.
      if (mLastKey != Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
        if (mAxis.hasNext()) {
          final long peekKey = mAxis.peek();
          mDepth = mAxis.getDepth();
          mAxis.getTransaction().moveTo(peekKey);
          if ((depth > mDepth) || (nodeKind == Kind.ELEMENT && mLastKey != mAxis.getTransaction().getParentKey())) {
            moveToNextNode();
          } else {
            mAxis.getTransaction().moveTo(mLastKey);
          }
        } else {
          mToLastKey = true;
          mCloseElements = true;
          mAxis.getTransaction().moveToDocumentRoot();
          mDepth = 0;
        }
      }
    }
  }

  /**
   * Move to next node in tree either in case of a right sibling of an empty element or if no further
   * child and no right sibling can be found, so that the next node is in the following axis.
   */
  private void moveToNextNode() {
    mToLastKey = true;
    if (mAxis.hasNext()) {
      mAxis.next();
    }
    mCloseElements = true;
  }

  /**
   * Implementation of an attribute-iterator.
   */
  private final static class AttributeIterator implements Iterator<Attribute> {

    /**
     * {@link XmlNodeReadOnlyTrx} implementation.
     */
    private final XmlNodeReadOnlyTrx mRtx;

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
     * @param rtx reference implementing the {@link XmlNodeReadOnlyTrx} interface
     */
    public AttributeIterator(final XmlNodeReadOnlyTrx rtx) {
      mRtx = checkNotNull(rtx);
      mNodeKey = mRtx.getNodeKey();
      mIndex = 0;

      if (mRtx.getKind() == Kind.ELEMENT) {
        mAttCount = mRtx.getAttributeCount();
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
      mRtx.moveTo(mNodeKey);
      mRtx.moveToAttribute(mIndex++);
      assert mRtx.getKind() == Kind.ATTRIBUTE;
      final QNm qName = mRtx.getName();
      final String value = XMLToken.escapeAttribute(mRtx.getValue());
      mRtx.moveTo(mNodeKey);
      return mFac.createAttribute(new QName(qName.getNamespaceURI(), qName.getLocalName(), qName.getPrefix()), value);
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
    private final XmlNodeReadOnlyTrx mRtx;

    /** Number of namespace nodes. */
    private final int mNamespCount;

    /** Index of namespace node. */
    private int mIndex;

    /** Node key. */
    private final long mNodeKey;

    /** Factory to create nodes {@link XMLEventFactory}. */
    private final transient XMLEventFactory mFac = XMLEventFactory.newInstance();

    /**
     * Constructor.
     *
     * @param rtx reference implementing the {@link XmlNodeReadOnlyTrx} interface
     */
    public NamespaceIterator(final XmlNodeReadOnlyTrx rtx) {
      mRtx = checkNotNull(rtx);
      mNodeKey = mRtx.getNodeKey();
      mIndex = 0;

      if (mRtx.getKind() == Kind.ELEMENT) {
        mNamespCount = mRtx.getNamespaceCount();
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
      mRtx.moveTo(mNodeKey);
      mRtx.moveToNamespace(mIndex++);
      assert mRtx.getKind() == Kind.NAMESPACE;
      final QNm qName = mRtx.getName();
      mRtx.moveTo(mNodeKey);
      return mFac.createNamespace(qName.getLocalName(), qName.getNamespaceURI());
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Not supported!");
    }
  }
}
