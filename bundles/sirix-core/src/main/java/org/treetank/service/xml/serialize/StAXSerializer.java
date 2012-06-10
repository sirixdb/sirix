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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

import javax.annotation.Nonnull;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Namespace;
import javax.xml.stream.events.XMLEvent;

import org.treetank.api.IAxis;
import org.treetank.api.INodeReadTrx;
import org.treetank.axis.DescendantAxis;
import org.treetank.axis.EIncludeSelf;
import org.treetank.axis.FilterAxis;
import org.treetank.axis.filter.TextFilter;
import org.treetank.exception.AbsTTException;
import org.treetank.node.ENode;
import org.treetank.node.ElementNode;
import org.treetank.utils.XMLToken;

/**
 * <h1>StAXSerializer</h1>
 * 
 * <p>
 * Provides a StAX implementation (event API) for retrieving a Treetank database.
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

  /** {@link IAxis} for iteration. */
  private final transient IAxis mAxis;

  /** Stack for reading end element. */
  private final Deque<Long> mStack;

  /**
   * Determines if the cursor has to move back after empty elements or go up in the tree (used in
   * getElementText().
   */
  private transient boolean mToLastKey;

  // /**
  // * Determines if the cursor has moved up and therefore has to move back
  // * after to the right node (used in getElementText()).
  // */
  // private transient boolean mGoUp;

  /**
   * Last emitted key (start tags, text... except end tags; used in
   * getElementText()).
   */
  private transient long mLastKey;

  /** Determines if {@link INodeReadTrx} should be closed afterwards. */
  private transient boolean mCloseRtx;

  /** First call. */
  private transient boolean mFirst;

  /** Determines if end document event should be emitted or not. */
  private transient boolean mEmitEndDocument;

  /** Determines if the serializer must emit a new element in the next call to nextEvent. */
  private boolean mHasNext;

  /**
   * Initialize XMLStreamReader implementation with transaction. The cursor
   * points to the node the XMLStreamReader starts to read. Do not serialize
   * the tank ids.
   * 
   * @param pAxis
   *          {@link INodeReadTrx} which is used to iterate over and generate StAX events
   */
  public StAXSerializer(@Nonnull final INodeReadTrx pRtx) {
    this(pRtx, true);
  }

  /**
   * Initialize XMLStreamReader implementation with transaction. The cursor
   * points to the node the XMLStreamReader starts to read. Do not serialize
   * the tank ids.
   * 
   * @param pAxis
   *          {@link pRtx} which is used to iterate over and generate StAX events
   * @param pCloseRtx
   *          Determines if rtx should be closed afterwards.
   */
  public StAXSerializer(@Nonnull final INodeReadTrx pRtx, final boolean pCloseRtx) {
    mNextTag = false;
    mAxis = new DescendantAxis(checkNotNull(pRtx), EIncludeSelf.YES);
    mCloseRtx = pCloseRtx;
    mStack = new ArrayDeque<Long>();
    mFirst = true;
    mEmitEndDocument = true;
    mHasNext = true;
  }

  /**
   * Emit end tag.
   * 
   * @param pRTX
   *          Treetank reading transaction {@link INodeReadTrx}.
   */
  private void emitEndTag(final INodeReadTrx pRTX) {
    assert pRTX != null;
    final long nodeKey = pRTX.getNode().getNodeKey();
    mEvent = mFac.createEndElement(pRTX.getQNameOfCurrentNode(), new NamespaceIterator(pRTX));
    pRTX.moveTo(nodeKey);
  }

  /**
   * Emit a node.
   * 
   * @param pRTX
   *          Treetank reading transaction {@link INodeReadTrx}.
   */
  private void emitNode(final INodeReadTrx pRTX) {
    assert pRTX != null;
    switch (pRTX.getNode().getKind()) {
    case ROOT_KIND:
      mEvent = mFac.createStartDocument();
      break;
    case ELEMENT_KIND:
      final long key = pRTX.getNode().getNodeKey();
      final QName qName = pRTX.getQNameOfCurrentNode();
      mEvent = mFac.createStartElement(qName, new AttributeIterator(pRTX), new NamespaceIterator(pRTX));
      pRTX.moveTo(key);
      break;
    case TEXT_KIND:
      mEvent = mFac.createCharacters(XMLToken.escape(pRTX.getValueOfCurrentNode()));
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
      } catch (final AbsTTException e) {
        throw new XMLStreamException(e);
      }
    }
  }

  @Override
  public String getElementText() throws XMLStreamException {
    final INodeReadTrx rtx = mAxis.getTransaction();
    final long nodeKey = rtx.getNode().getNodeKey();

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
      throw new XMLStreamException("getElementText() only can be called on a start element");
    }
    final FilterAxis textFilterAxis = new FilterAxis(new DescendantAxis(rtx), new TextFilter(rtx));
    final StringBuilder strBuilder = new StringBuilder();

    while (textFilterAxis.hasNext()) {
      textFilterAxis.next();
      strBuilder.append(mAxis.getTransaction().getValueOfCurrentNode());
    }

    rtx.moveTo(nodeKey);
    return XMLToken.escape(strBuilder.toString());
  }

  @Override
  public Object getProperty(final String pName) throws IllegalArgumentException {
    throw new UnsupportedOperationException("Not supported by Treetank!");
  }

  @Override
  public boolean hasNext() {
    boolean retVal = false;

    if (!mStack.isEmpty() && (mCloseElements || mCloseElementsEmitted)) {
      /*
       * mAxis.hasNext() can't be used in this case, because it would iterate
       * to the next node but at first all end-tags have to be emitted.
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
    try {
      if (!mCloseElements && !mCloseElementsEmitted) {
        mKey = mAxis.next();

        if (mNextTag) {
          if (mAxis.getTransaction().getNode().getKind() != ENode.ELEMENT_KIND) {
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
    final long currNodeKey = mAxis.getTransaction().getNode().getNodeKey();
    final INodeReadTrx rtx = mAxis.getTransaction();
    try {
      if (!mHasNext && mEmitEndDocument) {
        mEvent = mFac.createEndDocument();
      } else if (!mHasNext) {
        return null;
      } else {
        if (mCloseElements && !mCloseElementsEmitted) {
          rtx.moveTo(mStack.peek());
          emitEndTag(rtx);
        } else {
          if (mFirst && mAxis.isSelfIncluded() == EIncludeSelf.YES) {
            emitNode(rtx);
          } else {
            if (rtx.getStructuralNode().hasFirstChild()) {
              rtx.moveToFirstChild();
              emitNode(rtx);
            } else if (rtx.getStructuralNode().hasRightSibling()) {
              rtx.moveToRightSibling();
              final ENode nodeKind = rtx.getNode().getKind();
              processNode(nodeKind);
            } else if (rtx.getStructuralNode().hasParent()) {
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
   * Determines if a node or an end element has to be emitted.
   * 
   * @param pNodeKind
   *          the node kind
   * @throws IOException
   *           if any I/O error occurred
   */
  private void processNode(final ENode pNodeKind) throws IOException {
    assert pNodeKind != null;
    switch (pNodeKind) {
    case ELEMENT_KIND:
      emitEndTag(mAxis.getTransaction());
      break;
    case TEXT_KIND:
      emitNode(mAxis.getTransaction());
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
  private void emit(final INodeReadTrx pRtx) throws IOException {
    assert pRtx != null;
    // Emit pending end elements.
    if (mCloseElements) {
      if (!mStack.isEmpty() && mStack.peek() != pRtx.getStructuralNode().getLeftSiblingKey()) {
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

      mLastKey = pRtx.getNode().getNodeKey();

      // Push end element to stack if we are a start element.
      if (pRtx.getNode().getKind() == ENode.ELEMENT_KIND) {
        mStack.push(mLastKey);
      }

      // Remember to emit all pending end elements from stack if
      // required.
      if ((!pRtx.getStructuralNode().hasFirstChild() && !pRtx.getStructuralNode().hasRightSibling())
        || (pRtx.getNode().getKind() == ENode.ELEMENT_KIND && !pRtx.getStructuralNode().hasFirstChild())) {
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
     * {@link INodeReadTrx} implementation.
     */
    private final INodeReadTrx mRTX;

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
     *          reference implementing the {@link INodeReadTrx} interface
     */
    public AttributeIterator(final INodeReadTrx pRtx) {
      mRTX = checkNotNull(pRtx);
      mNodeKey = mRTX.getNode().getNodeKey();
      mIndex = 0;

      if (mRTX.getNode().getKind() == ENode.ELEMENT_KIND) {
        mAttCount = ((ElementNode)mRTX.getNode()).getAttributeCount();
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
      assert mRTX.getNode().getKind() == ENode.ATTRIBUTE_KIND;
      final QName qName = mRTX.getQNameOfCurrentNode();
      final String value = XMLToken.escape(mRTX.getValueOfCurrentNode());
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
     * Treetank {@link INodeReadTrx}.
     */
    private final INodeReadTrx mRTX;

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
     * @param pRtx
     *          reference implementing the {@link INodeReadTrx} interface
     */
    public NamespaceIterator(final INodeReadTrx pRtx) {
      mRTX = checkNotNull(pRtx);
      mNodeKey = mRTX.getNode().getNodeKey();
      mIndex = 0;

      if (mRTX.getNode().getKind() == ENode.ELEMENT_KIND) {
        mNamespCount = ((ElementNode)mRTX.getNode()).getNamespaceCount();
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
      assert mRTX.getNode().getKind() == ENode.NAMESPACE_KIND;
      final QName qName = mRTX.getQNameOfCurrentNode();
      mRTX.moveTo(mNodeKey);
      return mFac.createNamespace(qName.getLocalPart(), qName.getNamespaceURI());
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Not supported!");
    }
  }
}
