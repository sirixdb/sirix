package org.sirix.service.xml.shredder;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayDeque;
import java.util.Deque;

import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

import org.sirix.api.INodeWriteTrx;
import org.sirix.exception.AbsTTException;
import org.sirix.node.EKind;
import org.sirix.settings.EFixed;

/**
 * Skeleton implementation of {@link IShredder} interface methods.
 * 
 * All methods throw {@link NullPointerException}s in case of {@code null} values for reference parameters and
 * check the arguments, whereas in case they are not valid a {@link IllegalArgumentException} is thrown.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * @author Marc Kramis, Seabix GmbH
 * 
 */
public abstract class AbsShredder implements IShredder<String, QName> {

  /** sirix {@link INodeWriteTrx}. */
  private final INodeWriteTrx mWtx;

  /** Keeps track of visited keys. */
  private final Deque<Long> mLeftSiblingKeyStack;

  /** Determines the import location of a new node. */
  private transient EInsert mInsertLocation;

  /**
   * Constructor.
   * 
   * @throws NullPointerException
   *           if {@code pWtx} is {@code null}
   */
  public AbsShredder(@Nonnull final INodeWriteTrx pWtx, @Nonnull final EInsert pInsertLocation) {
    mWtx = checkNotNull(pWtx);
    mInsertLocation = checkNotNull(pInsertLocation);
    mLeftSiblingKeyStack = new ArrayDeque<Long>();
    mLeftSiblingKeyStack.push(EFixed.NULL_NODE_KEY.getStandardProperty());
  }

  @Override
  public void processStartTag(@Nonnull final QName pName) throws AbsTTException {
    final QName name = checkNotNull(pName);
    long key;
    if (mInsertLocation == EInsert.ASRIGHTSIBLING) {
      if (mWtx.getNode().getKind() == EKind.DOCUMENT_ROOT
        || mWtx.getNode().getParentKey() == EFixed.ROOT_NODE_KEY.getStandardProperty()) {
        throw new IllegalStateException(
          "Subtree can not be inserted as sibling of document root or the root-element!");
      }
      key = mWtx.insertElementAsRightSibling(name).getNode().getNodeKey();
      mInsertLocation = EInsert.ASFIRSTCHILD;
    } else {
      if (mLeftSiblingKeyStack.peek() == EFixed.NULL_NODE_KEY.getStandardProperty()) {
        key = mWtx.insertElementAsFirstChild(name).getNode().getNodeKey();
      } else {
        key = mWtx.insertElementAsRightSibling(name).getNode().getNodeKey();
      }
    }

    mLeftSiblingKeyStack.pop();
    mLeftSiblingKeyStack.push(key);
    mLeftSiblingKeyStack.push(EFixed.NULL_NODE_KEY.getStandardProperty());
  }

  @Override
  public void processText(@Nonnull final String pText) throws AbsTTException {
    final String text = checkNotNull(pText);
    long key;
    if (!text.isEmpty()) {
      if (mLeftSiblingKeyStack.peek() == EFixed.NULL_NODE_KEY.getStandardProperty()) {
        key = mWtx.insertTextAsFirstChild(text).getNode().getNodeKey();
      } else {
        key = mWtx.insertTextAsRightSibling(text).getNode().getNodeKey();
      }

      mLeftSiblingKeyStack.pop();
      mLeftSiblingKeyStack.push(key);
    }
  }

  @Override
  public void processEndTag(@Nonnull final QName pName) {
    mLeftSiblingKeyStack.pop();
    mWtx.moveTo(mLeftSiblingKeyStack.peek());
  }

  @Override
  public void processEmptyElement(@Nonnull final QName pName) throws AbsTTException {
    processStartTag(pName);
    processEndTag(pName);
  }

  /**
   * Get the stack involved in processing.
   * 
   * @return the stack
   */
  public Deque<Long> getStack() {
    return mLeftSiblingKeyStack;
  }

  /**
   * Get key on top of the stack.
   * 
   * @return key on top of the stack
   */
  public long getTopKey() {
    return mLeftSiblingKeyStack.peek();
  }
}
