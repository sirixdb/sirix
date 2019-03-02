package org.sirix.service.xml.shredder;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.ArrayDeque;
import java.util.Deque;
import org.brackit.xquery.atomic.QNm;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.exception.SirixException;
import org.sirix.node.Kind;
import org.sirix.settings.Fixed;

/**
 * Skeleton implementation of {@link Shredder} interface methods.
 *
 * All methods throw {@link NullPointerException}s in case of {@code null} values for reference
 * parameters and check the arguments, whereas in case they are not valid a
 * {@link IllegalArgumentException} is thrown.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 * @author Marc Kramis, Seabix GmbH
 *
 */
public abstract class AbstractShredder implements Shredder<String, QNm> {

  /** Sirix {@link XmlNodeTrx}. */
  private final XmlNodeTrx mWtx;

  /** Keeps track of visited keys. */
  private final Deque<Long> mParents;

  /** Determines the import location of a new node. */
  private InsertPosition mInsertLocation;

  /**
   * Constructor.
   *
   * @throws NullPointerException if {@code wtx} is {@code null} or {@code insertLocation} is
   *         {@code null}
   */
  public AbstractShredder(final XmlNodeTrx wtx, final InsertPosition insertLocation) {
    mWtx = checkNotNull(wtx);
    mInsertLocation = checkNotNull(insertLocation);
    mParents = new ArrayDeque<>();
    mParents.push(Fixed.NULL_NODE_KEY.getStandardProperty());
  }

  @Override
  public void processComment(final String commentValue) throws SirixException {
    final String value = checkNotNull(commentValue);
    if (!value.isEmpty()) {
      final long key;

      if (mParents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
        key = mWtx.insertCommentAsFirstChild(value).getNodeKey();
      } else {
        key = mWtx.insertCommentAsRightSibling(value).getNodeKey();
      }

      mParents.pop();
      mParents.push(key);
    }
  }

  @Override
  public void processPI(final String processingContent, final String processingTarget) throws SirixException {
    final String content = checkNotNull(processingContent);
    final String target = checkNotNull(processingTarget);

    if (!target.isEmpty()) {
      final long key;

      if (mParents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
        key = mWtx.insertPIAsFirstChild(target, content).getNodeKey();
      } else {
        key = mWtx.insertPIAsRightSibling(target, content).getNodeKey();
      }

      mParents.pop();
      mParents.push(key);
    }
  }

  @Override
  public void processText(final String textValue) throws SirixException {
    final String text = checkNotNull(textValue);
    if (!text.isEmpty()) {
      final long key;

      if (mParents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
        key = mWtx.insertTextAsFirstChild(text).getNodeKey();
      } else {
        key = mWtx.insertTextAsRightSibling(text).getNodeKey();
      }

      mParents.pop();
      mParents.push(key);
    }
  }

  @Override
  public void processStartTag(final QNm elementName) throws SirixException {
    final QNm name = checkNotNull(elementName);
    long key = -1;
    switch (mInsertLocation) {
      case AS_FIRST_CHILD:
        if (mParents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = mWtx.insertElementAsFirstChild(name).getNodeKey();
        } else {
          key = mWtx.insertElementAsRightSibling(name).getNodeKey();
        }
        break;
      case AS_RIGHT_SIBLING:
        if (mWtx.getKind() == Kind.XDM_DOCUMENT
            || mWtx.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
          throw new IllegalStateException(
              "Subtree can not be inserted as sibling of document root or the root-element!");
        }
        key = mWtx.insertElementAsRightSibling(name).getNodeKey();
        mInsertLocation = InsertPosition.AS_FIRST_CHILD;
        break;
      case AS_LEFT_SIBLING:
        if (mWtx.getKind() == Kind.XDM_DOCUMENT
            || mWtx.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
          throw new IllegalStateException(
              "Subtree can not be inserted as sibling of document root or the root-element!");
        }
        key = mWtx.insertElementAsLeftSibling(name).getNodeKey();
        mInsertLocation = InsertPosition.AS_FIRST_CHILD;
        break;
      default:
        throw new AssertionError();// Must not happen.
    }

    mParents.pop();
    mParents.push(key);
    mParents.push(Fixed.NULL_NODE_KEY.getStandardProperty());
  }

  @Override
  public void processEndTag(final QNm elementName) {
    mParents.pop();
    mWtx.moveTo(mParents.peek());
  }

  @Override
  public void processEmptyElement(final QNm elementName) throws SirixException {
    processStartTag(elementName);
    processEndTag(elementName);
  }
}
