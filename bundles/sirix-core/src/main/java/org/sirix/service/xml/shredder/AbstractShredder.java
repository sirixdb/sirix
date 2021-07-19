package org.sirix.service.xml.shredder;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.ArrayDeque;
import java.util.Deque;
import org.brackit.xquery.atomic.QNm;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.exception.SirixException;
import org.sirix.node.NodeKind;
import org.sirix.service.InsertPosition;
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
  private final XmlNodeTrx wtx;

  /** Keeps track of visited keys. */
  private final Deque<Long> parents;

  /** Determines the import location of a new node. */
  private InsertPosition insertLocation;

  /**
   * Constructor.
   *
   * @throws NullPointerException if {@code wtx} is {@code null} or {@code insertLocation} is
   *         {@code null}
   */
  public AbstractShredder(final XmlNodeTrx wtx, final InsertPosition insertLocation) {
    this.wtx = checkNotNull(wtx);
    this.insertLocation = checkNotNull(insertLocation);
    parents = new ArrayDeque<>();
    parents.push(Fixed.NULL_NODE_KEY.getStandardProperty());
  }

  @Override
  public void processComment(final String commentValue) throws SirixException {
    final String value = checkNotNull(commentValue);
    if (!value.isEmpty()) {
      final long key;

      if (parents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
        key = wtx.insertCommentAsFirstChild(value).getNodeKey();
      } else {
        key = wtx.insertCommentAsRightSibling(value).getNodeKey();
      }

      parents.pop();
      parents.push(key);
    }
  }

  @Override
  public void processPI(final String processingContent, final String processingTarget) throws SirixException {
    final String content = checkNotNull(processingContent);
    final String target = checkNotNull(processingTarget);

    if (!target.isEmpty()) {
      final long key;

      if (parents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
        key = wtx.insertPIAsFirstChild(target, content).getNodeKey();
      } else {
        key = wtx.insertPIAsRightSibling(target, content).getNodeKey();
      }

      parents.pop();
      parents.push(key);
    }
  }

  @Override
  public void processText(final String textValue) throws SirixException {
    final String text = checkNotNull(textValue);
    if (!text.isEmpty()) {
      final long key;

      if (parents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
        key = wtx.insertTextAsFirstChild(text).getNodeKey();
      } else {
        key = wtx.insertTextAsRightSibling(text).getNodeKey();
      }

      parents.pop();
      parents.push(key);
    }
  }

  @Override
  public void processStartTag(final QNm elementName) throws SirixException {
    final QNm name = checkNotNull(elementName);
    long key = -1;
    switch (insertLocation) {
      case AS_FIRST_CHILD:
        if (parents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertElementAsFirstChild(name).getNodeKey();
        } else {
          key = wtx.insertElementAsRightSibling(name).getNodeKey();
        }
        break;
      case AS_RIGHT_SIBLING:
        if (wtx.getKind() == NodeKind.XML_DOCUMENT
            || wtx.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
          throw new IllegalStateException(
              "Subtree can not be inserted as sibling of document root or the root-element!");
        }
        key = wtx.insertElementAsRightSibling(name).getNodeKey();
        insertLocation = InsertPosition.AS_FIRST_CHILD;
        break;
      case AS_LEFT_SIBLING:
        if (wtx.getKind() == NodeKind.XML_DOCUMENT
            || wtx.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
          throw new IllegalStateException(
              "Subtree can not be inserted as sibling of document root or the root-element!");
        }
        key = wtx.insertElementAsLeftSibling(name).getNodeKey();
        insertLocation = InsertPosition.AS_FIRST_CHILD;
        break;
      default:
        throw new AssertionError();// Must not happen.
    }

    parents.pop();
    parents.push(key);
    parents.push(Fixed.NULL_NODE_KEY.getStandardProperty());
  }

  @Override
  public void processEndTag(final QNm elementName) {
    parents.pop();
    wtx.moveTo(parents.peek());
  }

  @Override
  public void processEmptyElement(final QNm elementName) throws SirixException {
    processStartTag(elementName);
    processEndTag(elementName);
  }
}
