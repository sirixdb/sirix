package org.sirix.xquery.node;

import java.util.ArrayList;
import java.util.List;
import org.brackit.xquery.node.parser.SubtreeListener;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.node.Node;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

/**
 * SubtreeProcessor which notifies a list of listeners.
 * 
 * @author Johannes Lichtenberger
 * 
 * @param <E> the node implementation
 */
public class SubtreeProcessor<E extends Node<E>> {
  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER =
      new LogWrapper(LoggerFactory.getLogger(SubtreeProcessor.class));

  /** Observers. */
  private final List<SubtreeListener<? super E>> mListeners;

  /**
   * Constructor.
   * 
   * @param listeners list of listeners/observers
   */
  public SubtreeProcessor(final List<SubtreeListener<? super E>> listeners) {
    mListeners = new ArrayList<SubtreeListener<? super E>>();
    if (listeners != null) {
      for (final SubtreeListener<? super E> listener : listeners) {
        mListeners.add(listener);
      }
    }
  }

  /**
   * Notify the beginning.
   * 
   * @throws DocumentException if anything went wrong
   */
  public void notifyBegin() throws DocumentException {
    for (SubtreeListener<? super E> listener : mListeners) {
      listener.begin();
    }
  }

  /**
   * Notify the end.
   * 
   * @throws DocumentException if anything went wrong
   */
  public void notifyEnd() throws DocumentException {
    for (SubtreeListener<? super E> listener : mListeners) {
      listener.end();
    }
  }

  /**
   * Notify the beginning of a document.
   * 
   * @throws DocumentException if anything went wrong
   */
  public void notifyBeginDocument() throws DocumentException {
    for (SubtreeListener<? super E> listener : mListeners) {
      listener.startDocument();
    }
  }

  /**
   * Notify the end of a document.
   * 
   * @throws DocumentException if anything went wrong
   */
  public void notifyEndDocument() throws DocumentException {
    for (SubtreeListener<? super E> listener : mListeners) {
      listener.endDocument();
    }
  }

  /**
   * Notify the start of a fragment.
   * 
   * @throws DocumentException if anything went wrong
   */
  public void notifyBeginFragment() throws DocumentException {
    for (SubtreeListener<? super E> listener : mListeners) {
      listener.beginFragment();
    }
  }

  /**
   * Notify the end of a fragment.
   * 
   * @throws DocumentException if anything went wrong
   */
  public void notifyEndFragment() throws DocumentException {
    for (SubtreeListener<? super E> listener : mListeners) {
      listener.endFragment();
    }
  }

  /**
   * Notify a failure.
   */
  public void notifyFail() {
    for (final SubtreeListener<? super E> listener : mListeners) {
      try {
        listener.fail();
      } catch (final DocumentException e) {
        LOGWRAPPER.error(e.getMessage(), e);
      }
    }
  }

  /**
   * Notify a start tag.
   * 
   * @param node element node
   * @throws DocumentException if anything went wrong
   */
  public void notifyStartElement(final E node) throws DocumentException {
    for (SubtreeListener<? super E> listener : mListeners) {
      listener.startElement(node);
    }
  }

  /**
   * Notify an end tag.
   * 
   * @param node element node
   * @throws DocumentException if anything went wrong
   */
  public void notifyEndElement(final E node) throws DocumentException {
    for (SubtreeListener<? super E> listener : mListeners) {
      listener.endElement(node);
    }
  }

  /**
   * Notify an attribute node.
   * 
   * @param node attribute node
   * @throws DocumentException if anything went wrong
   */
  public void notifyAttribute(final E node) throws DocumentException {
    for (SubtreeListener<? super E> listener : mListeners) {
      listener.attribute(node);
    }
  }

  /**
   * Notify a text node.
   * 
   * @param node text node
   * @throws DocumentException if anything went wrong
   */
  public void notifyText(final E node) throws DocumentException {
    for (SubtreeListener<? super E> listener : mListeners) {
      listener.text(node);
    }
  }

  /**
   * Notify a comment node.
   * 
   * @param node comment node
   * @throws DocumentException if anything went wrong
   */
  public void notifyComment(final E node) throws DocumentException {
    for (SubtreeListener<? super E> listener : mListeners) {
      listener.comment(node);
    }
  }

  /**
   * Notify a processing instruction node.
   * 
   * @param node the processing instruction
   * @throws DocumentException if anything went wrong
   */
  public void notifyProcessingInstruction(final E node) throws DocumentException {
    for (SubtreeListener<? super E> listener : mListeners) {
      listener.processingInstruction(node);
    }
  }
}
