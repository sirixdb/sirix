package org.sirix.xquery.node;

import org.brackit.xquery.jdm.DocumentException;
import org.brackit.xquery.jdm.node.Node;
import org.brackit.xquery.node.parser.NodeSubtreeListener;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

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
  private final List<NodeSubtreeListener<? super E>> listeners;

  /**
   * Constructor.
   * 
   * @param listeners list of listeners/observers
   */
  public SubtreeProcessor(final List<NodeSubtreeListener<? super E>> listeners) {
    this.listeners = new ArrayList<>();
    if (listeners != null) {
      this.listeners.addAll(listeners);
    }
  }

  /**
   * Notify the beginning.
   * 
   * @throws DocumentException if anything went wrong
   */
  public void notifyBegin() throws DocumentException {
    for (NodeSubtreeListener<? super E> listener : listeners) {
      listener.begin();
    }
  }

  /**
   * Notify the end.
   * 
   * @throws DocumentException if anything went wrong
   */
  public void notifyEnd() throws DocumentException {
    for (NodeSubtreeListener<? super E> listener : listeners) {
      listener.end();
    }
  }

  /**
   * Notify the beginning of a document.
   * 
   * @throws DocumentException if anything went wrong
   */
  public void notifyBeginDocument() throws DocumentException {
    for (NodeSubtreeListener<? super E> listener : listeners) {
      listener.startDocument();
    }
  }

  /**
   * Notify the end of a document.
   * 
   * @throws DocumentException if anything went wrong
   */
  public void notifyEndDocument() throws DocumentException {
    for (NodeSubtreeListener<? super E> listener : listeners) {
      listener.endDocument();
    }
  }

  /**
   * Notify the start of a fragment.
   * 
   * @throws DocumentException if anything went wrong
   */
  public void notifyBeginFragment() throws DocumentException {
    for (NodeSubtreeListener<? super E> listener : listeners) {
      listener.beginFragment();
    }
  }

  /**
   * Notify the end of a fragment.
   * 
   * @throws DocumentException if anything went wrong
   */
  public void notifyEndFragment() throws DocumentException {
    for (NodeSubtreeListener<? super E> listener : listeners) {
      listener.endFragment();
    }
  }

  /**
   * Notify a failure.
   */
  public void notifyFail() {
    for (final NodeSubtreeListener<? super E> listener : listeners) {
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
    for (NodeSubtreeListener<? super E> listener : listeners) {
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
    for (NodeSubtreeListener<? super E> listener : listeners) {
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
    for (NodeSubtreeListener<? super E> listener : listeners) {
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
    for (NodeSubtreeListener<? super E> listener : listeners) {
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
    for (NodeSubtreeListener<? super E> listener : listeners) {
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
    for (NodeSubtreeListener<? super E> listener : listeners) {
      listener.processingInstruction(node);
    }
  }
}
