package org.sirix.xquery.node;

import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.jdm.DocumentException;
import org.brackit.xquery.jdm.node.AbstractTemporalNode;
import org.brackit.xquery.node.parser.NodeSubtreeHandler;
import org.brackit.xquery.node.parser.NodeSubtreeListener;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.exception.SirixException;
import org.sirix.service.InsertPosition;
import org.sirix.service.xml.shredder.AbstractShredder;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Subtree builder to build a new tree.
 *
 * @author Johannes Lichtenberger
 */
public final class SubtreeBuilder extends AbstractShredder implements NodeSubtreeHandler {

  /** {@link SubtreeProcessor} for listeners. */
  private final SubtreeProcessor<AbstractTemporalNode<XmlDBNode>> subtreeProcessor;

  /** Sirix {@link XmlNodeTrx}. */
  private final XmlNodeTrx wtx;

  /** Stack for saving the parent nodes. */
  private final Deque<XmlDBNode> parents;

  /** Collection. */
  private final XmlDBCollection collection;

  /** First element. */
  private boolean first;

  /** Start node key. */
  private long startNodeKey;

  /** Stack of namespace mappings. */
  private final Deque<QNm> namespaces;

  /** Stack of namespace mappings. */
  private final Deque<String> insertedNamespacePrefixes;

  /**
   * Constructor.
   *
   * @param collection the database collection
   * @param wtx the read/write transaction
   * @param insertPos determines how to insert (as a right sibling, first child or left sibling)
   * @param listeners listeners which implement
   */
  public SubtreeBuilder(final XmlDBCollection collection, final XmlNodeTrx wtx, final InsertPosition insertPos,
      final List<NodeSubtreeListener<? super AbstractTemporalNode<XmlDBNode>>> listeners) {
    super(wtx, insertPos);
    //((InternalXmlNodeTrx) wtx).setBulkInsertion(true);
    this.collection = checkNotNull(collection);
    subtreeProcessor = new SubtreeProcessor<>(checkNotNull(listeners));
    this.wtx = checkNotNull(wtx);
    parents = new ArrayDeque<>();
    first = true;
    namespaces = new ArrayDeque<>();
    insertedNamespacePrefixes = new ArrayDeque<>();
  }

  /**
   * Get start node key.
   *
   * @return start node key
   */
  public long getStartNodeKey() {
    return startNodeKey;
  }

  @Override
  public void begin() throws DocumentException {
    try {
      subtreeProcessor.notifyBegin();
    } catch (final DocumentException e) {
      subtreeProcessor.notifyFail();
      throw e;
    }
  }

  @Override
  public void end() throws DocumentException {
    try {
      subtreeProcessor.notifyEnd();
    } catch (final DocumentException e) {
      subtreeProcessor.notifyFail();
      throw e;
    }
  }

  @Override
  public void beginFragment() throws DocumentException {
    try {
      subtreeProcessor.notifyBeginFragment();
    } catch (final DocumentException e) {
      subtreeProcessor.notifyFail();
      throw e;
    }
  }

  @Override
  public void endFragment() throws DocumentException {
    try {
      subtreeProcessor.notifyEndFragment();
    } catch (final DocumentException e) {
      subtreeProcessor.notifyFail();
      throw e;
    }
  }

  @Override
  public void startDocument() throws DocumentException {
    try {
      subtreeProcessor.notifyBeginDocument();
    } catch (final DocumentException e) {
      subtreeProcessor.notifyFail();
      throw e;
    }
  }

  @Override
  public void endDocument() throws DocumentException {
    try {
      subtreeProcessor.notifyEndDocument();
//      ((InternalXmlNodeTrx) wtx).adaptHashesInPostorderTraversal();
//      ((InternalXmlNodeTrx) wtx).setBulkInsertion(false);
    } catch (final DocumentException e) {
      subtreeProcessor.notifyFail();
      throw e;
    }
  }

  @Override
  public void fail() throws DocumentException {
    subtreeProcessor.notifyFail();
  }

  @Override
  public void startMapping(final String prefix, final String uri) throws DocumentException {
    namespaces.push(new QNm(uri, prefix, null));
  }

  @Override
  public void endMapping(final String prefix) throws DocumentException {
    insertedNamespacePrefixes.pop();
  }

  @Override
  public void comment(final Atomic content) throws DocumentException {
    try {
      processComment(content.asStr().stringValue());
      if (first) {
        first = false;
        startNodeKey = wtx.getNodeKey();
      }
      subtreeProcessor.notifyComment(new XmlDBNode(wtx, collection));
    } catch (final SirixException e) {
      throw new DocumentException(e.getCause());
    }
  }

  @Override
  public void processingInstruction(final QNm target, final Atomic content) throws DocumentException {
    try {
      processPI(content.asStr().stringValue(), target.getLocalName());
      subtreeProcessor.notifyProcessingInstruction(new XmlDBNode(wtx, collection));
    } catch (final SirixException e) {
      throw new DocumentException(e.getCause());
    }
  }

  @Override
  public void startElement(final QNm name) throws DocumentException {
    try {
      processStartTag(name);
      while (!namespaces.isEmpty()) {
        final QNm namespace = namespaces.pop();

        if (!insertedNamespacePrefixes.contains(namespace.getPrefix())) {
          wtx.insertNamespace(namespace).moveToParent();
        }

        insertedNamespacePrefixes.push(namespace.getPrefix());
      }
      if (first) {
        first = false;
        startNodeKey = wtx.getNodeKey();
      }
      final XmlDBNode node = new XmlDBNode(wtx, collection);
      parents.push(node);
      subtreeProcessor.notifyStartElement(node);
    } catch (final SirixException e) {
      throw new DocumentException(e.getCause());
    }
  }

  @Override
  public void endElement(final QNm name) throws DocumentException {
    processEndTag(name);
    final XmlDBNode node = parents.pop();
    subtreeProcessor.notifyEndElement(node);
  }

  @Override
  public void text(final Atomic content) throws DocumentException {
    try {
      processText(content.stringValue());
      subtreeProcessor.notifyText(new XmlDBNode(wtx, collection));
    } catch (final SirixException e) {
      throw new DocumentException(e.getCause());
    }
  }

  @Override
  public void attribute(final QNm name, final Atomic value) throws DocumentException {
    try {
      wtx.insertAttribute(name, value.stringValue());
      wtx.moveToParent();
      subtreeProcessor.notifyAttribute(new XmlDBNode(wtx, collection));
    } catch (final SirixException e) {
      throw new DocumentException(e.getCause());
    }
  }

}
