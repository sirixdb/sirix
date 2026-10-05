package io.sirix.query.node;

import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.DocumentException;
import io.brackit.query.jdm.node.AbstractTemporalNode;
import io.brackit.query.node.parser.NodeSubtreeHandler;
import io.brackit.query.node.parser.NodeSubtreeListener;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.exception.SirixException;
import io.sirix.service.InsertPosition;
import io.sirix.service.xml.shredder.AbstractShredder;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import static java.util.Objects.requireNonNull;

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

  /**
   * Mappings pending for the next element. Local declarations must be retained even when an ancestor
   * declares the same prefix, since the namespace URI may differ or be undeclared.
   */
  private final Deque<QNm> namespaces;

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
    // ((InternalXmlNodeTrx) wtx).setBulkInsertion(true);
    this.collection = requireNonNull(collection);
    subtreeProcessor = new SubtreeProcessor<>(requireNonNull(listeners));
    this.wtx = requireNonNull(wtx);
    parents = new ArrayDeque<>();
    first = true;
    namespaces = new ArrayDeque<>();
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
      // ((InternalXmlNodeTrx) wtx).adaptHashesInPostorderTraversal();
      // ((InternalXmlNodeTrx) wtx).setBulkInsertion(false);
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
    // startElement consumes pending mappings; the stored element retains their namespace scope.
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
      // Ancestor bindings must not suppress local prefix rebindings or default namespace resets.
      while (!namespaces.isEmpty()) {
        wtx.insertNamespace(namespaces.pop()).moveToParent();
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
