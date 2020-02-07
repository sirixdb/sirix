package org.sirix.xquery.node;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.node.parser.SubtreeHandler;
import org.brackit.xquery.node.parser.SubtreeListener;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.node.AbstractTemporalNode;
import org.sirix.access.trx.node.xml.InternalXmlNodeTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.exception.SirixException;
import org.sirix.service.xml.shredder.AbstractShredder;
import org.sirix.service.xml.shredder.InsertPosition;

/**
 * Subtree builder to build a new tree.
 *
 * @author Johannes Lichtenberger
 */
public final class SubtreeBuilder extends AbstractShredder implements SubtreeHandler {

  /** {@link SubtreeProcessor} for listeners. */
  private final SubtreeProcessor<AbstractTemporalNode<XmlDBNode>> mSubtreeProcessor;

  /** Sirix {@link XmlNodeTrx}. */
  private final XmlNodeTrx mWtx;

  /** Stack for saving the parent nodes. */
  private final Deque<XmlDBNode> mParents;

  /** Collection. */
  private final XmlDBCollection mCollection;

  /** First element. */
  private boolean mFirst;

  /** Start node key. */
  private long mStartNodeKey;

  /** Stack of namespace mappings. */
  private final Deque<QNm> mNamespaces;

  /** Stack of namespace mappings. */
  private final Deque<String> mInsertedNamespacePrefixes;

  /**
   * Constructor.
   *
   * @param collection the database collection
   * @param wtx the read/write transaction
   * @param insertPos determines how to insert (as a right sibling, first child or left sibling)
   * @param listeners listeners which implement
   */
  public SubtreeBuilder(final XmlDBCollection collection, final XmlNodeTrx wtx, final InsertPosition insertPos,
      final List<SubtreeListener<? super AbstractTemporalNode<XmlDBNode>>> listeners) {
    super(wtx, insertPos);
    ((InternalXmlNodeTrx) wtx).setBulkInsertion(true);
    mCollection = checkNotNull(collection);
    mSubtreeProcessor = new SubtreeProcessor<>(checkNotNull(listeners));
    mWtx = checkNotNull(wtx);
    mParents = new ArrayDeque<>();
    mFirst = true;
    mNamespaces = new ArrayDeque<>();
    mInsertedNamespacePrefixes = new ArrayDeque<>();
  }

  /**
   * Get start node key.
   *
   * @return start node key
   */
  public long getStartNodeKey() {
    return mStartNodeKey;
  }

  @Override
  public void begin() throws DocumentException {
    try {
      mSubtreeProcessor.notifyBegin();
    } catch (final DocumentException e) {
      mSubtreeProcessor.notifyFail();
      throw e;
    }
  }

  @Override
  public void end() throws DocumentException {
    try {
      mSubtreeProcessor.notifyEnd();
    } catch (final DocumentException e) {
      mSubtreeProcessor.notifyFail();
      throw e;
    }
  }

  @Override
  public void beginFragment() throws DocumentException {
    try {
      mSubtreeProcessor.notifyBeginFragment();
    } catch (final DocumentException e) {
      mSubtreeProcessor.notifyFail();
      throw e;
    }
  }

  @Override
  public void endFragment() throws DocumentException {
    try {
      mSubtreeProcessor.notifyEndFragment();
    } catch (final DocumentException e) {
      mSubtreeProcessor.notifyFail();
      throw e;
    }
  }

  @Override
  public void startDocument() throws DocumentException {
    try {
      mSubtreeProcessor.notifyBeginDocument();
    } catch (final DocumentException e) {
      mSubtreeProcessor.notifyFail();
      throw e;
    }
  }

  @Override
  public void endDocument() throws DocumentException {
    try {
      mSubtreeProcessor.notifyEndDocument();
      ((InternalXmlNodeTrx) mWtx).adaptHashesInPostorderTraversal();
      ((InternalXmlNodeTrx) mWtx).setBulkInsertion(false);
    } catch (final DocumentException e) {
      mSubtreeProcessor.notifyFail();
      throw e;
    }
  }

  @Override
  public void fail() throws DocumentException {
    mSubtreeProcessor.notifyFail();
  }

  @Override
  public void startMapping(final String prefix, final String uri) throws DocumentException {
    mNamespaces.push(new QNm(uri, prefix, null));
  }

  @Override
  public void endMapping(final String prefix) throws DocumentException {
    mInsertedNamespacePrefixes.pop();
  }

  @Override
  public void comment(final Atomic content) throws DocumentException {
    try {
      processComment(content.asStr().stringValue());
      if (mFirst) {
        mFirst = false;
        mStartNodeKey = mWtx.getNodeKey();
      }
      mSubtreeProcessor.notifyComment(new XmlDBNode(mWtx, mCollection));
    } catch (final SirixException e) {
      throw new DocumentException(e.getCause());
    }
  }

  @Override
  public void processingInstruction(final QNm target, final Atomic content) throws DocumentException {
    try {
      processPI(content.asStr().stringValue(), target.getLocalName());
      mSubtreeProcessor.notifyProcessingInstruction(new XmlDBNode(mWtx, mCollection));
    } catch (final SirixException e) {
      throw new DocumentException(e.getCause());
    }
  }

  @Override
  public void startElement(final QNm name) throws DocumentException {
    try {
      processStartTag(name);
      while (!mNamespaces.isEmpty()) {
        final QNm namespace = mNamespaces.pop();

        if (!mInsertedNamespacePrefixes.contains(namespace.getPrefix())) {
          mWtx.insertNamespace(namespace).moveToParent();
        }

        mInsertedNamespacePrefixes.push(namespace.getPrefix());
      }
      if (mFirst) {
        mFirst = false;
        mStartNodeKey = mWtx.getNodeKey();
      }
      final XmlDBNode node = new XmlDBNode(mWtx, mCollection);
      mParents.push(node);
      mSubtreeProcessor.notifyStartElement(node);
    } catch (final SirixException e) {
      throw new DocumentException(e.getCause());
    }
  }

  @Override
  public void endElement(final QNm name) throws DocumentException {
    processEndTag(name);
    final XmlDBNode node = mParents.pop();
    mSubtreeProcessor.notifyEndElement(node);
  }

  @Override
  public void text(final Atomic content) throws DocumentException {
    try {
      processText(content.stringValue());
      mSubtreeProcessor.notifyText(new XmlDBNode(mWtx, mCollection));
    } catch (final SirixException e) {
      throw new DocumentException(e.getCause());
    }
  }

  @Override
  public void attribute(final QNm name, final Atomic value) throws DocumentException {
    try {
      mWtx.insertAttribute(name, value.stringValue());
      mWtx.moveToParent();
      mSubtreeProcessor.notifyAttribute(new XmlDBNode(mWtx, mCollection));
    } catch (final SirixException e) {
      throw new DocumentException(e.getCause());
    }
  }

}
