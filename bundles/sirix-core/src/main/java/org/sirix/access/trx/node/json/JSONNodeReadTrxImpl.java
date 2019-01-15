package org.sirix.access.trx.node.json;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import java.util.Optional;
import javax.annotation.Nonnegative;
import org.sirix.access.trx.node.AbstractNodeReadTrx;
import org.sirix.access.trx.node.Move;
import org.sirix.access.trx.node.xdm.XdmResourceManagerImpl;
import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.NodeWriteTrx;
import org.sirix.api.PageReadTrx;
import org.sirix.api.ResourceManager;
import org.sirix.exception.SirixIOException;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.ValueNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.node.json.JSONArrayNode;
import org.sirix.node.json.JSONNullNode;
import org.sirix.node.json.JSONNumberNode;
import org.sirix.node.json.JSONObjectKeyNode;
import org.sirix.node.json.JSONObjectNode;
import org.sirix.node.json.JSONStringNode;
import org.sirix.page.PageKind;
import org.sirix.settings.Constants;

public final class JSONNodeReadTrxImpl extends AbstractNodeReadTrx<JSONNodeReadOnlyTrx> implements JSONNodeReadOnlyTrx {

  /** ID of transaction. */
  private final long mTrxId;

  /** Resource manager this write transaction is bound to. */
  protected final XdmResourceManagerImpl mResourceManager;

  /** State of transaction including all cached stuff. */
  private PageReadTrx mPageReadTrx;

  /** Strong reference to currently selected node. */
  private ImmutableNode mCurrentNode;

  /** Tracks whether the transaction is closed. */
  private boolean mClosed;

  /**
   * Constructor.
   *
   * @param resourceManager the current {@link ResourceManager} the reader is bound to
   * @param trxId ID of the reader
   * @param pageReadTransaction {@link PageReadTrx} to interact with the page layer
   * @param documentNode the document node
   */
  // FIXME: ResourceManager.
  JSONNodeReadTrxImpl(final XdmResourceManagerImpl resourceManager, final @Nonnegative long trxId,
      final PageReadTrx pageReadTransaction, final Node documentNode) {
    super(trxId, pageReadTransaction);
    mResourceManager = checkNotNull(resourceManager);
    checkArgument(trxId >= 0);
    mTrxId = trxId;
    mPageReadTrx = checkNotNull(pageReadTransaction);
    mCurrentNode = checkNotNull(documentNode);
    mClosed = false;
  }

  @Override
  protected ImmutableNode getCurrentNode() {
    assertNotClosed();
    return mCurrentNode;
  }

  @Override
  public Move<JSONNodeReadOnlyTrx> moveTo(long nodeKey) {
    assertNotClosed();

    // Remember old node and fetch new one.
    final ImmutableNode oldNode = mCurrentNode;
    Optional<? extends Record> newNode;
    try {
      // Immediately return node from item list if node key negative.
      if (nodeKey < 0) {
        newNode = Optional.empty();
      } else {
        final Optional<? extends Record> node = mPageReadTrx.getRecord(nodeKey, PageKind.RECORDPAGE, -1);
        newNode = node;
      }
    } catch (final SirixIOException e) {
      newNode = Optional.empty();
    }

    if (newNode.isPresent()) {
      mCurrentNode = (Node) newNode.get();
      return Move.moved(this);
    } else {
      mCurrentNode = oldNode;
      return Move.notMoved();
    }
  }

  @Override
  public String getValue() {
    assertNotClosed();
    final String returnVal;
    if (mCurrentNode instanceof ValueNode) {
      returnVal = new String(((ValueNode) mCurrentNode).getRawValue(), Constants.DEFAULT_ENCODING);
    } else {
      returnVal = "";
    }
    return returnVal;
  }

  @Override
  protected void assertNotClosed() {
    if (mClosed) {
      throw new IllegalStateException("Transaction is already closed.");
    }
  }

  @Override
  public ResourceManager<? extends NodeReadTrx, ? extends NodeWriteTrx> getResourceManager() {
    assertNotClosed();
    return mResourceManager;
  }

  @Override
  public Move<? extends NodeCursor> moveToLeftSibling() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Move<? extends NodeCursor> moveToPrevious() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasLeftSibling() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLeftSiblingKey() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ImmutableNode getNode() {
    assertNotClosed();
    return mCurrentNode;
  }

  @Override
  public Kind getLeftSiblingKind() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isArray() {
    assertNotClosed();
    return mCurrentNode instanceof JSONArrayNode;
  }

  @Override
  public boolean isObject() {
    assertNotClosed();
    return mCurrentNode instanceof JSONObjectNode;
  }

  @Override
  public boolean isObjectKey() {
    assertNotClosed();
    return mCurrentNode instanceof JSONObjectKeyNode;
  }

  @Override
  public boolean isNumberValue() {
    assertNotClosed();
    return mCurrentNode instanceof JSONNumberNode;
  }

  @Override
  public boolean isNullValue() {
    assertNotClosed();
    return mCurrentNode instanceof JSONNullNode;
  }

  @Override
  public boolean isStringValue() {
    assertNotClosed();
    return mCurrentNode instanceof JSONStringNode;
  }

  @Override
  public void close() {
    if (!mClosed) {
      // Callback on session to make sure everything is cleaned up.
      mResourceManager.closeReadTransaction(mTrxId);

      // Close own state.
      mPageReadTrx.close();
      setPageReadTransaction(null);

      // Immediately release all references.
      mPageReadTrx = null;
      mCurrentNode = null;

      // Close state.
      mClosed = true;
    }
  }

  @Override
  protected JSONNodeReadOnlyTrx thisInstance() {
    return this;
  }
}
