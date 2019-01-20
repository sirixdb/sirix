package org.sirix.access.trx.node.json;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import java.util.Optional;
import javax.annotation.Nonnegative;
import org.brackit.xquery.atomic.QNm;
import org.sirix.access.trx.node.AbstractNodeReadTrx;
import org.sirix.access.trx.node.Move;
import org.sirix.api.PageReadTrx;
import org.sirix.api.ResourceManager;
import org.sirix.exception.SirixIOException;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.ValueNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.node.interfaces.immutable.ImmutableStructNode;
import org.sirix.node.json.JsonArrayNode;
import org.sirix.node.json.JsonNullNode;
import org.sirix.node.json.JsonNumberNode;
import org.sirix.node.json.JsonObjectKeyNode;
import org.sirix.node.json.JsonObjectNode;
import org.sirix.node.json.JsonStringNode;
import org.sirix.page.PageKind;
import org.sirix.settings.Constants;

public final class JsonNodeReadOnlyTrxImpl extends AbstractNodeReadTrx<JsonNodeReadOnlyTrx>
    implements JsonNodeReadOnlyTrx {

  /** ID of transaction. */
  private final long mTrxId;

  /** Resource manager this write transaction is bound to. */
  protected final JsonResourceManagerImpl mResourceManager;

  /** State of transaction including all cached stuff. */
  private PageReadTrx mPageReadTrx;

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
  JsonNodeReadOnlyTrxImpl(final JsonResourceManagerImpl resourceManager, final @Nonnegative long trxId,
      final PageReadTrx pageReadTransaction, final Node documentNode) {
    super(trxId, pageReadTransaction, documentNode);
    mResourceManager = checkNotNull(resourceManager);
    checkArgument(trxId >= 0);
    mTrxId = trxId;
    mPageReadTrx = checkNotNull(pageReadTransaction);
    mClosed = false;
  }

  @Override
  public int keyForName(String name) {
    assertNotClosed();
    return 0;
  }

  @Override
  public Move<JsonNodeReadOnlyTrx> moveTo(long nodeKey) {
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
  public JsonResourceManager getResourceManager() {
    assertNotClosed();
    return mResourceManager;
  }

  @Override
  public Move<? extends JsonNodeReadOnlyTrx> moveToLeftSibling() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Move<? extends JsonNodeReadOnlyTrx> moveToPrevious() {
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
    return mCurrentNode instanceof JsonArrayNode;
  }

  @Override
  public boolean isObject() {
    assertNotClosed();
    return mCurrentNode instanceof JsonObjectNode;
  }

  @Override
  public boolean isObjectKey() {
    assertNotClosed();
    return mCurrentNode instanceof JsonObjectKeyNode;
  }

  @Override
  public boolean isNumberValue() {
    assertNotClosed();
    return mCurrentNode instanceof JsonNumberNode;
  }

  @Override
  public boolean isNullValue() {
    assertNotClosed();
    return mCurrentNode instanceof JsonNullNode;
  }

  @Override
  public boolean isStringValue() {
    assertNotClosed();
    return mCurrentNode instanceof JsonStringNode;
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
  protected JsonNodeReadOnlyTrx thisInstance() {
    return this;
  }

  public void setCurrentNode(ImmutableNode node) {
    assertNotClosed();
    assert node != null : "Node must be given.";
    mCurrentNode = node;
  }

  @Override
  public long getDescendantCount() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long getChildCount() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Kind getPathKind() {
    return null;
  }

  @Override
  public boolean isDocumentRoot() {
    assertNotClosed();
    return mCurrentNode.getKind() == Kind.DOCUMENT;
  }

  @Override
  public boolean isClosed() {
    return mClosed;
  }

  @Override
  public QNm getName() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean hasChildren() {
    // TODO Auto-generated method stub
    return false;
  }

  public ImmutableStructNode getCurrentNode() {
    return (ImmutableStructNode) mCurrentNode;
  }
}
