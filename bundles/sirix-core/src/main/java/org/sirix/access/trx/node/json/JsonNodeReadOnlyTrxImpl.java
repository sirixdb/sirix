package org.sirix.access.trx.node.json;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import java.util.Optional;
import javax.annotation.Nonnegative;
import org.brackit.xquery.atomic.QNm;
import org.sirix.access.trx.node.AbstractNodeReadTrx;
import org.sirix.access.trx.node.InternalResourceManager;
import org.sirix.api.Move;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.ResourceManager;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.exception.SirixIOException;
import org.sirix.node.NodeKind;
import org.sirix.node.immutable.json.ImmutableObjectKeyNode;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.ValueNode;
import org.sirix.node.interfaces.immutable.ImmutableJsonNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.node.json.ArrayNode;
import org.sirix.node.json.BooleanNode;
import org.sirix.node.json.NullNode;
import org.sirix.node.json.NumberNode;
import org.sirix.node.json.ObjectKeyNode;
import org.sirix.node.json.ObjectNode;
import org.sirix.node.json.StringNode;
import org.sirix.page.PageKind;
import org.sirix.settings.Constants;
import com.google.common.base.MoreObjects;

public final class JsonNodeReadOnlyTrxImpl extends AbstractNodeReadTrx<JsonNodeReadOnlyTrx>
    implements InternalJsonNodeReadOnlyTrx {

  /** ID of transaction. */
  private final long mTrxId;

  /** Resource manager this write transaction is bound to. */
  protected final InternalResourceManager<JsonNodeReadOnlyTrx, JsonNodeTrx> mResourceManager;

  /** Tracks whether the transaction is closed. */
  private boolean mIsClosed;

  /**
   * Constructor.
   *
   * @param resourceManager the current {@link ResourceManager} the reader is bound to
   * @param trxId ID of the reader
   * @param pageReadTransaction {@link PageReadOnlyTrx} to interact with the page layer
   * @param documentNode the document node
   */
  JsonNodeReadOnlyTrxImpl(final InternalResourceManager<JsonNodeReadOnlyTrx, JsonNodeTrx> resourceManager,
      final @Nonnegative long trxId, final PageReadOnlyTrx pageReadTransaction, final ImmutableJsonNode documentNode) {
    super(trxId, pageReadTransaction, documentNode);
    mResourceManager = checkNotNull(resourceManager);
    checkArgument(trxId >= 0);
    mTrxId = trxId;
    mIsClosed = false;
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
    switch (mCurrentNode.getKind()) {
      case STRING_VALUE:
        returnVal = new String(((ValueNode) mCurrentNode).getRawValue(), Constants.DEFAULT_ENCODING);
        break;
      case BOOLEAN_VALUE:
        returnVal = String.valueOf(((BooleanNode) mCurrentNode).getValue());
        break;
      case NULL_VALUE:
        returnVal = "null";
        break;
      case NUMBER_VALUE:
        returnVal = String.valueOf(((NumberNode) mCurrentNode).getValue());
        break;
      // $CASES-OMITTED$
      default:
        returnVal = "";
        break;
    }
    return returnVal;
  }

  @Override
  public boolean getBooleanValue() {
    assertNotClosed();
    if (mCurrentNode.getKind() == NodeKind.BOOLEAN_VALUE)
      return ((BooleanNode) mCurrentNode).getValue();
    throw new IllegalStateException("Current node is no boolean node.");
  }

  @Override
  public Number getNumberValue() {
    assertNotClosed();
    if (mCurrentNode.getKind() == NodeKind.NUMBER_VALUE)
      return ((NumberNode) mCurrentNode).getValue();
    throw new IllegalStateException("Current node is no number node.");
  }

  @Override
  public void assertNotClosed() {
    if (mIsClosed) {
      throw new IllegalStateException("Transaction is already closed.");
    }
  }

  @Override
  public JsonResourceManager getResourceManager() {
    assertNotClosed();
    return (JsonResourceManager) mResourceManager;
  }

  @Override
  public ImmutableNode getNode() {
    assertNotClosed();
    return mCurrentNode;
  }

  @Override
  public boolean isArray() {
    assertNotClosed();
    return mCurrentNode instanceof ArrayNode;
  }

  @Override
  public boolean isObject() {
    assertNotClosed();
    return mCurrentNode instanceof ObjectNode;
  }

  @Override
  public boolean isObjectKey() {
    assertNotClosed();
    return mCurrentNode instanceof ObjectKeyNode;
  }

  @Override
  public boolean isNumberValue() {
    assertNotClosed();
    return mCurrentNode instanceof NumberNode;
  }

  @Override
  public boolean isNullValue() {
    assertNotClosed();
    return mCurrentNode instanceof NullNode;
  }

  @Override
  public boolean isStringValue() {
    assertNotClosed();
    return mCurrentNode instanceof StringNode;
  }

  @Override
  public boolean isBooleanValue() {
    assertNotClosed();
    return mCurrentNode instanceof BooleanNode;
  }

  @Override
  public void close() {
    if (!mIsClosed) {
      // Close own state.
      mPageReadTrx.close();

      // Callback on session to make sure everything is cleaned up.
      mResourceManager.closeReadTransaction(mTrxId);

      setPageReadTransaction(null);

      // Immediately release all references.
      mPageReadTrx = null;
      mCurrentNode = null;

      // Close state.
      mIsClosed = true;
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
  public boolean isDocumentRoot() {
    assertNotClosed();
    return mCurrentNode.getKind() == NodeKind.JSON_DOCUMENT;
  }

  @Override
  public boolean isClosed() {
    return mIsClosed;
  }

  @Override
  public QNm getName() {
    assertNotClosed();

    if (mCurrentNode.getKind() == NodeKind.OBJECT_KEY) {
      final int nameKey = ((ObjectKeyNode) mCurrentNode).getNameKey();
      final String localName = nameKey == -1
          ? ""
          : mPageReadTrx.getName(nameKey, mCurrentNode.getKind());
      return new QNm(localName);
    }

    return null;
  }

  @Override
  public VisitResult acceptVisitor(JsonNodeVisitor visitor) {
    assertNotClosed();
    return ((ImmutableJsonNode) mCurrentNode).acceptVisitor(visitor);
  }

  @Override
  public ImmutableJsonNode getCurrentNode() {
    return (ImmutableJsonNode) mCurrentNode;
  }

  @Override
  public void setCurrentNode(ImmutableJsonNode node) {
    assertNotClosed();
    mCurrentNode = node;
  }

  @Override
  public int getNameKey() {
    assertNotClosed();
    if (mCurrentNode.getKind() == NodeKind.OBJECT_KEY) {
      return ((ImmutableObjectKeyNode) mCurrentNode).getNameKey();
    }
    return -1;
  }

  @Override
  public String toString() {
    final MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
    helper.add("Revision number", getRevisionNumber());

    if (mCurrentNode.getKind() == NodeKind.OBJECT_KEY) {
      helper.add("Name of Node", getName().toString());
    }

    if (mCurrentNode.getKind() == NodeKind.BOOLEAN_VALUE || mCurrentNode.getKind() == NodeKind.STRING_VALUE
        || mCurrentNode.getKind() == NodeKind.NUMBER_VALUE) {
      helper.add("Value of Node", getValue());
    }

    if (mCurrentNode.getKind() == NodeKind.JSON_DOCUMENT) {
      helper.addValue("Node is DocumentRoot");
    }

    helper.add("node", mCurrentNode.toString());

    return helper.toString();
  }
}
