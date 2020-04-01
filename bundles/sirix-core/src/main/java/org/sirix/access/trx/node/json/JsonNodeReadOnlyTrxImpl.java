package org.sirix.access.trx.node.json;

import com.google.common.base.MoreObjects;
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
import org.sirix.node.immutable.json.*;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.ValueNode;
import org.sirix.node.interfaces.immutable.ImmutableJsonNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.node.json.*;
import org.sirix.page.PageKind;
import org.sirix.settings.Constants;

import javax.annotation.Nonnegative;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class JsonNodeReadOnlyTrxImpl extends AbstractNodeReadTrx<JsonNodeReadOnlyTrx>
    implements InternalJsonNodeReadOnlyTrx {

  /**
   * ID of transaction.
   */
  private final long trxId;

  /**
   * Resource manager this write transaction is bound to.
   */
  protected final InternalResourceManager<JsonNodeReadOnlyTrx, JsonNodeTrx> resourceManager;

  /**
   * Tracks whether the transaction is closed.
   */
  private boolean isClosed;

  /**
   * Constructor.
   *
   * @param resourceManager     the current {@link ResourceManager} the reader is bound to
   * @param trxId               ID of the reader
   * @param pageReadTransaction {@link PageReadOnlyTrx} to interact with the page layer
   * @param documentNode        the document node
   */
  JsonNodeReadOnlyTrxImpl(final InternalResourceManager<JsonNodeReadOnlyTrx, JsonNodeTrx> resourceManager,
      final @Nonnegative long trxId, final PageReadOnlyTrx pageReadTransaction, final ImmutableJsonNode documentNode) {
    super(trxId, pageReadTransaction, documentNode);
    this.resourceManager = checkNotNull(resourceManager);
    checkArgument(trxId >= 0);
    this.trxId = trxId;
    isClosed = false;
  }

  @Override
  public Move<JsonNodeReadOnlyTrx> moveTo(long nodeKey) {
    assertNotClosed();

    // Remember old node and fetch new one.
    final ImmutableNode oldNode = currentNode;
    Optional<? extends Record> newNode;
    try {
      // Immediately return node from item list if node key negative.
      if (nodeKey < 0) {
        newNode = Optional.empty();
      } else {
        newNode = pageReadTrx.getRecord(nodeKey, PageKind.RECORDPAGE, -1);
      }
    } catch (final SirixIOException e) {
      newNode = Optional.empty();
    }

    if (newNode.isPresent()) {
      currentNode = (Node) newNode.get();
      return Move.moved(this);
    } else {
      currentNode = oldNode;
      return Move.notMoved();
    }
  }

  @Override
  public String getValue() {
    assertNotClosed();
    final String returnVal;
    switch (currentNode.getKind()) {
      case OBJECT_STRING_VALUE:
      case STRING_VALUE:
        returnVal = new String(((ValueNode) currentNode).getRawValue(), Constants.DEFAULT_ENCODING);
        break;
      case OBJECT_BOOLEAN_VALUE:
        returnVal = String.valueOf(((ObjectBooleanNode) currentNode).getValue());
        break;
      case BOOLEAN_VALUE:
        returnVal = String.valueOf(((BooleanNode) currentNode).getValue());
        break;
      case OBJECT_NULL_VALUE:
      case NULL_VALUE:
        returnVal = "null";
        break;
      case OBJECT_NUMBER_VALUE:
        returnVal = String.valueOf(((ObjectNumberNode) currentNode).getValue());
        break;
      case NUMBER_VALUE:
        returnVal = String.valueOf(((NumberNode) currentNode).getValue());
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
    if (currentNode.getKind() == NodeKind.BOOLEAN_VALUE)
      return ((BooleanNode) currentNode).getValue();
    else if (currentNode.getKind() == NodeKind.OBJECT_BOOLEAN_VALUE)
      return ((ObjectBooleanNode) currentNode).getValue();
    throw new IllegalStateException("Current node is no boolean node.");
  }

  @Override
  public Number getNumberValue() {
    assertNotClosed();
    if (currentNode.getKind() == NodeKind.NUMBER_VALUE)
      return ((NumberNode) currentNode).getValue();
    else if (currentNode.getKind() == NodeKind.OBJECT_NUMBER_VALUE)
      return ((ObjectNumberNode) currentNode).getValue();
    throw new IllegalStateException("Current node is no number node.");
  }

  @Override
  public void assertNotClosed() {
    if (isClosed) {
      throw new IllegalStateException("Transaction is already closed.");
    }
  }

  @Override
  public JsonResourceManager getResourceManager() {
    assertNotClosed();
    return (JsonResourceManager) resourceManager;
  }

  @Override
  public ImmutableNode getNode() {
    assertNotClosed();

    switch (currentNode.getKind()) {
      case OBJECT:
        return ImmutableObjectNode.of((ObjectNode) currentNode);
      case OBJECT_KEY:
        return ImmutableObjectKeyNode.of((ObjectKeyNode) currentNode);
      case ARRAY:
        return ImmutableArrayNode.of((ArrayNode) currentNode);
      case BOOLEAN_VALUE:
        return ImmutableBooleanNode.of((BooleanNode) currentNode);
      case NUMBER_VALUE:
        return ImmutableNumberNode.of((NumberNode) currentNode);
      case STRING_VALUE:
        return ImmutableStringNode.of((StringNode) currentNode);
      case NULL_VALUE:
        return ImmutableNullNode.of((NullNode) currentNode);
      case OBJECT_BOOLEAN_VALUE:
        return ImmutableObjectBooleanNode.of((ObjectBooleanNode) currentNode);
      case OBJECT_NUMBER_VALUE:
        return ImmutableObjectNumberNode.of((ObjectNumberNode) currentNode);
      case OBJECT_STRING_VALUE:
        return ImmutableObjectStringNode.of((ObjectStringNode) currentNode);
      case OBJECT_NULL_VALUE:
        return ImmutableObjectNullNode.of((ObjectNullNode) currentNode);
      case JSON_DOCUMENT:
        return ImmutableJsonDocumentRootNode.of((JsonDocumentRootNode) currentNode);
      // $CASES-OMITTED$
      default:
        throw new IllegalStateException("Node kind not known!");
    }
  }

  @Override
  public boolean isArray() {
    assertNotClosed();
    return currentNode.getKind() == NodeKind.ARRAY;
  }

  @Override
  public boolean isObject() {
    assertNotClosed();
    return currentNode.getKind() == NodeKind.OBJECT;
  }

  @Override
  public boolean isObjectKey() {
    assertNotClosed();
    return currentNode.getKind() == NodeKind.OBJECT_KEY;
  }

  @Override
  public boolean isNumberValue() {
    assertNotClosed();
    return currentNode.getKind() == NodeKind.NUMBER_VALUE || currentNode.getKind() == NodeKind.OBJECT_NUMBER_VALUE;
  }

  @Override
  public boolean isNullValue() {
    assertNotClosed();
    return currentNode.getKind() == NodeKind.NULL_VALUE || currentNode.getKind() == NodeKind.OBJECT_NULL_VALUE;
  }

  @Override
  public boolean isStringValue() {
    assertNotClosed();
    return currentNode.getKind() == NodeKind.STRING_VALUE || currentNode.getKind() == NodeKind.OBJECT_STRING_VALUE;
  }

  @Override
  public boolean isBooleanValue() {
    assertNotClosed();
    return currentNode.getKind() == NodeKind.BOOLEAN_VALUE || currentNode.getKind() == NodeKind.OBJECT_BOOLEAN_VALUE;
  }

  @Override
  public void close() {
    if (!isClosed) {
      // Close own state.
      pageReadTrx.close();

      // Callback on session to make sure everything is cleaned up.
      resourceManager.closeReadTransaction(trxId);

      setPageReadTransaction(null);

      // Immediately release all references.
      pageReadTrx = null;
      currentNode = null;

      // Close state.
      isClosed = true;
    }
  }

  @Override
  protected JsonNodeReadOnlyTrx thisInstance() {
    return this;
  }

  @Override
  public boolean isDocumentRoot() {
    assertNotClosed();
    return currentNode.getKind() == NodeKind.JSON_DOCUMENT;
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  @Override
  public QNm getName() {
    assertNotClosed();

    if (currentNode.getKind() == NodeKind.OBJECT_KEY) {
      final int nameKey = ((ObjectKeyNode) currentNode).getNameKey();
      final String localName = nameKey == -1 ? "" : pageReadTrx.getName(nameKey, currentNode.getKind());
      return new QNm(localName);
    }

    return null;
  }

  @Override
  public VisitResult acceptVisitor(JsonNodeVisitor visitor) {
    assertNotClosed();
    return ((ImmutableJsonNode) currentNode).acceptVisitor(visitor);
  }

  @Override
  public ImmutableJsonNode getCurrentNode() {
    return (ImmutableJsonNode) currentNode;
  }

  @Override
  public void setCurrentNode(ImmutableJsonNode node) {
    assertNotClosed();
    currentNode = node;
  }

  @Override
  public int getNameKey() {
    assertNotClosed();
    if (currentNode.getKind() == NodeKind.OBJECT_KEY) {
      return ((ObjectKeyNode) currentNode).getNameKey();
    }
    return -1;
  }

  @Override
  public String toString() {
    final MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
    helper.add("Revision number", getRevisionNumber());

    if (currentNode.getKind() == NodeKind.OBJECT_KEY) {
      helper.add("Name of Node", getName().toString());
    }

    if (currentNode.getKind() == NodeKind.BOOLEAN_VALUE || currentNode.getKind() == NodeKind.STRING_VALUE
        || currentNode.getKind() == NodeKind.NUMBER_VALUE || currentNode.getKind() == NodeKind.OBJECT_BOOLEAN_VALUE
        || currentNode.getKind() == NodeKind.OBJECT_NULL_VALUE
        || currentNode.getKind() == NodeKind.OBJECT_NUMBER_VALUE
        || currentNode.getKind() == NodeKind.OBJECT_STRING_VALUE) {
      helper.add("Value of Node", getValue());
    }

    if (currentNode.getKind() == NodeKind.JSON_DOCUMENT) {
      helper.addValue("Node is DocumentRoot");
    }

    helper.add("node", currentNode.toString());

    return helper.toString();
  }
}
