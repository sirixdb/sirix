package io.sirix.access.trx.node.json;

import com.google.common.base.MoreObjects;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AbstractNodeReadOnlyTrx;
import io.sirix.access.trx.node.InternalResourceSession;
import io.sirix.api.PageReadOnlyTrx;
import io.sirix.api.PageTrx;
import io.sirix.api.ResourceSession;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.visitor.JsonNodeVisitor;
import io.sirix.api.visitor.VisitResult;
import io.sirix.diff.JsonDiffSerializer;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.immutable.json.*;
import io.sirix.node.interfaces.ValueNode;
import io.sirix.node.interfaces.immutable.ImmutableJsonNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.node.json.*;
import io.sirix.service.xml.xpath.ItemListImpl;
import io.sirix.settings.Constants;
import io.brackit.query.atomic.QNm;
import org.checkerframework.checker.index.qual.NonNegative;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public final class JsonNodeReadOnlyTrxImpl
    extends AbstractNodeReadOnlyTrx<JsonNodeReadOnlyTrx, JsonNodeTrx, ImmutableNode>
    implements InternalJsonNodeReadOnlyTrx {

  /**
   * Constructor.
   *
   * @param resourceManager     the current {@link ResourceSession} the reader is bound to
   * @param trxId               ID of the reader
   * @param pageReadTransaction {@link PageReadOnlyTrx} to interact with the page layer
   * @param documentNode        the document node
   */
  JsonNodeReadOnlyTrxImpl(final InternalResourceSession<JsonNodeReadOnlyTrx, JsonNodeTrx> resourceManager,
      final @NonNegative long trxId, final PageReadOnlyTrx pageReadTransaction, final ImmutableJsonNode documentNode) {
    super(trxId, pageReadTransaction, documentNode, resourceManager, new ItemListImpl());
  }

  private final String INSERT = InsertOperations.INSERT.getName();
  private final String UPDATE = InsertOperations.UPDATE.getName();
  private final String DELETE = InsertOperations.DELETE.getName();
  private final String REPLACE = InsertOperations.REPLACE.getName();

  @Override
  public boolean hasLastChild() {
    assertNotClosed();
    return getStructuralNode().hasLastChild();
  }

  @Override
  public boolean moveToLastChild() {
    assertNotClosed();
    if (getStructuralNode().hasLastChild()) {
      moveTo(getStructuralNode().getLastChildKey());
      return true;
    }
    return false;
  }

  @Override
  public List<JsonObject> getUpdateOperations() {
    final var revisionNumber = pageReadOnlyTrx instanceof PageTrx ? getRevisionNumber() - 1 : getRevisionNumber();
    final var updateOperationsFile = resourceSession.getResourceConfig()
                                                    .getResource()
                                                    .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath())
                                                    .resolve(
                                                        "diffFromRev" + (revisionNumber - 1) + "toRev" + revisionNumber
                                                            + ".json");

    final var diffTuples = new ArrayList<JsonObject>();

    try {
      final var jsonElement = JsonParser.parseString(Files.readString(updateOperationsFile));
      final var jsonObject = jsonElement.getAsJsonObject();
      final var diffs = jsonObject.getAsJsonArray("diffs");

      diffs.forEach(serializeJsonFragmentIfNeeded(diffTuples));
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }

    return diffTuples;
  }

  private Consumer<JsonElement> serializeJsonFragmentIfNeeded(final List<JsonObject> diffTuples) {
    return diff -> {
      final var diffObject = diff.getAsJsonObject();

      final JsonObject diffTupleObject;
      if (diffObject.has(INSERT)) {
        diffTupleObject = diffObject.getAsJsonObject(INSERT);
      } else if (diffObject.has(REPLACE)) {
        diffTupleObject = diffObject.getAsJsonObject(REPLACE);
      } else {
        diffTupleObject = null;
      }

      if (diffTupleObject != null && "jsonFragment".equals(diffTupleObject.getAsJsonPrimitive("type").getAsString())) {
        final var nodeKey = diffTupleObject.get("nodeKey").getAsLong();
        final var currentNodeKey = getNodeKey();
        moveTo(nodeKey);

        final int revisionNumber;

        if (pageReadOnlyTrx instanceof PageTrx) {
          revisionNumber = getRevisionNumber() - 1;
        } else {
          revisionNumber = getRevisionNumber();
        }

        JsonDiffSerializer.serialize(revisionNumber, this.getResourceSession(), this, diffTupleObject);
        moveTo(currentNodeKey);
      }

      diffTuples.add(diff.getAsJsonObject());
    };
  }

  @Override
  public List<JsonObject> getUpdateOperationsInSubtreeOfNode(final SirixDeweyID rootDeweyId, final long maxDepth) {
    requireNonNull(rootDeweyId);

    final var updateOperations = getUpdateOperations();

    return updateOperations.stream()
                           .filter(filterAncestorOperations(rootDeweyId, maxDepth))
                           .sorted(sortByDeweyID())
                           .collect(Collectors.toList());
  }

  private Comparator<JsonObject> sortByDeweyID() {
    return Comparator.comparing(updateOperation -> {
      if (updateOperation.has(INSERT)) {
        return getDeweyID(updateOperation, INSERT);
      } else if (updateOperation.has(DELETE)) {
        return getDeweyID(updateOperation, DELETE);
      } else if (updateOperation.has(UPDATE)) {
        return getDeweyID(updateOperation, UPDATE);
      } else if (updateOperation.has(REPLACE)) {
        return getDeweyID(updateOperation, REPLACE);
      }
      throw new IllegalStateException(updateOperation + " not known.");
    });
  }

  private Predicate<JsonObject> filterAncestorOperations(SirixDeweyID rootDeweyId, long maxDepth) {
    return updateOperation -> {
      if (updateOperation.has(INSERT)) {
        return isDescendatOrSelfOf(rootDeweyId, updateOperation, INSERT, maxDepth);
      } else if (updateOperation.has(DELETE)) {
        return isDescendatOrSelfOf(rootDeweyId, updateOperation, DELETE, maxDepth);
      } else if (updateOperation.has(UPDATE)) {
        return isDescendatOrSelfOf(rootDeweyId, updateOperation, UPDATE, maxDepth);
      } else if (updateOperation.has(REPLACE)) {
        return isDescendatOrSelfOf(rootDeweyId, updateOperation, REPLACE, maxDepth);
      } else {
        throw new IllegalStateException(updateOperation + " not known.");
      }
    };
  }

  private SirixDeweyID getDeweyID(final JsonObject updateOperation, final String operation) {
    final var opAsJsonObject = updateOperation.getAsJsonObject(operation);
    return new SirixDeweyID(opAsJsonObject.getAsJsonPrimitive("deweyID").getAsString());
  }

  private boolean isDescendatOrSelfOf(final SirixDeweyID rootDeweyId, final JsonObject updateOperation,
      final String operation, final long maxDepth) {
    final var opAsJsonObject = updateOperation.getAsJsonObject(operation);

    final var deweyId = new SirixDeweyID(opAsJsonObject.getAsJsonPrimitive("deweyID").getAsString());

    return deweyId.isDescendantOrSelfOf(rootDeweyId) && deweyId.getLevel() - rootDeweyId.getLevel() <= maxDepth;
  }

  @Override
  public String getValue() {
    assertNotClosed();

    final var currentNode = getCurrentNode();
    // $CASES-OMITTED$
    return switch (currentNode.getKind()) {
      case OBJECT_STRING_VALUE, STRING_VALUE ->
          new String(((ValueNode) currentNode).getRawValue(), Constants.DEFAULT_ENCODING);
      case OBJECT_BOOLEAN_VALUE -> String.valueOf(((ObjectBooleanNode) currentNode).getValue());
      case BOOLEAN_VALUE -> String.valueOf(((BooleanNode) currentNode).getValue());
      case OBJECT_NULL_VALUE, NULL_VALUE -> "null";
      case OBJECT_NUMBER_VALUE -> String.valueOf(((ObjectNumberNode) currentNode).getValue());
      case NUMBER_VALUE -> String.valueOf(((NumberNode) currentNode).getValue());
      default -> "";
    };
  }

  @Override
  public boolean getBooleanValue() {
    assertNotClosed();

    final var currentNode = getCurrentNode();
    if (currentNode.getKind() == NodeKind.BOOLEAN_VALUE)
      return ((BooleanNode) currentNode).getValue();
    else if (currentNode.getKind() == NodeKind.OBJECT_BOOLEAN_VALUE)
      return ((ObjectBooleanNode) currentNode).getValue();
    throw new IllegalStateException("Current node is no boolean node.");
  }

  @Override
  public Number getNumberValue() {
    assertNotClosed();
    final var currentNode = getCurrentNode();
    if (currentNode.getKind() == NodeKind.NUMBER_VALUE)
      return ((NumberNode) currentNode).getValue();
    else if (currentNode.getKind() == NodeKind.OBJECT_NUMBER_VALUE)
      return ((ObjectNumberNode) currentNode).getValue();
    throw new IllegalStateException("Current node is no number node.");
  }

  @Override
  public JsonResourceSession getResourceSession() {
    assertNotClosed();
    return (JsonResourceSession) resourceSession;
  }

  @Override
  public ImmutableNode getNode() {
    assertNotClosed();

    final var currentNode = getCurrentNode();
    // $CASES-OMITTED$
    return switch (currentNode.getKind()) {
      case OBJECT -> ImmutableObjectNode.of((ObjectNode) currentNode);
      case OBJECT_KEY -> ImmutableObjectKeyNode.of((ObjectKeyNode) currentNode);
      case ARRAY -> ImmutableArrayNode.of((ArrayNode) currentNode);
      case BOOLEAN_VALUE -> ImmutableBooleanNode.of((BooleanNode) currentNode);
      case NUMBER_VALUE -> ImmutableNumberNode.of((NumberNode) currentNode);
      case STRING_VALUE -> ImmutableStringNode.of((StringNode) currentNode);
      case NULL_VALUE -> ImmutableNullNode.of((NullNode) currentNode);
      case OBJECT_BOOLEAN_VALUE -> ImmutableObjectBooleanNode.of((ObjectBooleanNode) currentNode);
      case OBJECT_NUMBER_VALUE -> ImmutableObjectNumberNode.of((ObjectNumberNode) currentNode);
      case OBJECT_STRING_VALUE -> ImmutableObjectStringNode.of((ObjectStringNode) currentNode);
      case OBJECT_NULL_VALUE -> ImmutableObjectNullNode.of((ObjectNullNode) currentNode);
      case JSON_DOCUMENT -> ImmutableJsonDocumentRootNode.of((JsonDocumentRootNode) currentNode);
      default -> throw new IllegalStateException("Node kind not known!");
    };
  }

  @Override
  public boolean isArray() {
    assertNotClosed();
    return getCurrentNode().getKind() == NodeKind.ARRAY;
  }

  @Override
  public boolean isObject() {
    assertNotClosed();
    return getCurrentNode().getKind() == NodeKind.OBJECT;
  }

  @Override
  public boolean isObjectKey() {
    assertNotClosed();
    return getCurrentNode().getKind() == NodeKind.OBJECT_KEY;
  }

  @Override
  public boolean isNumberValue() {
    assertNotClosed();
    final var currentNode = getCurrentNode();
    return currentNode.getKind() == NodeKind.NUMBER_VALUE || currentNode.getKind() == NodeKind.OBJECT_NUMBER_VALUE;
  }

  @Override
  public boolean isNullValue() {
    assertNotClosed();
    final var currentNode = getCurrentNode();
    return currentNode.getKind() == NodeKind.NULL_VALUE || currentNode.getKind() == NodeKind.OBJECT_NULL_VALUE;
  }

  @Override
  public boolean isStringValue() {
    assertNotClosed();
    final var currentNode = getCurrentNode();
    return currentNode.getKind() == NodeKind.STRING_VALUE || currentNode.getKind() == NodeKind.OBJECT_STRING_VALUE;
  }

  @Override
  public boolean isBooleanValue() {
    assertNotClosed();
    final var currentNode = getCurrentNode();
    return currentNode.getKind() == NodeKind.BOOLEAN_VALUE || currentNode.getKind() == NodeKind.OBJECT_BOOLEAN_VALUE;
  }

  @Override
  public boolean isDocumentRoot() {
    assertNotClosed();
    final var currentNode = getCurrentNode();
    return currentNode.getKind() == NodeKind.JSON_DOCUMENT;
  }

  @Override
  public QNm getName() {
    assertNotClosed();

    final var currentNode = getCurrentNode();
    if (currentNode.getKind() == NodeKind.OBJECT_KEY) {
      final int nameKey = ((ObjectKeyNode) currentNode).getNameKey();
      final String localName = nameKey == -1 ? "" : pageReadOnlyTrx.getName(nameKey, currentNode.getKind());
      return new QNm(localName);
    }

    return null;
  }

  @Override
  public VisitResult acceptVisitor(final JsonNodeVisitor visitor) {
    assertNotClosed();
    return ((ImmutableJsonNode) getCurrentNode()).acceptVisitor(visitor);
  }

  @Override
  public int getNameKey() {
    assertNotClosed();
    final var currentNode = getCurrentNode();
    if (currentNode.getKind() == NodeKind.OBJECT_KEY) {
      return ((ObjectKeyNode) currentNode).getNameKey();
    }
    return -1;
  }

  @Override
  public String toString() {
    final MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
    helper.add("Revision number", getRevisionNumber());
    final var currentNode = getCurrentNode();
    final var name = getName();
    if (currentNode.getKind() == NodeKind.OBJECT_KEY && name != null) {
      helper.add("Name of Node", name.toString());
    }

    if (currentNode.getKind() == NodeKind.BOOLEAN_VALUE || currentNode.getKind() == NodeKind.STRING_VALUE
        || currentNode.getKind() == NodeKind.NUMBER_VALUE || currentNode.getKind() == NodeKind.OBJECT_BOOLEAN_VALUE
        || currentNode.getKind() == NodeKind.OBJECT_NULL_VALUE || currentNode.getKind() == NodeKind.OBJECT_NUMBER_VALUE
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
