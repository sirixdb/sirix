package org.sirix.access.trx.node.json;

import com.google.common.base.MoreObjects;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.brackit.xquery.atomic.QNm;
import org.checkerframework.checker.index.qual.NonNegative;
import org.sirix.access.ResourceConfiguration;
import org.sirix.access.trx.node.AbstractNodeReadOnlyTrx;
import org.sirix.access.trx.node.InternalResourceSession;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.PageTrx;
import org.sirix.api.ResourceSession;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.api.json.JsonResourceSession;
import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.diff.JsonDiffSerializer;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.immutable.json.*;
import org.sirix.node.interfaces.ValueNode;
import org.sirix.node.interfaces.immutable.ImmutableJsonNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.node.json.*;
import org.sirix.service.xml.xpath.ItemListImpl;
import org.sirix.settings.Constants;

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

public final class JsonNodeReadOnlyTrxImpl extends AbstractNodeReadOnlyTrx<JsonNodeReadOnlyTrx, JsonNodeTrx,
        ImmutableNode> implements InternalJsonNodeReadOnlyTrx {

  /**
   * Constructor.
   *
   * @param resourceManager     the current {@link ResourceSession} the reader is bound to
   * @param trxId               ID of the reader
   * @param pageReadTransaction {@link PageReadOnlyTrx} to interact with the page layer
   * @param documentNode        the document node
   */
  JsonNodeReadOnlyTrxImpl(final InternalResourceSession<JsonNodeReadOnlyTrx, JsonNodeTrx> resourceManager,
                          final @NonNegative long trxId,
                          final PageReadOnlyTrx pageReadTransaction,
                          final ImmutableJsonNode documentNode) {
    super(trxId, pageReadTransaction, documentNode, resourceManager, new ItemListImpl());
  }

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
      if (diffObject.has("insert")) {
        diffTupleObject = diffObject.getAsJsonObject("insert");
      } else if (diffObject.has("replace")) {
        diffTupleObject = diffObject.getAsJsonObject("replace");
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
      if (updateOperation.has("insert")) {
        return getDeweyID(updateOperation, "insert");
      } else if (updateOperation.has("delete")) {
        return getDeweyID(updateOperation, "delete");
      } else if (updateOperation.has("update")) {
        return getDeweyID(updateOperation, "update");
      } else if (updateOperation.has("replace")) {
        return getDeweyID(updateOperation, "replace");
      }
      throw new IllegalStateException(updateOperation + " not known.");
    });
  }

  private Predicate<JsonObject> filterAncestorOperations(SirixDeweyID rootDeweyId, long maxDepth) {
    return updateOperation -> {
      if (updateOperation.has("insert")) {
        return isDescendatOrSelfOf(rootDeweyId, updateOperation, "insert", maxDepth);
      } else if (updateOperation.has("delete")) {
        return isDescendatOrSelfOf(rootDeweyId, updateOperation, "delete", maxDepth);
      } else if (updateOperation.has("update")) {
        return isDescendatOrSelfOf(rootDeweyId, updateOperation, "update", maxDepth);
      } else if (updateOperation.has("replace")) {
        return isDescendatOrSelfOf(rootDeweyId, updateOperation, "replace", maxDepth);
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
      case OBJECT_STRING_VALUE, STRING_VALUE -> new String(((ValueNode) currentNode).getRawValue(),
                                                           Constants.DEFAULT_ENCODING);
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
