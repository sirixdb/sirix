package io.sirix.access.trx.node.json;

import io.sirix.utils.ToStringHelper;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AbstractNodeReadOnlyTrx;
import io.sirix.access.trx.node.InternalResourceSession;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.ResourceSession;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.visitor.JsonNodeVisitor;
import io.sirix.api.visitor.VisitResult;
import io.sirix.diff.JsonDiffSerializer;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.immutable.json.ImmutableArrayNode;
import io.sirix.node.immutable.json.ImmutableBooleanNode;
import io.sirix.node.immutable.json.ImmutableJsonDocumentRootNode;
import io.sirix.node.immutable.json.ImmutableNullNode;
import io.sirix.node.immutable.json.ImmutableNumberNode;
import io.sirix.node.immutable.json.ImmutableObjectNode;
import io.sirix.node.immutable.json.ImmutableStringNode;
import io.sirix.node.interfaces.ValueNode;
import io.sirix.node.interfaces.immutable.ImmutableJsonNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.node.json.ArrayNode;
import io.sirix.node.json.BooleanNode;
import io.sirix.node.json.JsonDocumentRootNode;
import io.sirix.node.json.NullNode;
import io.sirix.node.json.NumberNode;
import io.sirix.node.json.ObjectNamedArrayNode;
import io.sirix.node.json.ObjectNamedBooleanNode;
import io.sirix.node.json.ObjectNamedNullNode;
import io.sirix.node.json.ObjectNamedNumberNode;
import io.sirix.node.json.ObjectNamedObjectNode;
import io.sirix.node.json.ObjectNamedStringNode;
import io.sirix.node.json.ObjectNode;
import io.sirix.node.json.StringNode;
import io.sirix.service.xml.xpath.ItemListImpl;
import io.sirix.settings.Constants;
import io.brackit.query.atomic.QNm;

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

public final class JsonNodeReadOnlyTrxImpl extends
    AbstractNodeReadOnlyTrx<JsonNodeReadOnlyTrx, JsonNodeTrx, ImmutableNode> implements InternalJsonNodeReadOnlyTrx {

  /**
   * Constructor.
   *
   * @param resourceSession the current {@link ResourceSession} the reader is bound to
   * @param trxId ID of the reader
   * @param pageReadTransaction {@link StorageEngineReader} to interact with the page layer
   * @param documentNode the document node
   */
  JsonNodeReadOnlyTrxImpl(final InternalResourceSession<JsonNodeReadOnlyTrx, JsonNodeTrx> resourceSession,
      final int trxId, final StorageEngineReader pageReadTransaction,
      final ImmutableJsonNode documentNode) {
    super(trxId, pageReadTransaction, documentNode, resourceSession, new ItemListImpl());
  }

  private static final String INSERT = InsertOperations.INSERT.getName();
  private static final String UPDATE = InsertOperations.UPDATE.getName();
  private static final String DELETE = InsertOperations.DELETE.getName();
  private static final String REPLACE = InsertOperations.REPLACE.getName();

  @Override
  public boolean hasLastChild() {
    assertNotClosed();
    return getStructuralNodeView().hasLastChild();
  }

  @Override
  public boolean moveToLastChild() {
    assertNotClosed();
    if (getStructuralNodeView().hasLastChild()) {
      moveTo(getStructuralNodeView().getLastChildKey());
      return true;
    }
    return false;
  }

  @Override
  public List<JsonObject> getUpdateOperations() {
    final var revisionNumber = storageEngineReader instanceof StorageEngineWriter
        ? getRevisionNumber() - 1
        : getRevisionNumber();
    final var updateOperationsFile =
        resourceSession.getResourceConfig()
                       .getResource()
                       .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath())
                       .resolve("diffFromRev" + (revisionNumber - 1) + "toRev" + revisionNumber + ".json");

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

        if (storageEngineReader instanceof StorageEngineWriter) {
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
    // iter#31 Option B: fused OBJECT_NAMED_* carries the inline primitive value. Read
    // directly off the structural node — no synthetic-child indirection.
    return switch (getKind()) {
      case STRING_VALUE ->
        new String(((ValueNode) getStructuralNodeView()).getRawValue(), Constants.DEFAULT_ENCODING);
      case OBJECT_NAMED_STRING ->
        new String(((ObjectNamedStringNode) getStructuralNodeView()).getRawValue(), Constants.DEFAULT_ENCODING);
      case BOOLEAN_VALUE -> String.valueOf(((BooleanNode) getStructuralNodeView()).getValue());
      case OBJECT_NAMED_BOOLEAN -> String.valueOf(((ObjectNamedBooleanNode) getStructuralNodeView()).getValue());
      case NULL_VALUE, OBJECT_NAMED_NULL -> "null";
      case NUMBER_VALUE -> String.valueOf(((NumberNode) getStructuralNodeView()).getValue());
      case OBJECT_NAMED_NUMBER -> String.valueOf(((ObjectNamedNumberNode) getStructuralNodeView()).getValue());
      default -> "";
    };
  }

  @Override
  public byte[] getValueBytes() {
    assertNotClosed();
    return switch (getKind()) {
      case STRING_VALUE -> ((ValueNode) getStructuralNodeView()).getRawValue();
      case OBJECT_NAMED_STRING -> ((ObjectNamedStringNode) getStructuralNodeView()).getRawValue();
      default -> {
        String v = getValue();
        yield v != null ? v.getBytes(java.nio.charset.StandardCharsets.UTF_8) : null;
      }
    };
  }

  @Override
  public boolean getBooleanValue() {
    assertNotClosed();
    final NodeKind kind = getKind();
    if (kind == NodeKind.BOOLEAN_VALUE)
      return ((BooleanNode) getStructuralNodeView()).getValue();
    else if (kind == NodeKind.OBJECT_NAMED_BOOLEAN)
      return ((ObjectNamedBooleanNode) getStructuralNodeView()).getValue();
    throw new IllegalStateException("Current node is no boolean node.");
  }

  @Override
  public Number getNumberValue() {
    assertNotClosed();
    final NodeKind kind = getKind();
    if (kind == NodeKind.NUMBER_VALUE)
      return ((NumberNode) getStructuralNodeView()).getValue();
    else if (kind == NodeKind.OBJECT_NAMED_NUMBER)
      return ((ObjectNamedNumberNode) getStructuralNodeView()).getValue();
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
      case ARRAY -> ImmutableArrayNode.of((ArrayNode) currentNode);
      case BOOLEAN_VALUE -> ImmutableBooleanNode.of((BooleanNode) currentNode);
      case NUMBER_VALUE -> ImmutableNumberNode.of((NumberNode) currentNode);
      case STRING_VALUE -> ImmutableStringNode.of((StringNode) currentNode);
      case NULL_VALUE -> ImmutableNullNode.of((NullNode) currentNode);
      // Fused OBJECT_NAMED_* kinds are treated as themselves — no legacy immutable wrapper exists
      // yet. Callers that pattern-match on ImmutableNode will need to handle the concrete class.
      case OBJECT_NAMED_BOOLEAN, OBJECT_NAMED_NUMBER, OBJECT_NAMED_STRING, OBJECT_NAMED_NULL,
           OBJECT_NAMED_OBJECT, OBJECT_NAMED_ARRAY ->
          (ImmutableNode) currentNode;
      case JSON_DOCUMENT -> ImmutableJsonDocumentRootNode.of((JsonDocumentRootNode) currentNode);
      default -> throw new IllegalStateException("Node kind not known!");
    };
  }

  @Override
  public boolean isArray() {
    assertNotClosed();
    // iter#32 P2: OBJECT_NAMED_ARRAY plays the ARRAY role (fused). Cursor identity check.
    final var kind = getKind();
    return kind == NodeKind.ARRAY || kind == NodeKind.OBJECT_NAMED_ARRAY;
  }

  @Override
  public boolean isObject() {
    assertNotClosed();
    // iter#32 P2: OBJECT_NAMED_OBJECT plays the OBJECT role (fused). Cursor identity check.
    final var kind = getKind();
    return kind == NodeKind.OBJECT || kind == NodeKind.OBJECT_NAMED_OBJECT;
  }

  @Override
  public boolean isObjectKey() {
    assertNotClosed();
    // Use getKind() for zero-allocation check. Phase 4 — only fused OBJECT_NAMED_* records
    // carry the field-name role; the legacy OBJECT_KEY kind has been deleted.
    final var kind = getKind();
    return kind == NodeKind.OBJECT_NAMED_BOOLEAN
        || kind == NodeKind.OBJECT_NAMED_NUMBER
        || kind == NodeKind.OBJECT_NAMED_STRING
        || kind == NodeKind.OBJECT_NAMED_NULL
        || kind == NodeKind.OBJECT_NAMED_OBJECT
        || kind == NodeKind.OBJECT_NAMED_ARRAY;
  }

  @Override
  public boolean isNumberValue() {
    assertNotClosed();
    // Use getKind() for zero-allocation check
    final var kind = getKind();
    return kind == NodeKind.NUMBER_VALUE
        || kind == NodeKind.OBJECT_NAMED_NUMBER;
  }

  @Override
  public boolean isNullValue() {
    assertNotClosed();
    // Use getKind() for zero-allocation check
    final var kind = getKind();
    return kind == NodeKind.NULL_VALUE
        || kind == NodeKind.OBJECT_NAMED_NULL;
  }

  @Override
  public boolean isStringValue() {
    assertNotClosed();
    // Use getKind() for zero-allocation check
    final var kind = getKind();
    return kind == NodeKind.STRING_VALUE
        || kind == NodeKind.OBJECT_NAMED_STRING;
  }

  @Override
  public boolean isBooleanValue() {
    assertNotClosed();
    // Use getKind() for zero-allocation check
    final var kind = getKind();
    return kind == NodeKind.BOOLEAN_VALUE
        || kind == NodeKind.OBJECT_NAMED_BOOLEAN;
  }

  @Override
  public boolean isDocumentRoot() {
    assertNotClosed();
    // Use getKind() for zero-allocation check
    return getKind() == NodeKind.JSON_DOCUMENT;
  }

  @Override
  public QNm getName() {
    assertNotClosed();

    final NodeKind kind = getKind();
    if (kind == NodeKind.OBJECT_NAMED_BOOLEAN || kind == NodeKind.OBJECT_NAMED_NUMBER
        || kind == NodeKind.OBJECT_NAMED_STRING || kind == NodeKind.OBJECT_NAMED_NULL
        || kind == NodeKind.OBJECT_NAMED_OBJECT || kind == NodeKind.OBJECT_NAMED_ARRAY) {
      final int nameKey = getFusedNamedNodeKey(kind);
      if (nameKey == -1) {
        return new QNm("");
      }
      final String localName = storageEngineReader.getName(nameKey, NodeKind.OBJECT_NAMED_OBJECT);
      return new QNm(localName);
    }

    return null;
  }

  private int getFusedNamedNodeKey(final NodeKind kind) {
    // Transient int read — the VIEW avoids materializing a snapshot per object-key emit.
    return switch (kind) {
      case OBJECT_NAMED_BOOLEAN -> ((ObjectNamedBooleanNode) getStructuralNodeView()).getNameKey();
      case OBJECT_NAMED_NUMBER -> ((ObjectNamedNumberNode) getStructuralNodeView()).getNameKey();
      case OBJECT_NAMED_STRING -> ((ObjectNamedStringNode) getStructuralNodeView()).getNameKey();
      case OBJECT_NAMED_NULL -> ((ObjectNamedNullNode) getStructuralNodeView()).getNameKey();
      case OBJECT_NAMED_OBJECT -> ((ObjectNamedObjectNode) getStructuralNodeView()).getNameKey();
      case OBJECT_NAMED_ARRAY -> ((ObjectNamedArrayNode) getStructuralNodeView()).getNameKey();
      default -> -1;
    };
  }

  @Override
  public VisitResult acceptVisitor(final JsonNodeVisitor visitor) {
    assertNotClosed();
    return ((ImmutableJsonNode) getStructuralNodeView()).acceptVisitor(visitor);
  }

  @Override
  public int getNameKey() {
    assertNotClosed();
    final NodeKind kind = getKind();
    if (kind == NodeKind.OBJECT_NAMED_BOOLEAN || kind == NodeKind.OBJECT_NAMED_NUMBER
        || kind == NodeKind.OBJECT_NAMED_STRING || kind == NodeKind.OBJECT_NAMED_NULL
        || kind == NodeKind.OBJECT_NAMED_OBJECT || kind == NodeKind.OBJECT_NAMED_ARRAY) {
      return getFusedNamedNodeKey(kind);
    }
    return -1;
  }

  @Override
  public String toString() {
    final ToStringHelper helper = ToStringHelper.of(this);
    helper.add("Revision number", getRevisionNumber());
    final var currentNode = getStructuralNodeView();
    final var name = getName();
    if (currentNode.getKind().playsObjectKeyRole() && name != null) {
      helper.add("Name of Node", name.toString());
    }

    if (currentNode.getKind() == NodeKind.BOOLEAN_VALUE || currentNode.getKind() == NodeKind.STRING_VALUE
        || currentNode.getKind() == NodeKind.NUMBER_VALUE) {
      helper.add("Value of Node", getValue());
    }

    if (currentNode.getKind() == NodeKind.JSON_DOCUMENT) {
      helper.addValue("Node is DocumentRoot");
    }

    helper.add("node", currentNode.toString());

    return helper.toString();
  }
}
