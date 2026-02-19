package io.sirix.access.trx.node.json;

import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexType;
import io.sirix.index.path.summary.PathNode;
import io.sirix.node.DeweyIDNode;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.delegates.NameNodeDelegate;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.delegates.StructNodeDelegate;
import io.sirix.node.json.ArrayNode;
import io.sirix.node.json.BooleanNode;
import io.sirix.node.json.NullNode;
import io.sirix.node.json.NumberNode;
import io.sirix.node.json.ObjectBooleanNode;
import io.sirix.node.json.ObjectKeyNode;
import io.sirix.node.json.ObjectNode;
import io.sirix.node.json.ObjectNullNode;
import io.sirix.node.json.ObjectNumberNode;
import io.sirix.node.json.ObjectStringNode;
import io.sirix.node.json.StringNode;
import io.sirix.page.PathSummaryPage;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import io.sirix.settings.StringCompressionType;
import io.sirix.utils.NamePageHash;
import net.openhft.hashing.LongHashFunction;
import io.brackit.query.atomic.QNm;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;

import static java.util.Objects.requireNonNull;

/**
 * Node factory to create nodes.
 *
 * @author Johannes Lichtenberger
 */
final class JsonNodeFactoryImpl implements JsonNodeFactory {

  /**
   * Hash function used to hash nodes.
   */
  private final LongHashFunction hashFunction;

  /**
   * {@link StorageEngineWriter} implementation.
   */
  private final StorageEngineWriter storageEngineWriter;

  /**
   * The current revision number.
   */
  private final int revisionNumber;

  /**
   * Transaction-local reusable proxies for selected hot-path JSON node kinds.
   */
  private final ObjectNode reusableObjectNode;
  private final ArrayNode reusableArrayNode;
  private final ObjectKeyNode reusableObjectKeyNode;
  private final NullNode reusableNullNode;
  private final BooleanNode reusableBooleanNode;
  private final NumberNode reusableNumberNode;
  private final StringNode reusableStringNode;
  private final ObjectNullNode reusableObjectNullNode;
  private final ObjectBooleanNode reusableObjectBooleanNode;
  private final ObjectNumberNode reusableObjectNumberNode;
  private final ObjectStringNode reusableObjectStringNode;

  /**
   * Constructor.
   *
   * @param hashFunction hash function used to hash nodes
   * @param storageEngineWriter {@link StorageEngineWriter} implementation
   */
  JsonNodeFactoryImpl(final LongHashFunction hashFunction, final StorageEngineWriter storageEngineWriter) {
    this.hashFunction = requireNonNull(hashFunction);
    this.storageEngineWriter = requireNonNull(storageEngineWriter);
    this.revisionNumber = storageEngineWriter.getRevisionNumber();
    this.reusableObjectNode =
        new ObjectNode(0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber, Fixed.NULL_NODE_KEY.getStandardProperty(),
            Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(),
            Fixed.NULL_NODE_KEY.getStandardProperty(), 0, 0, 0, hashFunction, (SirixDeweyID) null);
    this.reusableArrayNode = new ArrayNode(0, 0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber,
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), 0, 0, 0, hashFunction,
        (SirixDeweyID) null);
    this.reusableObjectKeyNode = new ObjectKeyNode(0, 0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber,
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(), 0, 0, 0, hashFunction, (SirixDeweyID) null);
    this.reusableNullNode =
        new NullNode(0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber, Fixed.NULL_NODE_KEY.getStandardProperty(),
            Fixed.NULL_NODE_KEY.getStandardProperty(), 0, hashFunction, (SirixDeweyID) null);
    this.reusableBooleanNode =
        new BooleanNode(0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber, Fixed.NULL_NODE_KEY.getStandardProperty(),
            Fixed.NULL_NODE_KEY.getStandardProperty(), 0, false, hashFunction, (SirixDeweyID) null);
    this.reusableNumberNode =
        new NumberNode(0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber, Fixed.NULL_NODE_KEY.getStandardProperty(),
            Fixed.NULL_NODE_KEY.getStandardProperty(), 0, 0, hashFunction, (SirixDeweyID) null);
    this.reusableStringNode =
        new StringNode(0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber, Fixed.NULL_NODE_KEY.getStandardProperty(),
            Fixed.NULL_NODE_KEY.getStandardProperty(), 0, new byte[0], hashFunction, (SirixDeweyID) null, false, null);
    this.reusableObjectNullNode =
        new ObjectNullNode(0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber, 0, hashFunction, (SirixDeweyID) null);
    this.reusableObjectBooleanNode = new ObjectBooleanNode(0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber, 0,
        false, hashFunction, (SirixDeweyID) null);
    this.reusableObjectNumberNode = new ObjectNumberNode(0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber, 0, 0,
        hashFunction, (SirixDeweyID) null);
    this.reusableObjectStringNode = new ObjectStringNode(0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber, 0,
        new byte[0], hashFunction, (SirixDeweyID) null, false, null);
  }

  private long nextNodeKey() {
    return storageEngineWriter.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;
  }

  private ObjectNode bindObjectNode(final long nodeKey, final long parentKey, final long leftSibKey,
      final long rightSibKey, final SirixDeweyID id) {
    final ObjectNode node = reusableObjectNode;
    node.setNodeKey(nodeKey);
    node.setParentKey(parentKey);
    node.setPreviousRevision(Constants.NULL_REVISION_NUMBER);
    node.setLastModifiedRevision(revisionNumber);
    node.setRightSiblingKey(rightSibKey);
    node.setLeftSiblingKey(leftSibKey);
    node.setFirstChildKey(Fixed.NULL_NODE_KEY.getStandardProperty());
    node.setLastChildKey(Fixed.NULL_NODE_KEY.getStandardProperty());
    node.setChildCount(0);
    node.setDescendantCount(0);
    node.setHash(0);
    node.setDeweyID(id);
    return node;
  }

  private ArrayNode bindArrayNode(final long nodeKey, final long parentKey, final long leftSibKey,
      final long rightSibKey, final long pathNodeKey, final SirixDeweyID id) {
    final ArrayNode node = reusableArrayNode;
    node.setNodeKey(nodeKey);
    node.setParentKey(parentKey);
    node.setPathNodeKey(pathNodeKey);
    node.setPreviousRevision(Constants.NULL_REVISION_NUMBER);
    node.setLastModifiedRevision(revisionNumber);
    node.setRightSiblingKey(rightSibKey);
    node.setLeftSiblingKey(leftSibKey);
    node.setFirstChildKey(Fixed.NULL_NODE_KEY.getStandardProperty());
    node.setLastChildKey(Fixed.NULL_NODE_KEY.getStandardProperty());
    node.setChildCount(0);
    node.setDescendantCount(0);
    node.setHash(0);
    node.setDeweyID(id);
    return node;
  }

  private ObjectKeyNode bindObjectKeyNode(final long nodeKey, final long parentKey, final long leftSibKey,
      final long rightSibKey, final long pathNodeKey, final int nameKey, final String name, final long objectValueKey,
      final SirixDeweyID id) {
    final ObjectKeyNode node = reusableObjectKeyNode;
    node.setNodeKey(nodeKey);
    node.setParentKey(parentKey);
    node.setPathNodeKey(pathNodeKey);
    node.setPreviousRevision(Constants.NULL_REVISION_NUMBER);
    node.setLastModifiedRevision(revisionNumber);
    node.setRightSiblingKey(rightSibKey);
    node.setLeftSiblingKey(leftSibKey);
    node.setFirstChildKey(objectValueKey);
    node.setNameKey(nameKey);
    node.setName(name);
    node.setDescendantCount(0);
    node.setHash(0);
    node.setDeweyID(id);
    return node;
  }

  private NullNode bindNullNode(final long nodeKey, final long parentKey, final long leftSibKey, final long rightSibKey,
      final SirixDeweyID id) {
    final NullNode node = reusableNullNode;
    node.setNodeKey(nodeKey);
    node.setParentKey(parentKey);
    node.setPreviousRevision(Constants.NULL_REVISION_NUMBER);
    node.setLastModifiedRevision(revisionNumber);
    node.setRightSiblingKey(rightSibKey);
    node.setLeftSiblingKey(leftSibKey);
    node.setHash(0);
    node.setDeweyID(id);
    return node;
  }

  private BooleanNode bindBooleanNode(final long nodeKey, final long parentKey, final long leftSibKey,
      final long rightSibKey, final boolean boolValue, final SirixDeweyID id) {
    final BooleanNode node = reusableBooleanNode;
    node.setNodeKey(nodeKey);
    node.setParentKey(parentKey);
    node.setPreviousRevision(Constants.NULL_REVISION_NUMBER);
    node.setLastModifiedRevision(revisionNumber);
    node.setRightSiblingKey(rightSibKey);
    node.setLeftSiblingKey(leftSibKey);
    node.setHash(0);
    node.setValue(boolValue);
    node.setDeweyID(id);
    return node;
  }

  private NumberNode bindNumberNode(final long nodeKey, final long parentKey, final long leftSibKey,
      final long rightSibKey, final Number value, final SirixDeweyID id) {
    final NumberNode node = reusableNumberNode;
    node.setNodeKey(nodeKey);
    node.setParentKey(parentKey);
    node.setPreviousRevision(Constants.NULL_REVISION_NUMBER);
    node.setLastModifiedRevision(revisionNumber);
    node.setRightSiblingKey(rightSibKey);
    node.setLeftSiblingKey(leftSibKey);
    node.setHash(0);
    node.setValue(value);
    node.setDeweyID(id);
    return node;
  }

  private StringNode bindStringNode(final long nodeKey, final long parentKey, final long leftSibKey,
      final long rightSibKey, final byte[] value, final boolean isCompressed, final byte[] fsstSymbolTable,
      final SirixDeweyID id) {
    final StringNode node = reusableStringNode;
    node.setNodeKey(nodeKey);
    node.setParentKey(parentKey);
    node.setPreviousRevision(Constants.NULL_REVISION_NUMBER);
    node.setLastModifiedRevision(revisionNumber);
    node.setRightSiblingKey(rightSibKey);
    node.setLeftSiblingKey(leftSibKey);
    node.setHash(0);
    node.setRawValue(value, isCompressed, fsstSymbolTable);
    node.setDeweyID(id);
    return node;
  }

  private ObjectNullNode bindObjectNullNode(final long nodeKey, final long parentKey, final SirixDeweyID id) {
    final ObjectNullNode node = reusableObjectNullNode;
    node.setNodeKey(nodeKey);
    node.setParentKey(parentKey);
    node.setPreviousRevision(Constants.NULL_REVISION_NUMBER);
    node.setLastModifiedRevision(revisionNumber);
    node.setHash(0);
    node.setDeweyID(id);
    return node;
  }

  private ObjectBooleanNode bindObjectBooleanNode(final long nodeKey, final long parentKey, final boolean boolValue,
      final SirixDeweyID id) {
    final ObjectBooleanNode node = reusableObjectBooleanNode;
    node.setNodeKey(nodeKey);
    node.setParentKey(parentKey);
    node.setPreviousRevision(Constants.NULL_REVISION_NUMBER);
    node.setLastModifiedRevision(revisionNumber);
    node.setHash(0);
    node.setValue(boolValue);
    node.setDeweyID(id);
    return node;
  }

  private ObjectNumberNode bindObjectNumberNode(final long nodeKey, final long parentKey, final Number value,
      final SirixDeweyID id) {
    final ObjectNumberNode node = reusableObjectNumberNode;
    node.setNodeKey(nodeKey);
    node.setParentKey(parentKey);
    node.setPreviousRevision(Constants.NULL_REVISION_NUMBER);
    node.setLastModifiedRevision(revisionNumber);
    node.setHash(0);
    node.setValue(value);
    node.setDeweyID(id);
    return node;
  }

  private ObjectStringNode bindObjectStringNode(final long nodeKey, final long parentKey, final byte[] value,
      final boolean isCompressed, final byte[] fsstSymbolTable, final SirixDeweyID id) {
    final ObjectStringNode node = reusableObjectStringNode;
    node.setNodeKey(nodeKey);
    node.setParentKey(parentKey);
    node.setPreviousRevision(Constants.NULL_REVISION_NUMBER);
    node.setLastModifiedRevision(revisionNumber);
    node.setHash(0);
    node.setRawValue(value, isCompressed, fsstSymbolTable);
    node.setDeweyID(id);
    return node;
  }

  @Override
  public PathNode createPathNode(final @NonNegative long parentKey, final long leftSibKey, final long rightSibKey,
      @NonNull final QNm name, @NonNull final NodeKind kind, final @NonNegative int level) {
    final int uriKey = -1;
    final int prefixKey = -1;
    final int localName = name.getLocalName() != null && !name.getLocalName().isEmpty()
        ? NamePageHash.generateHashForString(name.getLocalName())
        : -1;

    // CRITICAL FIX: Use accessor method instead of direct .getPage() call
    // After TIL.put(), PageReference.getPage() returns null
    // Must use storageEngineWriter.getPathSummaryPage() which handles TIL lookups
    final PathSummaryPage pathSummaryPage = storageEngineWriter.getPathSummaryPage(storageEngineWriter.getActualRevisionRootPage());
    final NodeDelegate nodeDel = new NodeDelegate(pathSummaryPage.getMaxNodeKey(0) + 1, parentKey, hashFunction,
        Constants.NULL_REVISION_NUMBER, revisionNumber, (SirixDeweyID) null);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, uriKey, prefixKey, localName, 0);

    return storageEngineWriter.createRecord(new PathNode(name, nodeDel, structDel, nameDel, kind, 1, level), IndexType.PATH_SUMMARY,
        0);
  }

  @Override
  public ArrayNode createJsonArrayNode(long parentKey, long leftSibKey, long rightSibKey, long pathNodeKey,
      SirixDeweyID id) {
    final long nodeKey = nextNodeKey();
    final ArrayNode node = bindArrayNode(nodeKey, parentKey, leftSibKey, rightSibKey, pathNodeKey, id);
    return storageEngineWriter.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public ObjectNode createJsonObjectNode(long parentKey, long leftSibKey, long rightSibKey, SirixDeweyID id) {
    final long nodeKey = nextNodeKey();
    final ObjectNode node = bindObjectNode(nodeKey, parentKey, leftSibKey, rightSibKey, id);
    return storageEngineWriter.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public NullNode createJsonNullNode(long parentKey, long leftSibKey, long rightSibKey, SirixDeweyID id) {
    final long nodeKey = nextNodeKey();
    final NullNode node = bindNullNode(nodeKey, parentKey, leftSibKey, rightSibKey, id);
    return storageEngineWriter.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public ObjectKeyNode createJsonObjectKeyNode(long parentKey, long leftSibKey, long rightSibKey, long pathNodeKey,
      String name, long objectValueKey, SirixDeweyID id) {
    final int localNameKey = storageEngineWriter.createNameKey(name, NodeKind.OBJECT_KEY);
    final long nodeKey = nextNodeKey();
    final ObjectKeyNode node = bindObjectKeyNode(nodeKey, parentKey, leftSibKey, rightSibKey, pathNodeKey, localNameKey,
        name, objectValueKey, id);
    return storageEngineWriter.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public StringNode createJsonStringNode(long parentKey, long leftSibKey, long rightSibKey, byte[] value,
      boolean doCompress, SirixDeweyID id) {
    final long nodeKey = nextNodeKey();

    // For FSST, page-level symbol tables are required for decoding.
    // Until symbol table plumbing is complete, keep stored values uncompressed.
    final ResourceConfiguration config = storageEngineWriter.getResourceSession().getResourceConfig();
    final boolean shouldUseCompression = doCompress && config.stringCompressionType == StringCompressionType.FSST;
    if (shouldUseCompression) {
      // Intentionally not compressing here: no symbol table is available on this path yet.
    }
    final boolean isCompressed = false;

    final StringNode node = bindStringNode(nodeKey, parentKey, leftSibKey, rightSibKey, value, isCompressed, null, id);
    return storageEngineWriter.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public BooleanNode createJsonBooleanNode(long parentKey, long leftSibKey, long rightSibKey, boolean boolValue,
      SirixDeweyID id) {
    final long nodeKey = nextNodeKey();
    final BooleanNode node = bindBooleanNode(nodeKey, parentKey, leftSibKey, rightSibKey, boolValue, id);
    return storageEngineWriter.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public NumberNode createJsonNumberNode(long parentKey, long leftSibKey, long rightSibKey, Number value,
      SirixDeweyID id) {
    final long nodeKey = nextNodeKey();
    final NumberNode node = bindNumberNode(nodeKey, parentKey, leftSibKey, rightSibKey, value, id);
    return storageEngineWriter.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public ObjectNullNode createJsonObjectNullNode(long parentKey, SirixDeweyID id) {
    final long nodeKey = nextNodeKey();
    final ObjectNullNode node = bindObjectNullNode(nodeKey, parentKey, id);
    return storageEngineWriter.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public ObjectStringNode createJsonObjectStringNode(long parentKey, byte[] value, boolean doCompress,
      SirixDeweyID id) {
    final long nodeKey = nextNodeKey();

    // For FSST, page-level symbol tables are required for decoding.
    // Until symbol table plumbing is complete, keep stored values uncompressed.
    final ResourceConfiguration config = storageEngineWriter.getResourceSession().getResourceConfig();
    final boolean shouldUseCompression = doCompress && config.stringCompressionType == StringCompressionType.FSST;
    if (shouldUseCompression) {
      // Intentionally not compressing here: no symbol table is available on this path yet.
    }
    final boolean isCompressed = false;

    final ObjectStringNode node = bindObjectStringNode(nodeKey, parentKey, value, isCompressed, null, id);
    return storageEngineWriter.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public ObjectBooleanNode createJsonObjectBooleanNode(long parentKey, boolean boolValue, SirixDeweyID id) {
    final long nodeKey = nextNodeKey();
    final ObjectBooleanNode node = bindObjectBooleanNode(nodeKey, parentKey, boolValue, id);
    return storageEngineWriter.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public ObjectNumberNode createJsonObjectNumberNode(long parentKey, Number value, SirixDeweyID id) {
    final long nodeKey = nextNodeKey();
    final ObjectNumberNode node = bindObjectNumberNode(nodeKey, parentKey, value, id);
    return storageEngineWriter.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public DeweyIDNode createDeweyIdNode(long nodeKey, @NonNull SirixDeweyID id) {
    return storageEngineWriter.createRecord(new DeweyIDNode(nodeKey, id), IndexType.DEWEYID_TO_RECORDID, 0);
  }
}
