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
import io.sirix.node.json.*;
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
  private final StorageEngineWriter pageTrx;

  /**
   * The current revision number.
   */
  private final int revisionNumber;

  /**
   * Constructor.
   *
   * @param hashFunction hash function used to hash nodes
   * @param pageTrx      {@link StorageEngineWriter} implementation
   */
  JsonNodeFactoryImpl(final LongHashFunction hashFunction, final StorageEngineWriter pageTrx) {
    this.hashFunction = requireNonNull(hashFunction);
    this.pageTrx = requireNonNull(pageTrx);
    this.revisionNumber = pageTrx.getRevisionNumber();
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
    // Must use pageTrx.getPathSummaryPage() which handles TIL lookups
    final PathSummaryPage pathSummaryPage = pageTrx.getPathSummaryPage(pageTrx.getActualRevisionRootPage());
    final NodeDelegate nodeDel = new NodeDelegate(
        pathSummaryPage.getMaxNodeKey(0) + 1, 
        parentKey, hashFunction, Constants.NULL_REVISION_NUMBER, revisionNumber, (SirixDeweyID) null);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                rightSibKey,
                                                                leftSibKey,
                                                                0,
                                                                0);
    final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, uriKey, prefixKey, localName, 0);

    return pageTrx.createRecord(new PathNode(name, nodeDel, structDel, nameDel, kind, 1, level),
                                IndexType.PATH_SUMMARY,
                                0);
  }

  @Override
  public ArrayNode createJsonArrayNode(long parentKey, long leftSibKey, long rightSibKey, long pathNodeKey,
      SirixDeweyID id) {
    final long nodeKey = pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;
    
    // Create ArrayNode with primitive fields
    var node = new ArrayNode(
        nodeKey,
        parentKey,
        pathNodeKey,
        Constants.NULL_REVISION_NUMBER,
        revisionNumber,
        rightSibKey,
        leftSibKey,
        Fixed.NULL_NODE_KEY.getStandardProperty(), // firstChild
        Fixed.NULL_NODE_KEY.getStandardProperty(), // lastChild
        0, // childCount
        0, // descendantCount
        0, // hash
        hashFunction,
        id
    );
    
    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public ObjectNode createJsonObjectNode(long parentKey, long leftSibKey, long rightSibKey, SirixDeweyID id) {
    final long nodeKey = pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;
    
    // Create ObjectNode with primitive fields
    var node = new ObjectNode(
        nodeKey,
        parentKey,
        Constants.NULL_REVISION_NUMBER,
        revisionNumber,
        rightSibKey,
        leftSibKey,
        Fixed.NULL_NODE_KEY.getStandardProperty(), // firstChild
        Fixed.NULL_NODE_KEY.getStandardProperty(), // lastChild
        0, // childCount
        0, // descendantCount
        0, // hash
        hashFunction,
        id
    );
    
    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public NullNode createJsonNullNode(long parentKey, long leftSibKey, long rightSibKey, SirixDeweyID id) {
    final long nodeKey = pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;
    
    var node = new NullNode(
        nodeKey,
        parentKey,
        Constants.NULL_REVISION_NUMBER,
        revisionNumber,
        rightSibKey,
        leftSibKey,
        0, // hash
        hashFunction,
        id
    );
    
    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public ObjectKeyNode createJsonObjectKeyNode(long parentKey, long leftSibKey, long rightSibKey, long pathNodeKey,
      String name, long objectValueKey, SirixDeweyID id) {
    final int localNameKey = pageTrx.createNameKey(name, NodeKind.OBJECT_KEY);
    final long nodeKey = pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;
    
    var node = new ObjectKeyNode(
        nodeKey,
        parentKey,
        pathNodeKey,
        Constants.NULL_REVISION_NUMBER,
        revisionNumber,
        rightSibKey,
        leftSibKey,
        objectValueKey, // firstChild
        localNameKey,
        0, // descendantCount
        0, // hash
        hashFunction,
        id
    );
    
    // Set name for later retrieval (cached)
    node.setName(name);
    
    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public StringNode createJsonStringNode(long parentKey, long leftSibKey, long rightSibKey, byte[] value,
      boolean doCompress, SirixDeweyID id) {
    final long nodeKey = pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;
    
    // Check if FSST compression should be applied
    final ResourceConfiguration config = pageTrx.getResourceSession().getResourceConfig();
    final boolean useCompression = doCompress && config.stringCompressionType == StringCompressionType.FSST;
    
    // For FSST, we would ideally build a symbol table from page strings.
    // For now, we set the compression flag but keep the value as-is.
    // The page-level serialization will handle actual compression when
    // columnar storage is fully implemented.
    
    var node = new StringNode(
        nodeKey,
        parentKey,
        Constants.NULL_REVISION_NUMBER,
        revisionNumber,
        rightSibKey,
        leftSibKey,
        0, // hash
        value,
        hashFunction,
        id,
        false, // isCompressed - set to false since no page-level symbol table yet
        null   // fsstSymbolTable - will be set by page-level compression
    );
    
    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public BooleanNode createJsonBooleanNode(long parentKey, long leftSibKey, long rightSibKey, boolean boolValue,
      SirixDeweyID id) {
    final long nodeKey = pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;
    
    var node = new BooleanNode(
        nodeKey,
        parentKey,
        Constants.NULL_REVISION_NUMBER,
        revisionNumber,
        rightSibKey,
        leftSibKey,
        0, // hash
        boolValue,
        hashFunction,
        id
    );
    
    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public NumberNode createJsonNumberNode(long parentKey, long leftSibKey, long rightSibKey, Number value,
      SirixDeweyID id) {
    final long nodeKey = pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;
    
    var node = new NumberNode(
        nodeKey,
        parentKey,
        Constants.NULL_REVISION_NUMBER,
        revisionNumber,
        rightSibKey,
        leftSibKey,
        0, // hash
        value,
        hashFunction,
        id
    );
    
    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public ObjectNullNode createJsonObjectNullNode(long parentKey, SirixDeweyID id) {
    final long nodeKey = pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;
    
    var node = new ObjectNullNode(
        nodeKey,
        parentKey,
        Constants.NULL_REVISION_NUMBER,
        revisionNumber,
        0, // hash
        hashFunction,
        id
    );
    
    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public ObjectStringNode createJsonObjectStringNode(long parentKey, byte[] value, boolean doCompress,
      SirixDeweyID id) {
    final long nodeKey = pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;
    
    // Check if FSST compression should be applied
    final ResourceConfiguration config = pageTrx.getResourceSession().getResourceConfig();
    final boolean useCompression = doCompress && config.stringCompressionType == StringCompressionType.FSST;
    
    // For FSST, we would ideally build a symbol table from page strings.
    // For now, we set the compression flag but keep the value as-is.
    // The page-level serialization will handle actual compression when
    // columnar storage is fully implemented.
    
    var node = new ObjectStringNode(
        nodeKey,
        parentKey,
        Constants.NULL_REVISION_NUMBER,
        revisionNumber,
        0, // hash
        value,
        hashFunction,
        id,
        false, // isCompressed - set to false since no page-level symbol table yet
        null   // fsstSymbolTable - will be set by page-level compression
    );
    
    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public ObjectBooleanNode createJsonObjectBooleanNode(long parentKey, boolean boolValue, SirixDeweyID id) {
    final long nodeKey = pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;
    
    var node = new ObjectBooleanNode(
        nodeKey,
        parentKey,
        Constants.NULL_REVISION_NUMBER,
        revisionNumber,
        0, // hash
        boolValue,
        hashFunction,
        id
    );
    
    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public ObjectNumberNode createJsonObjectNumberNode(long parentKey, Number value, SirixDeweyID id) {
    final long nodeKey = pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;
    
    var node = new ObjectNumberNode(
        nodeKey,
        parentKey,
        Constants.NULL_REVISION_NUMBER,
        revisionNumber,
        0, // hash
        value,
        hashFunction,
        id
    );
    
    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public DeweyIDNode createDeweyIdNode(long nodeKey, @NonNull SirixDeweyID id) {
    return pageTrx.createRecord(new DeweyIDNode(nodeKey, id), IndexType.DEWEYID_TO_RECORDID, 0);
  }
}
