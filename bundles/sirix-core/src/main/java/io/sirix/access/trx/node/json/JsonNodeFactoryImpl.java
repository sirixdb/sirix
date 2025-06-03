package io.sirix.access.trx.node.json;

import io.sirix.api.PageTrx;
import io.sirix.index.IndexType;
import io.sirix.index.path.summary.PathNode;
import io.sirix.node.DeweyIDNode;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.delegates.NameNodeDelegate;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.delegates.StructNodeDelegate;
import io.sirix.node.delegates.ValueNodeDelegate;
import io.sirix.node.json.*;
import io.sirix.page.PathSummaryPage;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import io.sirix.utils.Compression;
import io.sirix.utils.NamePageHash;
import net.openhft.hashing.LongHashFunction;
import io.brackit.query.atomic.QNm;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.zip.Deflater;

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
   * {@link PageTrx} implementation.
   */
  private final PageTrx pageTrx;

  /**
   * The current revision number.
   */
  private final int revisionNumber;

  /**
   * Constructor.
   *
   * @param hashFunction hash function used to hash nodes
   * @param pageTrx      {@link PageTrx} implementation
   */
  JsonNodeFactoryImpl(final LongHashFunction hashFunction, final PageTrx pageTrx) {
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

    final NodeDelegate nodeDel = new NodeDelegate(
        ((PathSummaryPage) pageTrx.getActualRevisionRootPage().getPathSummaryPageReference().getPage()).getMaxNodeKey(0)
            + 1, parentKey, hashFunction, Constants.NULL_REVISION_NUMBER, revisionNumber, (SirixDeweyID) null);
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


    final NodeDelegate nodeDel =
        new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1,
                         parentKey,
                         hashFunction, Constants.NULL_REVISION_NUMBER,
                         revisionNumber,
                         id);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                rightSibKey,
                                                                leftSibKey,
                                                                0,
                                                                0);
    return pageTrx.createRecord(new ArrayNode(structDel, pathNodeKey), IndexType.DOCUMENT, -1);

//    return pageTrx.createRecord(new ArrayNode(nodeKey,
//                                              parentKey,
//                                              Fixed.NULL_NODE_KEY.getStandardProperty(),
//                                              Fixed.NULL_NODE_KEY.getStandardProperty(),
//                                              rightSibKey,
//                                              leftSibKey,
//                                              hashFunction,
//                                              pathNodeKey,
//                                              null,
//                                              Constants.NULL_REVISION_NUMBER,
//                                              revisionNumber,
//
//                                              ), IndexType.DOCUMENT, -1);
  }

  @Override
  public ObjectNode createJsonObjectNode(long parentKey, long leftSibKey, long rightSibKey, SirixDeweyID id) {
    final NodeDelegate nodeDel =
        new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1,
                         parentKey,
                         hashFunction, Constants.NULL_REVISION_NUMBER,
                         revisionNumber,
                         id);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                rightSibKey,
                                                                leftSibKey,
                                                                0,
                                                                0);
    return pageTrx.createRecord(new ObjectNode(structDel), IndexType.DOCUMENT, -1);
  }

  @Override
  public NullNode createJsonNullNode(long parentKey, long leftSibKey, long rightSibKey, SirixDeweyID id) {
    final NodeDelegate nodeDel =
        new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1,
                         parentKey,
                         hashFunction, Constants.NULL_REVISION_NUMBER,
                         revisionNumber,
                         id);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                rightSibKey,
                                                                leftSibKey,
                                                                0,
                                                                0);
    return pageTrx.createRecord(new NullNode(structDel), IndexType.DOCUMENT, -1);
  }

  @Override
  public ObjectKeyNode createJsonObjectKeyNode(long parentKey, long leftSibKey, long rightSibKey, long pathNodeKey,
      String name, long objectValueKey, SirixDeweyID id) {
    final int localNameKey = pageTrx.createNameKey(name, NodeKind.OBJECT_KEY);
    final NodeDelegate nodeDel =
        new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1,
                         parentKey,
                         hashFunction, Constants.NULL_REVISION_NUMBER,
                         revisionNumber,
                         id);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
                                                                objectValueKey,
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                rightSibKey,
                                                                leftSibKey,
                                                                0,
                                                                0);
    return pageTrx.createRecord(new ObjectKeyNode(structDel, localNameKey, name, pathNodeKey),
                                IndexType.DOCUMENT,
                                -1);
  }

  @Override
  public StringNode createJsonStringNode(long parentKey, long leftSibKey, long rightSibKey, byte[] value,
      boolean doCompress, SirixDeweyID id) {
    final NodeDelegate nodeDel =
        new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1,
                         parentKey,
                         hashFunction, Constants.NULL_REVISION_NUMBER,
                         revisionNumber,
                         id);
    final boolean compression = doCompress && value.length > 10;
    final byte[] compressedValue = compression ? Compression.compress(value, Deflater.HUFFMAN_ONLY) : value;
    final ValueNodeDelegate valDel = new ValueNodeDelegate(nodeDel, compressedValue, compression);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                rightSibKey,
                                                                leftSibKey,
                                                                0,
                                                                0);
    return pageTrx.createRecord(new StringNode(valDel, structDel), IndexType.DOCUMENT, -1);
  }

  @Override
  public BooleanNode createJsonBooleanNode(long parentKey, long leftSibKey, long rightSibKey, boolean boolValue,
      SirixDeweyID id) {
    final NodeDelegate nodeDel =
        new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1,
                         parentKey,
                         hashFunction, Constants.NULL_REVISION_NUMBER,
                         revisionNumber,
                         id);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                rightSibKey,
                                                                leftSibKey,
                                                                0,
                                                                0);
    return pageTrx.createRecord(new BooleanNode(boolValue, structDel), IndexType.DOCUMENT, -1);
  }

  @Override
  public NumberNode createJsonNumberNode(long parentKey, long leftSibKey, long rightSibKey, Number value,
      SirixDeweyID id) {
    final NodeDelegate nodeDel =
        new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1,
                         parentKey,
                         hashFunction, Constants.NULL_REVISION_NUMBER,
                         revisionNumber,
                         id);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                rightSibKey,
                                                                leftSibKey,
                                                                0,
                                                                0);
    return pageTrx.createRecord(new NumberNode(value, structDel), IndexType.DOCUMENT, -1);
  }

  @Override
  public ObjectNullNode createJsonObjectNullNode(long parentKey, SirixDeweyID id) {
    final NodeDelegate nodeDel =
        new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1,
                         parentKey,
                         hashFunction, Constants.NULL_REVISION_NUMBER,
                         revisionNumber,
                         id);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                0,
                                                                0);
    return pageTrx.createRecord(new ObjectNullNode(structDel), IndexType.DOCUMENT, -1);
  }

  @Override
  public ObjectStringNode createJsonObjectStringNode(long parentKey, byte[] value, boolean doCompress,
      SirixDeweyID id) {
    final NodeDelegate nodeDel =
        new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1,
                         parentKey,
                         hashFunction, Constants.NULL_REVISION_NUMBER,
                         revisionNumber,
                         id);
    final boolean compression = doCompress && value.length > 40;
    final byte[] compressedValue = compression ? Compression.compress(value, Deflater.BEST_COMPRESSION) : value;
    final ValueNodeDelegate valDel = new ValueNodeDelegate(nodeDel, compressedValue, compression);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                0,
                                                                0);
    return pageTrx.createRecord(new ObjectStringNode(valDel, structDel), IndexType.DOCUMENT, -1);
  }

  @Override
  public ObjectBooleanNode createJsonObjectBooleanNode(long parentKey, boolean boolValue, SirixDeweyID id) {
    final NodeDelegate nodeDel =
        new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1,
                         parentKey,
                         hashFunction, Constants.NULL_REVISION_NUMBER,
                         revisionNumber,
                         id);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                0,
                                                                0);
    return pageTrx.createRecord(new ObjectBooleanNode(boolValue, structDel),
                                IndexType.DOCUMENT,
                                -1);
  }

  @Override
  public ObjectNumberNode createJsonObjectNumberNode(long parentKey, Number value, SirixDeweyID id) {
    final NodeDelegate nodeDel =
        new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1,
                         parentKey,
                         hashFunction, Constants.NULL_REVISION_NUMBER,
                         revisionNumber,
                         id);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                0,
                                                                0);
    return pageTrx.createRecord(new ObjectNumberNode(value, structDel), IndexType.DOCUMENT, -1);
  }

  @Override
  public DeweyIDNode createDeweyIdNode(long nodeKey, @NonNull SirixDeweyID id) {
    return pageTrx.createRecord(new DeweyIDNode(nodeKey, id), IndexType.DEWEYID_TO_RECORDID, 0);
  }
}
