package org.sirix.access.trx.node.json;

import com.google.common.hash.HashFunction;
import org.brackit.xquery.atomic.QNm;
import org.sirix.api.PageTrx;
import org.sirix.index.IndexType;
import org.sirix.index.path.summary.PathNode;
import org.sirix.node.DeweyIDNode;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.delegates.ValueNodeDelegate;
import org.sirix.node.json.*;
import org.sirix.page.PathSummaryPage;
import org.sirix.settings.Fixed;
import org.sirix.utils.Compression;
import org.sirix.utils.NamePageHash;

import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import java.util.zip.Deflater;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Node factory to create nodes.
 *
 * @author Johannes Lichtenberger
 */
final class JsonNodeFactoryImpl implements JsonNodeFactory {

  /**
   * Hash function used to hash nodes.
   */
  private final HashFunction hashFunction;

  /**
   * {@link PageTrx} implementation.
   */
  private final PageTrx pageTrx;

  /**
   * Constructor.
   *
   * @param hashFunction hash function used to hash nodes
   * @param pageTrx      {@link PageTrx} implementation
   */
  JsonNodeFactoryImpl(final HashFunction hashFunction, final PageTrx pageTrx) {
    this.hashFunction = checkNotNull(hashFunction);
    this.pageTrx = checkNotNull(pageTrx);
  }

  @Override
  public PathNode createPathNode(final @NonNegative long parentKey, final long leftSibKey, final long rightSibKey,
      @NonNull final QNm name, @NonNull final NodeKind kind, final @NonNegative int level) {
    final int uriKey = -1;
    final int prefixKey = -1;
    final int localName = name.getLocalName() != null && !name.getLocalName().isEmpty()
        ? NamePageHash.generateHashForString(name.getLocalName())
        : -1;

    final long revision = pageTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(
        ((PathSummaryPage) pageTrx.getActualRevisionRootPage().getPathSummaryPageReference().getPage()).getMaxNodeKey(0)
            + 1, parentKey, hashFunction, null, revision, (SirixDeweyID) null);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                rightSibKey,
                                                                leftSibKey,
                                                                0,
                                                                0);
    final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, uriKey, prefixKey, localName, 0);

    return pageTrx.createRecord(nodeDel.getNodeKey(),
                                new PathNode(name, nodeDel, structDel, nameDel, kind, 1, level),
                                IndexType.PATH_SUMMARY,
                                0);
  }

  @Override
  public ArrayNode createJsonArrayNode(long parentKey, long leftSibKey, long rightSibKey, long pathNodeKey,
      SirixDeweyID id) {
    final long revision = pageTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1,
                                                  parentKey,
                                                  hashFunction,
                                                  null,
                                                  revision,
                                                  id);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                rightSibKey,
                                                                leftSibKey,
                                                                0,
                                                                0);
    return pageTrx.createRecord(nodeDel.getNodeKey(), new ArrayNode(structDel, pathNodeKey), IndexType.DOCUMENT, -1);
  }

  @Override
  public ObjectNode createJsonObjectNode(long parentKey, long leftSibKey, long rightSibKey, SirixDeweyID id) {
    final long revision = pageTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1,
                                                  parentKey,
                                                  hashFunction,
                                                  null,
                                                  revision,
                                                  id);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                rightSibKey,
                                                                leftSibKey,
                                                                0,
                                                                0);
    return pageTrx.createRecord(nodeDel.getNodeKey(), new ObjectNode(structDel), IndexType.DOCUMENT, -1);
  }

  @Override
  public NullNode createJsonNullNode(long parentKey, long leftSibKey, long rightSibKey, SirixDeweyID id) {
    final long revision = pageTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1,
                                                  parentKey,
                                                  hashFunction,
                                                  null,
                                                  revision,
                                                  id);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                rightSibKey,
                                                                leftSibKey,
                                                                0,
                                                                0);
    return pageTrx.createRecord(nodeDel.getNodeKey(), new NullNode(structDel), IndexType.DOCUMENT, -1);
  }

  @Override
  public ObjectKeyNode createJsonObjectKeyNode(long parentKey, long leftSibKey, long rightSibKey, long pathNodeKey,
      String name, long objectValueKey, SirixDeweyID id) {
    final int localNameKey = pageTrx.createNameKey(name, NodeKind.OBJECT_KEY);
    final long revision = pageTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1,
                                                  parentKey,
                                                  hashFunction,
                                                  null,
                                                  revision,
                                                  id);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
                                                                objectValueKey,
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                rightSibKey,
                                                                leftSibKey,
                                                                0,
                                                                0);
    return pageTrx.createRecord(nodeDel.getNodeKey(),
                                new ObjectKeyNode(structDel, localNameKey, name, pathNodeKey),
                                IndexType.DOCUMENT,
                                -1);
  }

  @Override
  public StringNode createJsonStringNode(long parentKey, long leftSibKey, long rightSibKey, byte[] value,
      boolean doCompress, SirixDeweyID id) {
    final long revision = pageTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1,
                                                  parentKey,
                                                  hashFunction,
                                                  null,
                                                  revision,
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
    return pageTrx.createRecord(nodeDel.getNodeKey(), new StringNode(valDel, structDel), IndexType.DOCUMENT, -1);
  }

  @Override
  public BooleanNode createJsonBooleanNode(long parentKey, long leftSibKey, long rightSibKey, boolean boolValue,
      SirixDeweyID id) {
    final long revision = pageTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1,
                                                  parentKey,
                                                  hashFunction,
                                                  null,
                                                  revision,
                                                  id);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                rightSibKey,
                                                                leftSibKey,
                                                                0,
                                                                0);
    return pageTrx.createRecord(nodeDel.getNodeKey(), new BooleanNode(boolValue, structDel), IndexType.DOCUMENT, -1);
  }

  @Override
  public NumberNode createJsonNumberNode(long parentKey, long leftSibKey, long rightSibKey, Number value,
      SirixDeweyID id) {
    final long revision = pageTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1,
                                                  parentKey,
                                                  hashFunction,
                                                  null,
                                                  revision,
                                                  id);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                rightSibKey,
                                                                leftSibKey,
                                                                0,
                                                                0);
    return pageTrx.createRecord(nodeDel.getNodeKey(), new NumberNode(value, structDel), IndexType.DOCUMENT, -1);
  }

  @Override
  public ObjectNullNode createJsonObjectNullNode(long parentKey, SirixDeweyID id) {
    final long revision = pageTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1,
                                                  parentKey,
                                                  hashFunction,
                                                  null,
                                                  revision,
                                                  id);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                0,
                                                                0);
    return pageTrx.createRecord(nodeDel.getNodeKey(), new ObjectNullNode(structDel), IndexType.DOCUMENT, -1);
  }

  @Override
  public ObjectStringNode createJsonObjectStringNode(long parentKey, byte[] value, boolean doCompress,
      SirixDeweyID id) {
    final long revision = pageTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1,
                                                  parentKey,
                                                  hashFunction,
                                                  null,
                                                  revision,
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
    return pageTrx.createRecord(nodeDel.getNodeKey(), new ObjectStringNode(valDel, structDel), IndexType.DOCUMENT, -1);
  }

  @Override
  public ObjectBooleanNode createJsonObjectBooleanNode(long parentKey, boolean boolValue, SirixDeweyID id) {
    final long revision = pageTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1,
                                                  parentKey,
                                                  hashFunction,
                                                  null,
                                                  revision,
                                                  id);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                0,
                                                                0);
    return pageTrx.createRecord(nodeDel.getNodeKey(),
                                new ObjectBooleanNode(boolValue, structDel),
                                IndexType.DOCUMENT,
                                -1);
  }

  @Override
  public ObjectNumberNode createJsonObjectNumberNode(long parentKey, Number value, SirixDeweyID id) {
    final long revision = pageTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1,
                                                  parentKey,
                                                  hashFunction,
                                                  null,
                                                  revision,
                                                  id);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                0,
                                                                0);
    return pageTrx.createRecord(nodeDel.getNodeKey(), new ObjectNumberNode(value, structDel), IndexType.DOCUMENT, -1);
  }

  @Override
  public DeweyIDNode createDeweyIdNode(long nodeKey, @NonNull SirixDeweyID id) {
    return pageTrx.createRecord(nodeKey, new DeweyIDNode(nodeKey, id), IndexType.DEWEYID_TO_RECORDID, 0);
  }
}
