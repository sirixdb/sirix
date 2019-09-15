package org.sirix.access.trx.node.json;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.zip.Deflater;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.brackit.xquery.atomic.QNm;
import org.sirix.api.PageTrx;
import org.sirix.index.path.summary.PathNode;
import org.sirix.node.NodeKind;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.delegates.ValueNodeDelegate;
import org.sirix.node.interfaces.Record;
import org.sirix.node.json.ArrayNode;
import org.sirix.node.json.BooleanNode;
import org.sirix.node.json.NullNode;
import org.sirix.node.json.NumberNode;
import org.sirix.node.json.ObjectKeyNode;
import org.sirix.node.json.ObjectNode;
import org.sirix.node.json.StringNode;
import org.sirix.page.PageKind;
import org.sirix.page.PathSummaryPage;
import org.sirix.page.UnorderedKeyValuePage;
import org.sirix.settings.Fixed;
import org.sirix.utils.Compression;
import org.sirix.utils.NamePageHash;
import com.google.common.hash.HashFunction;

/**
 * Node factory to create nodes.
 *
 * @author Johannes Lichtenberger
 *
 */
final class JsonNodeFactoryImpl implements JsonNodeFactory {

  /** Hash function used to hash nodes. */
  private final HashFunction mHashFunction;

  /** {@link PageTrx} implementation. */
  private final PageTrx<Long, Record, UnorderedKeyValuePage> mPageWriteTrx;

  /**
   * Constructor.
   *
   * @param hashFunction hash function used to hash nodes
   * @param pageWriteTrx {@link PageTrx} implementation
   */
  JsonNodeFactoryImpl(final HashFunction hashFunction,
      final PageTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx) {
    mHashFunction = checkNotNull(hashFunction);
    mPageWriteTrx = checkNotNull(pageWriteTrx);
    // pageWriteTrx.createNameKey("array", Kind.ARRAY);
    // pageWriteTrx.createNameKey("object", Kind.ARRAY);
  }

  @Override
  public PathNode createPathNode(final @Nonnegative long parentKey, final long leftSibKey, final long rightSibKey,
      @Nonnull final QNm name, @Nonnull final NodeKind kind, final @Nonnegative int level) {
    final int uriKey = -1;
    final int prefixKey = -1;
    final int localName = name.getLocalName() != null && !name.getLocalName().isEmpty()
        ? NamePageHash.generateHashForString(name.getLocalName())
        : -1;

    final long revision = mPageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(
        ((PathSummaryPage) mPageWriteTrx.getActualRevisionRootPage()
                                        .getPathSummaryPageReference()
                                        .getPage()).getMaxNodeKey(0)
            + 1,
        parentKey, mHashFunction, null, revision, null);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, uriKey, prefixKey, localName, 0);

    return (PathNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(),
        new PathNode(nodeDel, structDel, nameDel, kind, 1, level), PageKind.PATHSUMMARYPAGE, 0);
  }

  @Override
  public ArrayNode createJsonArrayNode(long parentKey, long leftSibKey, long rightSibKey, long pathNodeKey) {
    final long revision = mPageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(mPageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1,
        parentKey, mHashFunction, null, revision, null);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    return (ArrayNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(), new ArrayNode(structDel, pathNodeKey),
        PageKind.RECORDPAGE, -1);
  }

  @Override
  public ObjectNode createJsonObjectNode(long parentKey, long leftSibKey, long rightSibKey) {
    final long revision = mPageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(mPageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1,
        parentKey, mHashFunction, null, revision, null);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    return (ObjectNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(), new ObjectNode(structDel), PageKind.RECORDPAGE,
        -1);
  }

  @Override
  public NullNode createJsonNullNode(long parentKey, long leftSibKey, long rightSibKey) {
    final long revision = mPageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(mPageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1,
        parentKey, mHashFunction, null, revision, null);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    return (NullNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(), new NullNode(structDel), PageKind.RECORDPAGE, -1);
  }

  @Override
  public ObjectKeyNode createJsonObjectKeyNode(long parentKey, long leftSibKey, long rightSibKey, long pathNodeKey,
      String name, long objectValueKey) {
    final int localNameKey = mPageWriteTrx.createNameKey(name, NodeKind.OBJECT_KEY);
    final long revision = mPageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(mPageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1,
        parentKey, mHashFunction, null, revision, null);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel, objectValueKey, rightSibKey, leftSibKey, 0, 0);
    return (ObjectKeyNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(),
        new ObjectKeyNode(structDel, localNameKey, name, pathNodeKey), PageKind.RECORDPAGE, -1);
  }

  @Override
  public StringNode createJsonStringNode(long parentKey, long leftSibKey, long rightSibKey, byte[] value,
      boolean doCompress) {
    final long revision = mPageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(mPageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1,
        parentKey, mHashFunction, null, revision, null);
    final boolean compression = doCompress && value.length > 10;
    final byte[] compressedValue = compression
        ? Compression.compress(value, Deflater.HUFFMAN_ONLY)
        : value;
    final ValueNodeDelegate valDel = new ValueNodeDelegate(nodeDel, compressedValue, compression);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    return (StringNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(), new StringNode(valDel, structDel),
        PageKind.RECORDPAGE, -1);
  }

  @Override
  public BooleanNode createJsonBooleanNode(long parentKey, long leftSibKey, long rightSibKey, boolean boolValue) {
    final long revision = mPageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(mPageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1,
        parentKey, mHashFunction, null, revision, null);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    return (BooleanNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(), new BooleanNode(boolValue, structDel),
        PageKind.RECORDPAGE, -1);
  }

  @Override
  public NumberNode createJsonNumberNode(long parentKey, long leftSibKey, long rightSibKey, Number value) {
    final long revision = mPageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(mPageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1,
        parentKey, mHashFunction, null, revision, null);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    return (NumberNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(), new NumberNode(value, structDel),
        PageKind.RECORDPAGE, -1);
  }
}
