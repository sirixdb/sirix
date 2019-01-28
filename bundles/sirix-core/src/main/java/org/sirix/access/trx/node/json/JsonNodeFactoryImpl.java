package org.sirix.access.trx.node.json;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.zip.Deflater;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.brackit.xquery.atomic.QNm;
import org.sirix.api.PageWriteTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.index.path.summary.PathNode;
import org.sirix.node.Kind;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.delegates.ValNodeDelegate;
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

/**
 * Node factory to create nodes.
 *
 * @author Johannes Lichtenberger
 *
 */
final class JsonNodeFactoryImpl implements JsonNodeFactory {

  /** {@link PageWriteTrx} implementation. */
  private final PageWriteTrx<Long, Record, UnorderedKeyValuePage> mPageWriteTrx;

  /**
   * Constructor.
   *
   * @param pageWriteTrx {@link PageWriteTrx} implementation
   * @throws SirixIOException if an I/O exception occured due to name key creation
   */
  JsonNodeFactoryImpl(final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx) {
    mPageWriteTrx = checkNotNull(pageWriteTrx);
    pageWriteTrx.createNameKey("array", Kind.JSON_ARRAY);
  }

  @Override
  public PathNode createPathNode(final @Nonnegative long parentKey, final long leftSibKey, final long rightSibKey,
      @Nonnull final QNm name, @Nonnull final Kind kind, final @Nonnegative int level) {
    final int uriKey = NamePageHash.generateHashForString(name.getNamespaceURI());
    final int prefixKey = name.getPrefix() != null && !name.getPrefix().isEmpty()
        ? NamePageHash.generateHashForString(name.getPrefix())
        : -1;
    final int localName = name.getLocalName() != null && !name.getLocalName().isEmpty()
        ? NamePageHash.generateHashForString(name.getLocalName())
        : -1;

    final long revision = mPageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(((PathSummaryPage) mPageWriteTrx.getActualRevisionRootPage()
                                                                                  .getPathSummaryPageReference()
                                                                                  .getPage()).getMaxNodeKey(0)
        + 1, parentKey, 0, revision, null);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, uriKey, prefixKey, localName, 0);

    return (PathNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(),
        new PathNode(nodeDel, structDel, nameDel, kind, 1, level), PageKind.PATHSUMMARYPAGE, 0);
  }

  @Override
  public ArrayNode createJsonArrayNode(long parentKey, long rightSibKey, long pathNodeKey) {
    final long revision = mPageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel =
        new NodeDelegate(mPageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1, parentKey, 0, revision, null);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(),
        rightSibKey, Fixed.NULL_NODE_KEY.getStandardProperty(), 0, 0);
    return (ArrayNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(), new ArrayNode(structDel, pathNodeKey),
        PageKind.RECORDPAGE, -1);
  }

  @Override
  public ObjectNode createJsonObjectNode(long parentKey, long rightSibKey) {
    final long revision = mPageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel =
        new NodeDelegate(mPageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1, parentKey, 0, revision, null);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(),
        rightSibKey, Fixed.NULL_NODE_KEY.getStandardProperty(), 0, 0);
    return (ObjectNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(), new ObjectNode(structDel), PageKind.RECORDPAGE,
        -1);
  }

  @Override
  public NullNode createJsonNullNode(long parentKey, long rightSibKey) {
    final long revision = mPageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel =
        new NodeDelegate(mPageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1, parentKey, 0, revision, null);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(),
        rightSibKey, Fixed.NULL_NODE_KEY.getStandardProperty(), 0, 0);
    return (NullNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(), new NullNode(structDel), PageKind.RECORDPAGE, -1);
  }

  @Override
  public ObjectKeyNode createJsonObjectKeyNode(long parentKey, long rightSibKey, long pathNodeKey, String name) {
    final int localNameKey = mPageWriteTrx.createNameKey(name, Kind.JSON_OBJECT_KEY);
    final long revision = mPageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel =
        new NodeDelegate(mPageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1, parentKey, 0, revision, null);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(),
        rightSibKey, Fixed.NULL_NODE_KEY.getStandardProperty(), 0, 0);
    return (ObjectKeyNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(),
        new ObjectKeyNode(structDel, localNameKey, name, pathNodeKey), PageKind.RECORDPAGE, -1);
  }

  @Override
  public StringNode createJsonStringNode(long parentKey, long rightSibKey, byte[] value, boolean doCompress) {
    final long revision = mPageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel =
        new NodeDelegate(mPageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1, parentKey, 0, revision, null);
    final boolean compression = doCompress && value.length > 10;
    final byte[] compressedValue = compression
        ? Compression.compress(value, Deflater.HUFFMAN_ONLY)
        : value;
    final ValNodeDelegate valDel = new ValNodeDelegate(nodeDel, compressedValue, compression);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(),
        rightSibKey, Fixed.NULL_NODE_KEY.getStandardProperty(), 0, 0);
    return (StringNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(), new StringNode(valDel, structDel),
        PageKind.RECORDPAGE, -1);
  }

  @Override
  public BooleanNode createJsonBooleanNode(long parentKey, long rightSibKey, boolean boolValue) {
    final long revision = mPageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel =
        new NodeDelegate(mPageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1, parentKey, 0, revision, null);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(),
        rightSibKey, Fixed.NULL_NODE_KEY.getStandardProperty(), 0, 0);
    return (BooleanNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(), new BooleanNode(boolValue, structDel),
        PageKind.RECORDPAGE, -1);
  }

  @Override
  public NumberNode createJsonNumberNode(long parentKey, long rightSibKey, double dblValue) {
    final long revision = mPageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel =
        new NodeDelegate(mPageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1, parentKey, 0, revision, null);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(),
        rightSibKey, Fixed.NULL_NODE_KEY.getStandardProperty(), 0, 0);
    return (NumberNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(), new NumberNode(dblValue, structDel),
        PageKind.RECORDPAGE, -1);
  }
}
