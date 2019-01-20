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
import org.sirix.node.json.JsonArrayNode;
import org.sirix.node.json.JsonBooleanNode;
import org.sirix.node.json.JsonNullNode;
import org.sirix.node.json.JsonNumberNode;
import org.sirix.node.json.JsonObjectKeyNode;
import org.sirix.node.json.JsonObjectNode;
import org.sirix.node.json.JsonStringNode;
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
    mPageWriteTrx.createNameKey("xs:untyped", Kind.ATTRIBUTE);
    mPageWriteTrx.createNameKey("xs:untyped", Kind.NAMESPACE);
    mPageWriteTrx.createNameKey("xs:untyped", Kind.ELEMENT);
    mPageWriteTrx.createNameKey("xs:untyped", Kind.PROCESSING_INSTRUCTION);
  }

  @Override
  public PathNode createPathNode(final @Nonnegative long parentKey, final @Nonnegative long leftSibKey,
      final long rightSibKey, @Nonnull final QNm name, @Nonnull final Kind kind, final @Nonnegative int level) {
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
  public JsonArrayNode createJsonArrayNode(long parentKey, long leftSibKey, long rightSibKey) {
    final long revision = mPageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel =
        new NodeDelegate(mPageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1, parentKey, 0, revision, null);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    return (JsonArrayNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(), new JsonArrayNode(structDel),
        PageKind.RECORDPAGE, -1);
  }

  @Override
  public JsonObjectNode createJsonObjectNode(long parentKey, long leftSibKey, long rightSibKey) {
    final long revision = mPageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel =
        new NodeDelegate(mPageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1, parentKey, 0, revision, null);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    return (JsonObjectNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(), new JsonObjectNode(structDel),
        PageKind.RECORDPAGE, -1);
  }

  @Override
  public JsonNullNode createJsonNullNode(long parentKey, long leftSibKey, long rightSibKey) {
    final long revision = mPageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel =
        new NodeDelegate(mPageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1, parentKey, 0, revision, null);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    return (JsonNullNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(), new JsonNullNode(structDel),
        PageKind.RECORDPAGE, -1);
  }

  @Override
  public JsonObjectKeyNode createJsonObjectKeyNode(long parentKey, long leftSibKey, long rightSibKey, long pathNodeKey,
      String name) {
    final int localNameKey = mPageWriteTrx.createNameKey(name, Kind.JSON_OBJECT_KEY);
    final long revision = mPageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel =
        new NodeDelegate(mPageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1, parentKey, 0, revision, null);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    return (JsonObjectKeyNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(),
        new JsonObjectKeyNode(structDel, localNameKey, name, pathNodeKey), PageKind.RECORDPAGE, -1);
  }

  @Override
  public JsonStringNode createJsonStringNode(long parentKey, long leftSibKey, long rightSibKey, byte[] value,
      boolean doCompress) {
    final long revision = mPageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel =
        new NodeDelegate(mPageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1, parentKey, 0, revision, null);
    final boolean compression = doCompress && value.length > 10;
    final byte[] compressedValue = compression
        ? Compression.compress(value, Deflater.HUFFMAN_ONLY)
        : value;
    final ValNodeDelegate valDel = new ValNodeDelegate(nodeDel, compressedValue, compression);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    return (JsonStringNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(), new JsonStringNode(valDel, structDel),
        PageKind.RECORDPAGE, -1);
  }

  @Override
  public JsonBooleanNode createJsonBooleanNode(long parentKey, long leftSibKey, long rightSibKey, boolean boolValue) {
    final long revision = mPageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel =
        new NodeDelegate(mPageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1, parentKey, 0, revision, null);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    return (JsonBooleanNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(), new JsonBooleanNode(boolValue, structDel),
        PageKind.RECORDPAGE, -1);
  }

  @Override
  public JsonNumberNode createJsonNumberNode(long parentKey, long leftSibKey, long rightSibKey, double dblValue) {
    final long revision = mPageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel =
        new NodeDelegate(mPageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1, parentKey, 0, revision, null);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    return (JsonNumberNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(), new JsonNumberNode(dblValue, structDel),
        PageKind.RECORDPAGE, -1);
  }
}
