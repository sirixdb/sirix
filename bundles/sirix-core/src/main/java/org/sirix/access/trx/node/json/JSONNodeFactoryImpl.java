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
import org.sirix.node.json.JSONArrayNode;
import org.sirix.node.json.JSONBooleanNode;
import org.sirix.node.json.JSONNumberNode;
import org.sirix.node.json.JSONObjectKeyNode;
import org.sirix.node.json.JSONObjectNode;
import org.sirix.node.json.JSONStringNode;
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
final class JSONNodeFactoryImpl implements JSONNodeFactory {

  /** {@link PageWriteTrx} implementation. */
  private final PageWriteTrx<Long, Record, UnorderedKeyValuePage> mPageWriteTrx;

  /**
   * Constructor.
   *
   * @param pageWriteTrx {@link PageWriteTrx} implementation
   * @throws SirixIOException if an I/O exception occured due to name key creation
   */
  JSONNodeFactoryImpl(final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx) {
    mPageWriteTrx = checkNotNull(pageWriteTrx);
    mPageWriteTrx.createNameKey("xs:untyped", Kind.ATTRIBUTE);
    mPageWriteTrx.createNameKey("xs:untyped", Kind.NAMESPACE);
    mPageWriteTrx.createNameKey("xs:untyped", Kind.ELEMENT);
    mPageWriteTrx.createNameKey("xs:untyped", Kind.PROCESSING_INSTRUCTION);
  }

  @Override
  public PathNode createPathNode(final @Nonnegative long parentKey, final @Nonnegative long leftSibKey,
      final long rightSibKey, final long hash, @Nonnull final QNm name, @Nonnull final Kind kind,
      final @Nonnegative int level) {
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
  public JSONArrayNode createJSONArrayNode(long parentKey, long leftSibKey, long rightSibKey, long hash) {
    final long revision = mPageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel =
        new NodeDelegate(mPageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1, parentKey, 0, revision, null);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    return (JSONArrayNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(), new JSONArrayNode(structDel),
        PageKind.RECORDPAGE, -1);
  }

  @Override
  public JSONObjectNode createJSONObjectNode(long parentKey, long leftSibKey, long rightSibKey, long hash) {
    final long revision = mPageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel =
        new NodeDelegate(mPageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1, parentKey, 0, revision, null);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    return (JSONObjectNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(), new JSONObjectNode(structDel),
        PageKind.RECORDPAGE, -1);
  }

  @Override
  public JSONObjectKeyNode createJSONObjectKeyNode(long parentKey, long leftSibKey, long rightSibKey, long hash,
      long pathNodeKey, String name) {
    final int localNameKey = mPageWriteTrx.createNameKey(name, Kind.JSON_OBJECT_KEY);
    final long revision = mPageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel =
        new NodeDelegate(mPageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1, parentKey, 0, revision, null);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    return (JSONObjectKeyNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(),
        new JSONObjectKeyNode(structDel, localNameKey, name, pathNodeKey), PageKind.RECORDPAGE, -1);
  }

  @Override
  public JSONStringNode createJSONStringNode(long parentKey, long leftSibKey, long rightSibKey, long hash, byte[] value,
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
    return (JSONStringNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(), new JSONStringNode(valDel, structDel),
        PageKind.RECORDPAGE, -1);
  }

  @Override
  public JSONBooleanNode createJSONBooleanNode(long parentKey, long leftSibKey, long rightSibKey, long hash,
      boolean boolValue) {
    final long revision = mPageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel =
        new NodeDelegate(mPageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1, parentKey, 0, revision, null);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    return (JSONBooleanNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(), new JSONBooleanNode(boolValue, structDel),
        PageKind.RECORDPAGE, -1);
  }

  @Override
  public JSONNumberNode createJSONNumberNode(long parentKey, long leftSibKey, long rightSibKey, long hash,
      double dblValue) {
    final long revision = mPageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel =
        new NodeDelegate(mPageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1, parentKey, 0, revision, null);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    return (JSONNumberNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(), new JSONNumberNode(dblValue, structDel),
        PageKind.RECORDPAGE, -1);
  }
}
