package org.sirix.access.trx.node.json;

import com.google.common.hash.HashFunction;
import org.brackit.xquery.atomic.QNm;
import org.sirix.api.PageTrx;
import org.sirix.index.path.summary.PathNode;
import org.sirix.node.NodeKind;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.delegates.ValueNodeDelegate;
import org.sirix.node.interfaces.Record;
import org.sirix.node.json.*;
import org.sirix.page.PageKind;
import org.sirix.page.PathSummaryPage;
import org.sirix.page.UnorderedKeyValuePage;
import org.sirix.settings.Fixed;
import org.sirix.utils.Compression;
import org.sirix.utils.NamePageHash;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.util.zip.Deflater;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Node factory to create nodes.
 *
 * @author Johannes Lichtenberger
 *
 */
final class JsonNodeFactoryImpl implements JsonNodeFactory {

  /** Hash function used to hash nodes. */
  private final HashFunction hashFunction;

  /** {@link PageTrx} implementation. */
  private final PageTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx;

  /**
   * Constructor.
   *
   * @param hashFunction hash function used to hash nodes
   * @param pageWriteTrx {@link PageTrx} implementation
   */
  JsonNodeFactoryImpl(final HashFunction hashFunction,
      final PageTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx) {
    this.hashFunction = checkNotNull(hashFunction);
    this.pageWriteTrx = checkNotNull(pageWriteTrx);
  }

  @Override
  public PathNode createPathNode(final @Nonnegative long parentKey, final long leftSibKey, final long rightSibKey,
      @Nonnull final QNm name, @Nonnull final NodeKind kind, final @Nonnegative int level) {
    final int uriKey = -1;
    final int prefixKey = -1;
    final int localName = name.getLocalName() != null && !name.getLocalName().isEmpty()
        ? NamePageHash.generateHashForString(name.getLocalName())
        : -1;

    final long revision = pageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(
        ((PathSummaryPage) pageWriteTrx.getActualRevisionRootPage()
                                       .getPathSummaryPageReference()
                                       .getPage()).getMaxNodeKey(0)
            + 1,
        parentKey, hashFunction, null, revision, null);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, uriKey, prefixKey, localName, 0);

    return (PathNode) pageWriteTrx.createEntry(nodeDel.getNodeKey(),
        new PathNode(name, nodeDel, structDel, nameDel, kind, 1, level), PageKind.PATHSUMMARYPAGE, 0);
  }

  @Override
  public ArrayNode createJsonArrayNode(long parentKey, long leftSibKey, long rightSibKey, long pathNodeKey) {
    final long revision = pageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(pageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1,
        parentKey, hashFunction, null, revision, null);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    return (ArrayNode) pageWriteTrx.createEntry(nodeDel.getNodeKey(), new ArrayNode(structDel, pathNodeKey),
        PageKind.RECORDPAGE, -1);
  }

  @Override
  public ObjectNode createJsonObjectNode(long parentKey, long leftSibKey, long rightSibKey) {
    final long revision = pageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(pageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1,
        parentKey, hashFunction, null, revision, null);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    return (ObjectNode) pageWriteTrx.createEntry(nodeDel.getNodeKey(), new ObjectNode(structDel), PageKind.RECORDPAGE,
        -1);
  }

  @Override
  public NullNode createJsonNullNode(long parentKey, long leftSibKey, long rightSibKey) {
    final long revision = pageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(pageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1,
        parentKey, hashFunction, null, revision, null);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    return (NullNode) pageWriteTrx.createEntry(nodeDel.getNodeKey(), new NullNode(structDel), PageKind.RECORDPAGE, -1);
  }

  @Override
  public ObjectKeyNode createJsonObjectKeyNode(long parentKey, long leftSibKey, long rightSibKey, long pathNodeKey,
      String name, long objectValueKey) {
    final int localNameKey = pageWriteTrx.createNameKey(name, NodeKind.OBJECT_KEY);
    final long revision = pageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(pageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1,
        parentKey, hashFunction, null, revision, null);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel, objectValueKey, rightSibKey, leftSibKey, 0, 0);
    return (ObjectKeyNode) pageWriteTrx.createEntry(nodeDel.getNodeKey(),
        new ObjectKeyNode(structDel, localNameKey, name, pathNodeKey), PageKind.RECORDPAGE, -1);
  }

  @Override
  public StringNode createJsonStringNode(long parentKey, long leftSibKey, long rightSibKey, byte[] value,
      boolean doCompress) {
    final long revision = pageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(pageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1,
        parentKey, hashFunction, null, revision, null);
    final boolean compression = doCompress && value.length > 10;
    final byte[] compressedValue = compression
        ? Compression.compress(value, Deflater.HUFFMAN_ONLY)
        : value;
    final ValueNodeDelegate valDel = new ValueNodeDelegate(nodeDel, compressedValue, compression);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    return (StringNode) pageWriteTrx.createEntry(nodeDel.getNodeKey(), new StringNode(valDel, structDel),
        PageKind.RECORDPAGE, -1);
  }

  @Override
  public BooleanNode createJsonBooleanNode(long parentKey, long leftSibKey, long rightSibKey, boolean boolValue) {
    final long revision = pageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(pageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1,
        parentKey, hashFunction, null, revision, null);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    return (BooleanNode) pageWriteTrx.createEntry(nodeDel.getNodeKey(), new BooleanNode(boolValue, structDel),
        PageKind.RECORDPAGE, -1);
  }

  @Override
  public NumberNode createJsonNumberNode(long parentKey, long leftSibKey, long rightSibKey, Number value) {
    final long revision = pageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(pageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1,
        parentKey, hashFunction, null, revision, null);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    return (NumberNode) pageWriteTrx.createEntry(nodeDel.getNodeKey(), new NumberNode(value, structDel),
        PageKind.RECORDPAGE, -1);
  }

  @Override
  public ObjectNullNode createJsonObjectNullNode(long parentKey) {
    final long revision = pageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(pageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1,
                                                  parentKey, hashFunction, null, revision, null);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), 0, 0);
    return (ObjectNullNode) pageWriteTrx.createEntry(nodeDel.getNodeKey(), new ObjectNullNode(structDel), PageKind.RECORDPAGE, -1);
  }

  @Override
  public ObjectStringNode createJsonObjectStringNode(long parentKey, byte[] value,
      boolean doCompress) {
    final long revision = pageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(pageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1,
                                                  parentKey, hashFunction, null, revision, null);
    final boolean compression = doCompress && value.length > 40;
    final byte[] compressedValue = compression
        ? Compression.compress(value, Deflater.BEST_COMPRESSION)
        : value;
    final ValueNodeDelegate valDel = new ValueNodeDelegate(nodeDel, compressedValue, compression);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), 0, 0);
    return (ObjectStringNode) pageWriteTrx.createEntry(nodeDel.getNodeKey(), new ObjectStringNode(valDel, structDel),
                                                  PageKind.RECORDPAGE, -1);
  }

  @Override
  public ObjectBooleanNode createJsonObjectBooleanNode(long parentKey, boolean boolValue) {
    final long revision = pageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(pageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1,
                                                  parentKey, hashFunction, null, revision, null);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), 0, 0);
    return (ObjectBooleanNode) pageWriteTrx.createEntry(nodeDel.getNodeKey(), new ObjectBooleanNode(boolValue, structDel),
                                                   PageKind.RECORDPAGE, -1);
  }

  @Override
  public ObjectNumberNode createJsonObjectNumberNode(long parentKey, Number value) {
    final long revision = pageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(pageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1,
                                                  parentKey, hashFunction, null, revision, null);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), 0, 0);
    return (ObjectNumberNode) pageWriteTrx.createEntry(nodeDel.getNodeKey(), new ObjectNumberNode(value, structDel),
                                                  PageKind.RECORDPAGE, -1);
  }
}
