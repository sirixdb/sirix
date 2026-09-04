/*
 * Copyright (c) 2023, Sirix Contributors
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.node;

import io.brackit.query.atomic.QNm;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.index.path.summary.PathNode;
import io.sirix.index.path.summary.PathStats;
import io.sirix.index.projection.ProjectionIndexRowGroupRecord;
import io.sirix.index.vector.VectorIndexMetadataNode;
import io.sirix.index.vector.VectorNode;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.delegates.StructNodeDelegate;
import io.sirix.node.delegates.ValueNodeDelegate;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.DeweyIdSerializer;
import io.sirix.node.interfaces.StructNode;
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
import io.sirix.node.xml.AttributeNode;
import io.sirix.node.xml.CommentNode;
import io.sirix.node.xml.ElementNode;
import io.sirix.node.xml.NamespaceNode;
import io.sirix.node.xml.PINode;
import io.sirix.node.xml.TextNode;
import io.sirix.node.xml.XmlDocumentRootNode;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import net.openhft.hashing.LongHashFunction;
import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static io.sirix.node.Utils.getVarLong;
import static io.sirix.node.Utils.putVarLong;

/**
 * Enumeration for different nodes. All nodes are determined by a unique id.
 *
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 */
@SuppressWarnings({"DuplicatedCode", "unchecked"})
public enum NodeKind implements DeweyIdSerializer {

  /**
   * Node kind is element.
   */
  ELEMENT((byte) 1) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      final long nodeKey = recordID;
      final var config = resourceConfiguration;

      // Structural fields first.
      final long parentKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
      final long rightSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
      final long leftSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
      final long firstChildKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
      final long lastChildKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);

      // Name node fields.
      final long pathNodeKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
      final int prefixKey = DeltaVarIntCodec.decodeSigned(source);
      final int localNameKey = DeltaVarIntCodec.decodeSigned(source);
      final int uriKey = DeltaVarIntCodec.decodeSigned(source);

      // Metadata.
      final int previousRevision = DeltaVarIntCodec.decodeSigned(source);
      final int lastModifiedRevision = DeltaVarIntCodec.decodeSigned(source);
      final long childCount = config.storeChildCount()
          ? DeltaVarIntCodec.decodeSignedLong(source)
          : 0;
      final long hash;
      final long descendantCount;
      if (config.hashType != HashType.NONE) {
        hash = source.readLong();
        descendantCount = DeltaVarIntCodec.decodeSignedLong(source);
      } else {
        hash = 0L;
        descendantCount = 0L;
      }

      // Attribute keys.
      final int attrCount = DeltaVarIntCodec.decodeSigned(source);
      final LongList attrKeys = new LongArrayList(attrCount);
      for (int i = 0; i < attrCount; i++) {
        attrKeys.add(DeltaVarIntCodec.decodeDelta(source, nodeKey));
      }

      // Namespace keys.
      final int nsCount = DeltaVarIntCodec.decodeSigned(source);
      final LongList namespaceKeys = new LongArrayList(nsCount);
      for (int i = 0; i < nsCount; i++) {
        namespaceKeys.add(DeltaVarIntCodec.decodeDelta(source, nodeKey));
      }

      return new ElementNode(nodeKey, parentKey, previousRevision, lastModifiedRevision, rightSiblingKey,
          leftSiblingKey, firstChildKey, lastChildKey, childCount, descendantCount, hash, pathNodeKey, prefixKey,
          localNameKey, uriKey, resourceConfiguration.nodeHashFunction, deweyID, attrKeys, namespaceKeys, EMPTY_QNM);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final ElementNode node = (ElementNode) record;
      final var config = resourceConfiguration;
      final long nodeKey = node.getNodeKey();

      // Structural fields first.
      DeltaVarIntCodec.encodeDelta(sink, node.getParentKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getRightSiblingKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getLeftSiblingKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getFirstChildKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getLastChildKey(), nodeKey);

      // Name node fields.
      DeltaVarIntCodec.encodeDelta(sink, node.getPathNodeKey(), nodeKey);
      DeltaVarIntCodec.encodeSigned(sink, node.getPrefixKey());
      DeltaVarIntCodec.encodeSigned(sink, node.getLocalNameKey());
      DeltaVarIntCodec.encodeSigned(sink, node.getURIKey());

      // Metadata.
      DeltaVarIntCodec.encodeSigned(sink, node.getPreviousRevisionNumber());
      DeltaVarIntCodec.encodeSigned(sink, node.getLastModifiedRevisionNumber());
      if (config.storeChildCount()) {
        DeltaVarIntCodec.encodeSignedLong(sink, node.getChildCount());
      }
      if (config.hashType != HashType.NONE) {
        writeHash(sink, node.getHash());
        DeltaVarIntCodec.encodeSignedLong(sink, node.getDescendantCount());
      }

      // Attribute keys.
      DeltaVarIntCodec.encodeSigned(sink, node.getAttributeCount());
      for (int i = 0, attCount = node.getAttributeCount(); i < attCount; i++) {
        DeltaVarIntCodec.encodeDelta(sink, node.getAttributeKey(i), nodeKey);
      }

      // Namespace keys.
      DeltaVarIntCodec.encodeSigned(sink, node.getNamespaceCount());
      for (int i = 0, nspCount = node.getNamespaceCount(); i < nspCount; i++) {
        DeltaVarIntCodec.encodeDelta(sink, node.getNamespaceKey(i), nodeKey);
      }
    }
  },

  /**
   * Node kind is attribute.
   */
  ATTRIBUTE((byte) 2) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      final long nodeKey = recordID;

      final long parentKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
      final long pathNodeKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
      final int prefixKey = DeltaVarIntCodec.decodeSigned(source);
      final int localNameKey = DeltaVarIntCodec.decodeSigned(source);
      final int uriKey = DeltaVarIntCodec.decodeSigned(source);
      final int previousRevision = DeltaVarIntCodec.decodeSigned(source);
      final int lastModifiedRevision = DeltaVarIntCodec.decodeSigned(source);

      source.readByte(); // reserved compression flag
      final byte[] value = new byte[DeltaVarIntCodec.decodeSigned(source)];
      source.read(value, 0, value.length);

      return new AttributeNode(nodeKey, parentKey, previousRevision, lastModifiedRevision, pathNodeKey, prefixKey,
          localNameKey, uriKey, 0, value, resourceConfiguration.nodeHashFunction, deweyID, EMPTY_QNM);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final AttributeNode node = (AttributeNode) record;
      final long nodeKey = node.getNodeKey();

      DeltaVarIntCodec.encodeDelta(sink, node.getParentKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getPathNodeKey(), nodeKey);
      DeltaVarIntCodec.encodeSigned(sink, node.getPrefixKey());
      DeltaVarIntCodec.encodeSigned(sink, node.getLocalNameKey());
      DeltaVarIntCodec.encodeSigned(sink, node.getURIKey());
      DeltaVarIntCodec.encodeSigned(sink, node.getPreviousRevisionNumber());
      DeltaVarIntCodec.encodeSigned(sink, node.getLastModifiedRevisionNumber());

      // Attribute values are currently written uncompressed.
      final byte[] value = node.getRawValue();
      sink.writeByte((byte) 0);
      DeltaVarIntCodec.encodeSigned(sink, value.length);
      sink.write(value);
    }
  },

  /**
   * Node kind is namespace.
   */
  NAMESPACE((byte) 13) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      final long nodeKey = recordID;
      final long parentKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
      final long pathNodeKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
      final int prefixKey = DeltaVarIntCodec.decodeSigned(source);
      final int localNameKey = DeltaVarIntCodec.decodeSigned(source);
      final int uriKey = DeltaVarIntCodec.decodeSigned(source);
      final int previousRevision = DeltaVarIntCodec.decodeSigned(source);
      final int lastModifiedRevision = DeltaVarIntCodec.decodeSigned(source);

      return new NamespaceNode(nodeKey, parentKey, previousRevision, lastModifiedRevision, pathNodeKey, prefixKey,
          localNameKey, uriKey, 0, resourceConfiguration.nodeHashFunction, deweyID, EMPTY_QNM);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final NamespaceNode node = (NamespaceNode) record;
      final long nodeKey = node.getNodeKey();
      DeltaVarIntCodec.encodeDelta(sink, node.getParentKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getPathNodeKey(), nodeKey);
      DeltaVarIntCodec.encodeSigned(sink, node.getPrefixKey());
      DeltaVarIntCodec.encodeSigned(sink, node.getLocalNameKey());
      DeltaVarIntCodec.encodeSigned(sink, node.getURIKey());
      DeltaVarIntCodec.encodeSigned(sink, node.getPreviousRevisionNumber());
      DeltaVarIntCodec.encodeSigned(sink, node.getLastModifiedRevisionNumber());
    }
  },

  /**
   * Node kind is text. Note: Hash is NOT serialized for text nodes because sibling keys affect the
   * hash, and sibling key changes don't trigger hash recalculation. The hash is computed on-the-fly
   * when needed.
   */
  TEXT((byte) 3) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      final long nodeKey = recordID;

      final long parentKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
      final long rightSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
      final long leftSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
      final int previousRevision = DeltaVarIntCodec.decodeSigned(source);
      final int lastModifiedRevision = DeltaVarIntCodec.decodeSigned(source);

      final boolean isCompressed = source.readByte() == (byte) 1;
      final byte[] value = new byte[DeltaVarIntCodec.decodeSigned(source)];
      source.read(value, 0, value.length);

      // Hash is NOT deserialized - it's computed on-the-fly in getHash()
      return new TextNode(nodeKey, parentKey, previousRevision, lastModifiedRevision, rightSiblingKey, leftSiblingKey,
          0, value, isCompressed, resourceConfiguration.nodeHashFunction, deweyID);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final TextNode node = (TextNode) record;

      final long nodeKey = node.getNodeKey();
      DeltaVarIntCodec.encodeDelta(sink, node.getParentKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getRightSiblingKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getLeftSiblingKey(), nodeKey);
      DeltaVarIntCodec.encodeSigned(sink, node.getPreviousRevisionNumber());
      DeltaVarIntCodec.encodeSigned(sink, node.getLastModifiedRevisionNumber());

      sink.writeByte(node.isCompressed()
          ? (byte) 1
          : (byte) 0);
      final byte[] value = node.getRawValue();
      DeltaVarIntCodec.encodeSigned(sink, value.length);
      sink.write(value);

      // Hash is NOT serialized - it's computed on-the-fly in getHash()
    }
  },

  /**
   * Node kind is processing instruction.
   */
  PROCESSING_INSTRUCTION((byte) 7) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      final long nodeKey = recordID;
      var config = resourceConfiguration;

      final long parentKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
      final long rightSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
      final long leftSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
      final long firstChildKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
      final long lastChildKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
      final long pathNodeKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
      final int prefixKey = DeltaVarIntCodec.decodeSigned(source);
      final int localNameKey = DeltaVarIntCodec.decodeSigned(source);
      final int uriKey = DeltaVarIntCodec.decodeSigned(source);
      final int previousRevision = DeltaVarIntCodec.decodeSigned(source);
      final int lastModifiedRevision = DeltaVarIntCodec.decodeSigned(source);
      final long childCount = config.storeChildCount()
          ? DeltaVarIntCodec.decodeSignedLong(source)
          : 0;
      final long descendantCount;
      if (config.hashType != HashType.NONE) {
        descendantCount = DeltaVarIntCodec.decodeSignedLong(source);
      } else {
        descendantCount = 0;
      }

      final boolean isCompressed = source.readByte() == (byte) 1;
      final byte[] value = new byte[DeltaVarIntCodec.decodeSigned(source)];
      source.read(value, 0, value.length);

      return new PINode(recordID, parentKey, previousRevision, lastModifiedRevision, rightSiblingKey, leftSiblingKey,
          firstChildKey, lastChildKey, childCount, descendantCount, 0, pathNodeKey, prefixKey, localNameKey, uriKey,
          value, isCompressed, resourceConfiguration.nodeHashFunction, deweyID, EMPTY_QNM);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final PINode node = (PINode) record;
      final long nodeKey = node.getNodeKey();
      var config = resourceConfiguration;

      DeltaVarIntCodec.encodeDelta(sink, node.getParentKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getRightSiblingKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getLeftSiblingKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getFirstChildKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getLastChildKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getPathNodeKey(), nodeKey);
      DeltaVarIntCodec.encodeSigned(sink, node.getPrefixKey());
      DeltaVarIntCodec.encodeSigned(sink, node.getLocalNameKey());
      DeltaVarIntCodec.encodeSigned(sink, node.getURIKey());
      DeltaVarIntCodec.encodeSigned(sink, node.getPreviousRevisionNumber());
      DeltaVarIntCodec.encodeSigned(sink, node.getLastModifiedRevisionNumber());

      if (config.storeChildCount()) {
        DeltaVarIntCodec.encodeSignedLong(sink, node.getChildCount());
      }
      if (config.hashType != HashType.NONE) {
        DeltaVarIntCodec.encodeSignedLong(sink, node.getDescendantCount());
      }

      sink.writeByte(node.isCompressed()
          ? (byte) 1
          : (byte) 0);
      final byte[] value = node.getRawValue();
      DeltaVarIntCodec.encodeSigned(sink, value.length);
      sink.write(value);
    }
  },

  /**
   * Node kind is comment. Note: Hash is NOT serialized for comment nodes because sibling keys affect
   * the hash, and sibling key changes don't trigger hash recalculation. The hash is computed
   * on-the-fly when needed.
   */
  COMMENT((byte) 8) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      final long nodeKey = recordID;

      final long parentKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
      final long rightSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
      final long leftSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
      final int previousRevision = DeltaVarIntCodec.decodeSigned(source);
      final int lastModifiedRevision = DeltaVarIntCodec.decodeSigned(source);

      final boolean isCompressed = source.readByte() == (byte) 1;
      final byte[] value = new byte[DeltaVarIntCodec.decodeSigned(source)];
      source.read(value, 0, value.length);

      // Hash is NOT deserialized - it's computed on-the-fly in getHash()
      return new CommentNode(nodeKey, parentKey, previousRevision, lastModifiedRevision, rightSiblingKey,
          leftSiblingKey, 0, value, isCompressed, resourceConfiguration.nodeHashFunction, deweyID);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final CommentNode node = (CommentNode) record;

      final long nodeKey = node.getNodeKey();
      DeltaVarIntCodec.encodeDelta(sink, node.getParentKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getRightSiblingKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getLeftSiblingKey(), nodeKey);
      DeltaVarIntCodec.encodeSigned(sink, node.getPreviousRevisionNumber());
      DeltaVarIntCodec.encodeSigned(sink, node.getLastModifiedRevisionNumber());

      sink.writeByte(node.isCompressed()
          ? (byte) 1
          : (byte) 0);
      final byte[] value = node.getRawValue();
      DeltaVarIntCodec.encodeSigned(sink, value.length);
      sink.write(value);

      // Hash is NOT serialized - it's computed on-the-fly in getHash()
    }
  },

  /**
   * Node kind is document root.
   */
  // Virtualize document root node?
  XML_DOCUMENT((byte) 9) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      final LongHashFunction hashFunction = resourceConfiguration.nodeHashFunction;
      final long nodeKey = recordID;
      final long firstChildKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
      final long childCount = firstChildKey == Fixed.NULL_NODE_KEY.getStandardProperty()
          ? 0
          : 1;

      final long hash;
      final long descendantCount;
      if (resourceConfiguration.hashType != HashType.NONE) {
        hash = source.readLong();
        descendantCount = DeltaVarIntCodec.decodeSignedLong(source);
      } else {
        hash = 0;
        descendantCount = 0;
      }

      final XmlDocumentRootNode node = new XmlDocumentRootNode(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(),
          firstChildKey, Fixed.NULL_NODE_KEY.getStandardProperty(), // lastChildKey not stored for XML doc root
          childCount, descendantCount, hashFunction);
      node.setHash(hash);
      return node;
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final XmlDocumentRootNode node = (XmlDocumentRootNode) record;
      DeltaVarIntCodec.encodeDelta(sink, node.getFirstChildKey(), node.getNodeKey());
      if (resourceConfiguration.hashType != HashType.NONE) {
        writeHash(sink, node.getHash());
        DeltaVarIntCodec.encodeSignedLong(sink, node.getDescendantCount());
      }
    }
  },

  /**
   * Whitespace text.
   */
  WHITESPACE((byte) 4) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      throw new UnsupportedOperationException();
    }
  },

  /**
   * Node kind is deleted node.
   */
  DELETE((byte) 5) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      final LongHashFunction hashFunction = resourceConfiguration.nodeHashFunction;
      final NodeDelegate delegate = new NodeDelegate(recordID, 0, hashFunction, 0, 0, (SirixDeweyID) null);
      return new DeletedNode(delegate);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {}
  },

  /**
   * NullNode to support the Null Object pattern.
   */
  NULL((byte) 6) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      throw new UnsupportedOperationException();
    }
  },

  /**
   * Dumb node for testing.
   */
  DUMB((byte) 20) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      return new DumbNode(recordID);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },

  /**
   * AtomicKind.
   */
  ATOMIC((byte) 15) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },

  /**
   * Node kind is path node.
   */
  PATH((byte) 16) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      // Inlined NodeDelegate fields (delegate-less PathNode owns its structural state directly).
      final long parentKey = recordID - getVarLong(source);
      final int previousRevision = source.readInt();
      final int lastModifiedRevision = source.readInt();

      // Inlined StructNodeDelegate fields. PATH is never a value-node, so firstChild/lastChild
      // are always present on the wire; childCount and descendantCount are gated on config.
      final long rightSibling = source.readLong();
      final long leftSibling = source.readLong();
      final long firstChild = source.readLong();
      final long lastChild = source.readLong();
      final long childCount = resourceConfiguration.storeChildCount()
          ? source.readLong()
          : 0L;
      final long descendantCount = resourceConfiguration.hashType != HashType.NONE
          ? source.readLong()
          : 0L;

      // Inlined NameNodeDelegate fields.
      final int uriKey = source.readInt();
      final int prefixKey = source.readInt();
      final int localNameKey = source.readInt();
      final long pathNodeKey = getVarLong(source);

      final NodeKind kind = NodeKind.getKind(source.readByte());
      final int references = source.readInt();
      final int level = source.readInt();

      final PathNode pathNode = new PathNode(null, kind, references, level, recordID, parentKey, previousRevision,
          lastModifiedRevision, (SirixDeweyID) null, firstChild, lastChild, rightSibling, leftSibling, childCount,
          descendantCount, uriKey, prefixKey, localNameKey, pathNodeKey);

      // Optional per-path statistics trailer — present iff the resource was configured
      // with withPathStatistics=true. Older resources / disabled configs pay zero bytes.
      // The on-disk format is owned by PathStats so the encoder / decoder stay in lockstep.
      if (resourceConfiguration.withPathStatistics) {
        pathNode.setStats(PathStats.readFromOrNullIfEmpty(source));
      }

      return pathNode;
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final PathNode node = (PathNode) record;
      // Inlined NodeDelegate fields.
      putVarLong(sink, node.getNodeKey() - node.getParentKey());
      sink.writeInt(node.getPreviousRevisionNumber());
      sink.writeInt(node.getLastModifiedRevisionNumber());

      // Inlined StructNodeDelegate fields.
      sink.writeLong(node.getRightSiblingKey());
      sink.writeLong(node.getLeftSiblingKey());
      sink.writeLong(node.getFirstChildKey());
      sink.writeLong(node.getLastChildKey());
      if (resourceConfiguration.storeChildCount()) {
        sink.writeLong(node.getChildCount());
      }
      if (resourceConfiguration.hashType != HashType.NONE) {
        sink.writeLong(node.getDescendantCount());
      }

      // Inlined NameNodeDelegate fields.
      sink.writeInt(node.getURIKey());
      sink.writeInt(node.getPrefixKey());
      sink.writeInt(node.getLocalNameKey());
      putVarLong(sink, node.getPathNodeKey());

      sink.writeByte(node.getPathKind().getId());
      sink.writeInt(node.getReferences());
      sink.writeInt(node.getLevel());

      // Optional per-path statistics trailer — mirrors the deserialize path. Writes
      // zero extra bytes when withPathStatistics is false (backward compatible).
      // PathStats owns the on-disk format; null stats are written as the empty trailer.
      if (resourceConfiguration.withPathStatistics) {
        PathStats.writeOrEmpty(sink, node.getStats());
      }
    }

    @Override
    public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },

  /**
   * Node includes a deweyID &lt;=&gt; nodeKey mapping.
   */
  DEWEYIDMAPPING((byte) 23) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },

  /**
   * JSON object node.
   */
  OBJECT((byte) 24) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      // STRUCTURAL FIELDS FIRST (for lazy singleton optimization)
      long parentKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      long rightSiblingKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      long leftSiblingKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      long firstChildKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      long lastChildKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      // LAZY FIELDS (metadata - parsed on demand in singleton mode)
      int prevRev = DeltaVarIntCodec.decodeSigned(source);
      int lastModRev = DeltaVarIntCodec.decodeSigned(source);
      long childCount = resourceConfiguration.storeChildCount()
          ? DeltaVarIntCodec.decodeSignedLong(source)
          : 0;
      long hash = 0;
      long descendantCount = 0;
      if (resourceConfiguration.hashType != HashType.NONE) {
        hash = source.readLong();
        descendantCount = DeltaVarIntCodec.decodeSignedLong(source);
      }
      return new ObjectNode(recordID, parentKey, prevRev, lastModRev, rightSiblingKey, leftSiblingKey, firstChildKey,
          lastChildKey, childCount, descendantCount, hash, resourceConfiguration.nodeHashFunction, deweyID);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final ObjectNode node = (ObjectNode) record;
      final long nodeKey = node.getNodeKey();
      // STRUCTURAL FIELDS FIRST
      DeltaVarIntCodec.encodeDelta(sink, node.getParentKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getRightSiblingKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getLeftSiblingKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getFirstChildKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getLastChildKey(), nodeKey);
      // LAZY FIELDS (metadata)
      DeltaVarIntCodec.encodeSigned(sink, node.getPreviousRevisionNumber());
      DeltaVarIntCodec.encodeSigned(sink, node.getLastModifiedRevisionNumber());
      if (resourceConfiguration.storeChildCount()) {
        DeltaVarIntCodec.encodeSignedLong(sink, node.getChildCount());
      }
      if (resourceConfiguration.hashType != HashType.NONE) {
        sink.writeLong(node.getHash());
        DeltaVarIntCodec.encodeSignedLong(sink, node.getDescendantCount());
      }
    }

    @Override
    public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },

  /**
   * JSON array node.
   */
  ARRAY((byte) 25) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      // STRUCTURAL FIELDS FIRST (for lazy singleton optimization)
      long parentKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      long rightSiblingKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      long leftSiblingKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      long firstChildKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      long lastChildKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      long pathNodeKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      // LAZY FIELDS (metadata)
      int prevRev = DeltaVarIntCodec.decodeSigned(source);
      int lastModRev = DeltaVarIntCodec.decodeSigned(source);
      long childCount = resourceConfiguration.storeChildCount()
          ? DeltaVarIntCodec.decodeSignedLong(source)
          : 0;
      long hash = 0;
      long descendantCount = 0;
      if (resourceConfiguration.hashType != HashType.NONE) {
        hash = source.readLong();
        descendantCount = DeltaVarIntCodec.decodeSignedLong(source);
      }
      return new ArrayNode(recordID, parentKey, pathNodeKey, prevRev, lastModRev, rightSiblingKey, leftSiblingKey,
          firstChildKey, lastChildKey, childCount, descendantCount, hash, resourceConfiguration.nodeHashFunction,
          deweyID);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final ArrayNode node = (ArrayNode) record;
      final long nodeKey = node.getNodeKey();
      // STRUCTURAL FIELDS FIRST
      DeltaVarIntCodec.encodeDelta(sink, node.getParentKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getRightSiblingKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getLeftSiblingKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getFirstChildKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getLastChildKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getPathNodeKey(), nodeKey);
      // LAZY FIELDS (metadata)
      DeltaVarIntCodec.encodeSigned(sink, node.getPreviousRevisionNumber());
      DeltaVarIntCodec.encodeSigned(sink, node.getLastModifiedRevisionNumber());
      if (resourceConfiguration.storeChildCount()) {
        DeltaVarIntCodec.encodeSignedLong(sink, node.getChildCount());
      }
      if (resourceConfiguration.hashType != HashType.NONE) {
        sink.writeLong(node.getHash());
        DeltaVarIntCodec.encodeSignedLong(sink, node.getDescendantCount());
      }
    }

    @Override
    public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },

  // (Phase 4: legacy OBJECT_KEY (byte 26) and OBJECT_KEY_PAX (byte 126) enum constants
  // deleted — fully replaced by the 6 fused OBJECT_NAMED_* kinds 48-53.)

  /**
   * JSON fused object-key + boolean value (single record per {@code {"name": true|false}}). On-wire
   * layout mirrors {@link ObjectNamedBooleanNode}'s flyweight serialization: structural fields first
   * (parent, siblings), then nameKey + pathNodeKey, then revisions + hash + value.
   */
  OBJECT_NAMED_BOOLEAN((byte) 48) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      long parentKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      long rightSiblingKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      long leftSiblingKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      int nameKey = DeltaVarIntCodec.decodeSigned(source);
      long pathNodeKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      int prevRev = DeltaVarIntCodec.decodeSigned(source);
      int lastModRev = DeltaVarIntCodec.decodeSigned(source);
      long hash = 0;
      if (resourceConfiguration.hashType != HashType.NONE) {
        hash = source.readLong();
      }
      boolean value = source.readBoolean();
      return new ObjectNamedBooleanNode(recordID, parentKey, rightSiblingKey, leftSiblingKey, nameKey, pathNodeKey,
          prevRev, lastModRev, hash, value, resourceConfiguration.nodeHashFunction, deweyID);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final ObjectNamedBooleanNode node = (ObjectNamedBooleanNode) record;
      final long nodeKey = node.getNodeKey();
      DeltaVarIntCodec.encodeDelta(sink, node.getParentKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getRightSiblingKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getLeftSiblingKey(), nodeKey);
      DeltaVarIntCodec.encodeSigned(sink, node.getNameKey());
      DeltaVarIntCodec.encodeDelta(sink, node.getPathNodeKey(), nodeKey);
      DeltaVarIntCodec.encodeSigned(sink, node.getPreviousRevisionNumber());
      DeltaVarIntCodec.encodeSigned(sink, node.getLastModifiedRevisionNumber());
      if (resourceConfiguration.hashType != HashType.NONE) {
        sink.writeLong(node.getHash());
      }
      sink.writeBoolean(node.getValue());
    }

    @Override
    public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },

  /**
   * JSON fused object-key + number value (single record per {@code {"name": 42}}).
   */
  OBJECT_NAMED_NUMBER((byte) 49) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      long parentKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      long rightSiblingKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      long leftSiblingKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      int nameKey = DeltaVarIntCodec.decodeSigned(source);
      long pathNodeKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      int prevRev = DeltaVarIntCodec.decodeSigned(source);
      int lastModRev = DeltaVarIntCodec.decodeSigned(source);
      long hash = 0;
      if (resourceConfiguration.hashType != HashType.NONE) {
        hash = source.readLong();
      }
      Number value = deserializeNumber(source);
      return new ObjectNamedNumberNode(recordID, parentKey, rightSiblingKey, leftSiblingKey, nameKey, pathNodeKey,
          prevRev, lastModRev, hash, value, resourceConfiguration.nodeHashFunction, deweyID);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final ObjectNamedNumberNode node = (ObjectNamedNumberNode) record;
      final long nodeKey = node.getNodeKey();
      DeltaVarIntCodec.encodeDelta(sink, node.getParentKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getRightSiblingKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getLeftSiblingKey(), nodeKey);
      DeltaVarIntCodec.encodeSigned(sink, node.getNameKey());
      DeltaVarIntCodec.encodeDelta(sink, node.getPathNodeKey(), nodeKey);
      DeltaVarIntCodec.encodeSigned(sink, node.getPreviousRevisionNumber());
      DeltaVarIntCodec.encodeSigned(sink, node.getLastModifiedRevisionNumber());
      if (resourceConfiguration.hashType != HashType.NONE) {
        sink.writeLong(node.getHash());
      }
      serializeNumber(node.getValue(), sink);
    }

    @Override
    public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },

  /**
   * JSON fused object-key + string value (single record per {@code {"name": "value"}}).
   */
  OBJECT_NAMED_STRING((byte) 50) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      long parentKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      long rightSiblingKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      long leftSiblingKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      int nameKey = DeltaVarIntCodec.decodeSigned(source);
      long pathNodeKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      int prevRev = DeltaVarIntCodec.decodeSigned(source);
      int lastModRev = DeltaVarIntCodec.decodeSigned(source);
      long hash = 0;
      if (resourceConfiguration.hashType != HashType.NONE) {
        hash = source.readLong();
      }
      boolean isCompressed = source.readByte() == 1;
      int length = DeltaVarIntCodec.decodeSigned(source);
      byte[] value = new byte[length];
      source.read(value);
      return new ObjectNamedStringNode(recordID, parentKey, rightSiblingKey, leftSiblingKey, nameKey, pathNodeKey,
          prevRev, lastModRev, hash, value, resourceConfiguration.nodeHashFunction, deweyID, isCompressed, null);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final ObjectNamedStringNode node = (ObjectNamedStringNode) record;
      final long nodeKey = node.getNodeKey();
      DeltaVarIntCodec.encodeDelta(sink, node.getParentKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getRightSiblingKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getLeftSiblingKey(), nodeKey);
      DeltaVarIntCodec.encodeSigned(sink, node.getNameKey());
      DeltaVarIntCodec.encodeDelta(sink, node.getPathNodeKey(), nodeKey);
      DeltaVarIntCodec.encodeSigned(sink, node.getPreviousRevisionNumber());
      DeltaVarIntCodec.encodeSigned(sink, node.getLastModifiedRevisionNumber());
      if (resourceConfiguration.hashType != HashType.NONE) {
        sink.writeLong(node.getHash());
      }
      sink.writeByte(node.isCompressed()
          ? (byte) 1
          : (byte) 0);
      final byte[] value = node.getRawValueWithoutDecompression();
      final byte[] actual = value != null
          ? value
          : new byte[0];
      DeltaVarIntCodec.encodeSigned(sink, actual.length);
      sink.write(actual);
    }

    @Override
    public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },

  /**
   * JSON fused object-key + null value (single record per {@code {"name": null}}).
   */
  OBJECT_NAMED_NULL((byte) 51) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      long parentKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      long rightSiblingKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      long leftSiblingKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      int nameKey = DeltaVarIntCodec.decodeSigned(source);
      long pathNodeKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      int prevRev = DeltaVarIntCodec.decodeSigned(source);
      int lastModRev = DeltaVarIntCodec.decodeSigned(source);
      long hash = 0;
      if (resourceConfiguration.hashType != HashType.NONE) {
        hash = source.readLong();
      }
      return new ObjectNamedNullNode(recordID, parentKey, rightSiblingKey, leftSiblingKey, nameKey, pathNodeKey,
          prevRev, lastModRev, hash, resourceConfiguration.nodeHashFunction, deweyID);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final ObjectNamedNullNode node = (ObjectNamedNullNode) record;
      final long nodeKey = node.getNodeKey();
      DeltaVarIntCodec.encodeDelta(sink, node.getParentKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getRightSiblingKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getLeftSiblingKey(), nodeKey);
      DeltaVarIntCodec.encodeSigned(sink, node.getNameKey());
      DeltaVarIntCodec.encodeDelta(sink, node.getPathNodeKey(), nodeKey);
      DeltaVarIntCodec.encodeSigned(sink, node.getPreviousRevisionNumber());
      DeltaVarIntCodec.encodeSigned(sink, node.getLastModifiedRevisionNumber());
      if (resourceConfiguration.hashType != HashType.NONE) {
        sink.writeLong(node.getHash());
      }
    }

    @Override
    public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },

  /**
   * JSON fused object-key + nested object value (single record per {@code {"name": { ... }}}).
   * Internal node carrying first/last child + child/desc count.
   *
   * <p>
   * <b>Phase 1 stub</b>: kindId reserved and class plumbing exists, but no factory/shredder path
   * emits this kind yet. Wire serialize/deserialize throw — if reached, treat as a bug.
   *
   * <h2>Wire layout</h2>
   * 
   * <pre>
   * Field order (offset-table indices for the slotted-page heap encoding):
   *   0 parentKey         (delta-varint)
   *   1 rightSiblingKey   (delta-varint)
   *   2 leftSiblingKey    (delta-varint)
   *   3 firstChildKey     (delta-varint)
   *   4 lastChildKey      (delta-varint)
   *   5 nameKey           (signed varint)
   *   6 pathNodeKey       (delta-varint)
   *   7 previousRevision  (signed varint)
   *   8 lastModifiedRev   (signed varint)
   *   9 hash              (fixed 8 bytes)
   *  10 childCount        (signed long varint)
   *  11 descendantCount   (signed long varint)
   * </pre>
   */
  OBJECT_NAMED_OBJECT((byte) 52) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      final long parentKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      final long rightSiblingKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      final long leftSiblingKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      final long firstChildKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      final long lastChildKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      final int nameKey = DeltaVarIntCodec.decodeSigned(source);
      final long pathNodeKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      final int prevRev = DeltaVarIntCodec.decodeSigned(source);
      final int lastModRev = DeltaVarIntCodec.decodeSigned(source);
      long hash = 0L;
      if (resourceConfiguration.hashType != HashType.NONE) {
        hash = source.readLong();
      }
      final long childCount = DeltaVarIntCodec.decodeSignedLong(source);
      final long descendantCount = DeltaVarIntCodec.decodeSignedLong(source);
      return new ObjectNamedObjectNode(recordID, parentKey, rightSiblingKey, leftSiblingKey, firstChildKey,
          lastChildKey, nameKey, pathNodeKey, prevRev, lastModRev, hash, childCount, descendantCount,
          resourceConfiguration.nodeHashFunction, deweyID);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final ObjectNamedObjectNode node = (ObjectNamedObjectNode) record;
      final long nodeKey = node.getNodeKey();
      DeltaVarIntCodec.encodeDelta(sink, node.getParentKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getRightSiblingKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getLeftSiblingKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getFirstChildKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getLastChildKey(), nodeKey);
      DeltaVarIntCodec.encodeSigned(sink, node.getNameKey());
      DeltaVarIntCodec.encodeDelta(sink, node.getPathNodeKey(), nodeKey);
      DeltaVarIntCodec.encodeSigned(sink, node.getPreviousRevisionNumber());
      DeltaVarIntCodec.encodeSigned(sink, node.getLastModifiedRevisionNumber());
      if (resourceConfiguration.hashType != HashType.NONE) {
        sink.writeLong(node.getHash());
      }
      DeltaVarIntCodec.encodeSignedLong(sink, node.getChildCount());
      DeltaVarIntCodec.encodeSignedLong(sink, node.getDescendantCount());
    }

    @Override
    public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },

  /**
   * JSON fused object-key + nested array value (single record per {@code {"name": [ ... ]}}).
   * Internal node carrying first/last child + child/desc count.
   *
   * <p>
   * <b>Phase 1 stub</b>: kindId reserved and class plumbing exists, but no factory/shredder path
   * emits this kind yet. Wire serialize/deserialize throw — if reached, treat as a bug.
   *
   * <p>
   * Field layout identical to {@link #OBJECT_NAMED_OBJECT}.
   */
  OBJECT_NAMED_ARRAY((byte) 53) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      final long parentKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      final long rightSiblingKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      final long leftSiblingKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      final long firstChildKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      final long lastChildKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      final int nameKey = DeltaVarIntCodec.decodeSigned(source);
      final long pathNodeKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      final int prevRev = DeltaVarIntCodec.decodeSigned(source);
      final int lastModRev = DeltaVarIntCodec.decodeSigned(source);
      long hash = 0L;
      if (resourceConfiguration.hashType != HashType.NONE) {
        hash = source.readLong();
      }
      final long childCount = DeltaVarIntCodec.decodeSignedLong(source);
      final long descendantCount = DeltaVarIntCodec.decodeSignedLong(source);
      return new ObjectNamedArrayNode(recordID, parentKey, rightSiblingKey, leftSiblingKey, firstChildKey, lastChildKey,
          nameKey, pathNodeKey, prevRev, lastModRev, hash, childCount, descendantCount,
          resourceConfiguration.nodeHashFunction, deweyID);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final ObjectNamedArrayNode node = (ObjectNamedArrayNode) record;
      final long nodeKey = node.getNodeKey();
      DeltaVarIntCodec.encodeDelta(sink, node.getParentKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getRightSiblingKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getLeftSiblingKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getFirstChildKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getLastChildKey(), nodeKey);
      DeltaVarIntCodec.encodeSigned(sink, node.getNameKey());
      DeltaVarIntCodec.encodeDelta(sink, node.getPathNodeKey(), nodeKey);
      DeltaVarIntCodec.encodeSigned(sink, node.getPreviousRevisionNumber());
      DeltaVarIntCodec.encodeSigned(sink, node.getLastModifiedRevisionNumber());
      if (resourceConfiguration.hashType != HashType.NONE) {
        sink.writeLong(node.getHash());
      }
      DeltaVarIntCodec.encodeSignedLong(sink, node.getChildCount());
      DeltaVarIntCodec.encodeSignedLong(sink, node.getDescendantCount());
    }

    @Override
    public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },

  /**
   * JSON string value node.
   */
  STRING_VALUE((byte) 30) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      // STRUCTURAL FIELDS FIRST (for lazy singleton optimization)
      long parentKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      long rightSiblingKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      long leftSiblingKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      // LAZY FIELDS (metadata + value)
      int prevRev = DeltaVarIntCodec.decodeSigned(source);
      int lastModRev = DeltaVarIntCodec.decodeSigned(source);
      // Compression flag (1 byte: 0 = none, 1 = FSST)
      boolean isCompressed = source.readByte() == 1;
      int length = DeltaVarIntCodec.decodeSigned(source);
      byte[] value = new byte[length];
      source.read(value);
      // Note: fsstSymbolTable will be set by the page after deserialization if needed
      return new StringNode(recordID, parentKey, prevRev, lastModRev, rightSiblingKey, leftSiblingKey, 0, value,
          resourceConfiguration.nodeHashFunction, deweyID, isCompressed, null);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final StringNode node = (StringNode) record;
      final long nodeKey = node.getNodeKey();
      // STRUCTURAL FIELDS FIRST
      DeltaVarIntCodec.encodeDelta(sink, node.getParentKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getRightSiblingKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getLeftSiblingKey(), nodeKey);
      // LAZY FIELDS (metadata + value)
      DeltaVarIntCodec.encodeSigned(sink, node.getPreviousRevisionNumber());
      DeltaVarIntCodec.encodeSigned(sink, node.getLastModifiedRevisionNumber());
      // Compression flag (1 byte: 0 = none, 1 = FSST)
      sink.writeByte(node.isCompressed()
          ? (byte) 1
          : (byte) 0);
      // Use raw value without decompression to preserve compression
      final byte[] value = node.getRawValueWithoutDecompression();
      DeltaVarIntCodec.encodeSigned(sink, value.length);
      sink.write(value);
    }

    @Override
    public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },

  /**
   * JSON boolean value node.
   */
  BOOLEAN_VALUE((byte) 27) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      // STRUCTURAL FIELDS FIRST (for lazy singleton optimization)
      long parentKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      long rightSiblingKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      long leftSiblingKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      // LAZY FIELDS (metadata + value)
      int prevRev = DeltaVarIntCodec.decodeSigned(source);
      int lastModRev = DeltaVarIntCodec.decodeSigned(source);
      boolean value = source.readBoolean();
      return new BooleanNode(recordID, parentKey, prevRev, lastModRev, rightSiblingKey, leftSiblingKey, 0, value,
          resourceConfiguration.nodeHashFunction, deweyID);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final BooleanNode node = (BooleanNode) record;
      final long nodeKey = node.getNodeKey();
      // STRUCTURAL FIELDS FIRST
      DeltaVarIntCodec.encodeDelta(sink, node.getParentKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getRightSiblingKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getLeftSiblingKey(), nodeKey);
      // LAZY FIELDS (metadata + value)
      DeltaVarIntCodec.encodeSigned(sink, node.getPreviousRevisionNumber());
      DeltaVarIntCodec.encodeSigned(sink, node.getLastModifiedRevisionNumber());
      sink.writeBoolean(node.getValue());
    }

    @Override
    public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },

  /**
   * JSON number value node.
   */
  NUMBER_VALUE((byte) 28) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      // STRUCTURAL FIELDS FIRST (for lazy singleton optimization)
      long parentKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      long rightSiblingKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      long leftSiblingKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      // LAZY FIELDS (metadata + value)
      int prevRev = DeltaVarIntCodec.decodeSigned(source);
      int lastModRev = DeltaVarIntCodec.decodeSigned(source);
      Number value = deserializeNumber(source);
      return new NumberNode(recordID, parentKey, prevRev, lastModRev, rightSiblingKey, leftSiblingKey, 0, value,
          resourceConfiguration.nodeHashFunction, deweyID);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final NumberNode node = (NumberNode) record;
      final long nodeKey = node.getNodeKey();
      // STRUCTURAL FIELDS FIRST
      DeltaVarIntCodec.encodeDelta(sink, node.getParentKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getRightSiblingKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getLeftSiblingKey(), nodeKey);
      // LAZY FIELDS (metadata + value)
      DeltaVarIntCodec.encodeSigned(sink, node.getPreviousRevisionNumber());
      DeltaVarIntCodec.encodeSigned(sink, node.getLastModifiedRevisionNumber());
      serializeNumber(node.getValue(), sink);
    }

    @Override
    public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },
  /**
   * JSON null node.
   */
  NULL_VALUE((byte) 29) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      // STRUCTURAL FIELDS FIRST (for lazy singleton optimization)
      long parentKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      long rightSiblingKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      long leftSiblingKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      // LAZY FIELDS (metadata)
      int prevRev = DeltaVarIntCodec.decodeSigned(source);
      int lastModRev = DeltaVarIntCodec.decodeSigned(source);
      return new NullNode(recordID, parentKey, prevRev, lastModRev, rightSiblingKey, leftSiblingKey, 0,
          resourceConfiguration.nodeHashFunction, deweyID);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final NullNode node = (NullNode) record;
      final long nodeKey = node.getNodeKey();
      // STRUCTURAL FIELDS FIRST
      DeltaVarIntCodec.encodeDelta(sink, node.getParentKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getRightSiblingKey(), nodeKey);
      DeltaVarIntCodec.encodeDelta(sink, node.getLeftSiblingKey(), nodeKey);
      // LAZY FIELDS (metadata)
      DeltaVarIntCodec.encodeSigned(sink, node.getPreviousRevisionNumber());
      DeltaVarIntCodec.encodeSigned(sink, node.getLastModifiedRevisionNumber());
    }

    @Override
    public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },
  /**
   * Node kind is document root.
   */
  // Virtualize document root node?
  JSON_DOCUMENT((byte) 31) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      final LongHashFunction hashFunction = resourceConfiguration.nodeHashFunction;
      final long firstChildKey = DeltaVarIntCodec.decodeDelta(source, recordID);
      final long childCount = firstChildKey == Fixed.NULL_NODE_KEY.getStandardProperty()
          ? 0
          : 1;
      final long descendantCount = DeltaVarIntCodec.decodeSignedLong(source);

      return new JsonDocumentRootNode(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), firstChildKey, firstChildKey, // lastChildKey
                                                                                                                   // same
                                                                                                                   // as
                                                                                                                   // firstChildKey
                                                                                                                   // for
                                                                                                                   // document
                                                                                                                   // root
          childCount, descendantCount, hashFunction);
    }

    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final JsonDocumentRootNode node = (JsonDocumentRootNode) record;
      DeltaVarIntCodec.encodeDelta(sink, node.getFirstChildKey(), node.getNodeKey());
      DeltaVarIntCodec.encodeSignedLong(sink, node.getDescendantCount());
    }
  },

  HASH_ENTRY((byte) 32) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      return new HashEntryNode(recordID, source.readInt(), source.readUtf8());
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final HashEntryNode node = (HashEntryNode) record;
      sink.writeInt(node.getKey());
      sink.writeUtf8(node.getValue());
    }
  },

  HASH_NAME_COUNT_TO_NAME_ENTRY((byte) 33) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      return new HashCountEntryNode(recordID, source.readInt());
    }

    public DataRecord deserialize(final MemorySegment segment, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      // HashCountEntryNode doesn't have MemorySegment constructor support yet
      throw new UnsupportedOperationException(
          "HASH_NAME_COUNT_TO_NAME_ENTRY MemorySegment deserialization not implemented");
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final HashCountEntryNode node = (HashCountEntryNode) record;
      sink.writeInt(node.getValue());
    }
  },

  DEWEY_ID_NODE((byte) 34) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      return new DeweyIDNode(recordID, new SirixDeweyID(deweyID));
    }

    public DataRecord deserialize(final MemorySegment segment, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      // DeweyIDNode uses simple constructor with recordID and deweyID
      return new DeweyIDNode(recordID, new SirixDeweyID(deweyID));
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {}
  },

  REVISION_REFERENCES_NODE((byte) 35) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      final boolean isCompressed = source.readBoolean();
      // Use int for length to support nodes with many revision references (> 255)
      final var length = source.readInt();
      final var revisions = new int[length];
      for (int i = 0; i < length; i++) {
        revisions[i] = source.readInt();
      }
      final int[] uncompressedRevisions;
      if (isCompressed) {
        uncompressedRevisions = INTEGRATED_INT_COMPRESSOR.uncompress(revisions);
      } else {
        uncompressedRevisions = revisions;
      }
      return new RevisionReferencesNode(recordID, uncompressedRevisions);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final var revisionRefNode = (RevisionReferencesNode) record;
      final var revisions = revisionRefNode.getRevisions();
      final int[] compressedRevisions;
      if (revisions.length > 10) {
        compressedRevisions = INTEGRATED_INT_COMPRESSOR.compress(revisions);
        sink.writeBoolean(true);
      } else {
        compressedRevisions = revisions;
        sink.writeBoolean(false);
      }
      // Use int for length to support nodes with many revision references (> 255)
      sink.writeInt(compressedRevisions.length);
      for (int compressedRevision : compressedRevisions) {
        sink.writeInt(compressedRevision);
      }
    }
  },

  /**
   * Projection-index leaf chunk. Wraps the serialised
   * {@link io.sirix.index.projection.ProjectionIndexRowGroupPage} byte[] so the projection index can
   * live as a HOT sub-tree rooted at {@code RevisionRootPage#getProjectionPageReference}. The
   * {@code nodeKey} is the sequential leaf index; payload length is varint-free (plain int prefix) —
   * leaves are typically in the 4–20 KB range.
   */
  /**
   * One FSST symbol table, held in the name dictionary's trie so that it is copy-on-write versioned
   * alongside everything else. See {@link FsstSymbolTableNode} for why a symbol table cannot live on
   * the pages that use it.
   */
  FSST_SYMBOL_TABLE((byte) 36) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      final int length = source.readInt();
      if (length <= 0) {
        throw new IllegalStateException("FSST symbol table record " + recordID + " declares a length of " + length
            + "; an absent table is encoded by omitting the record, not by an empty one");
      }
      final byte[] table = new byte[length];
      source.read(table);
      return new FsstSymbolTableNode(recordID, table);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final FsstSymbolTableNode node = (FsstSymbolTableNode) record;
      final byte[] table = node.getTable();
      sink.writeInt(table.length);
      sink.write(table);
    }
  },

  /**
   * One immutable value of a global projection dictionary. Its reverse radix bucket maps the stable
   * dictionary id to this record's densely reserved persistence key.
   */
  VALUE_DICTIONARY_ENTRY((byte) 37) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      final int length = source.readInt();
      if (length < 0) {
        throw new IllegalStateException(
            "Value dictionary entry " + recordID + " declares a negative length of " + length);
      }
      if (length > ValueDictionaryEntryNode.MAX_VALUE_LENGTH) {
        throw new IllegalStateException("Value dictionary entry " + recordID + " exceeds the safe V0 payload limit of "
            + ValueDictionaryEntryNode.MAX_VALUE_LENGTH + " bytes");
      }
      if ((long) length > source.remaining()) {
        throw new IllegalStateException("Value dictionary entry " + recordID + " exceeds its remaining wire payload");
      }
      final byte[] value = new byte[length];
      source.read(value);
      return ValueDictionaryEntryNode.takeOwnership(recordID, value);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final ValueDictionaryEntryNode node = (ValueDictionaryEntryNode) record;
      final byte[] value = node.borrowedValueForSerialization();
      sink.writeInt(value.length);
      sink.write(value);
    }
  },

  /**
   * One block of a global projection value dictionary's forward directory. See
   * {@link ValueDictionaryDirectoryNode}.
   */
  VALUE_DICTIONARY_DIRECTORY((byte) 38) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      final int count = source.readInt();
      if (count <= 0 || count > ValueDictionaryDirectoryNode.ENTRIES_PER_BLOCK
          || (long) count * (Long.BYTES + Integer.BYTES) > source.remaining()) {
        throw new IllegalStateException(
            "Value dictionary directory block " + recordID + " declares " + count + " entries");
      }
      final long[] hashes = new long[count];
      for (int i = 0; i < count; i++) {
        hashes[i] = source.readLong();
      }
      final int[] ids = new int[count];
      for (int i = 0; i < count; i++) {
        ids[i] = source.readInt();
      }
      return new ValueDictionaryDirectoryNode(recordID, hashes, ids);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final ValueDictionaryDirectoryNode node = (ValueDictionaryDirectoryNode) record;
      final long[] hashes = node.getHashes();
      final int[] ids = node.getIds();
      sink.writeInt(hashes.length);
      for (final long hash : hashes) {
        sink.writeLong(hash);
      }
      for (final int id : ids) {
        sink.writeInt(id);
      }
    }
  },

  /**
   * The header of one global projection value dictionary namespace. See
   * {@link ValueDictionaryHeaderNode}.
   */
  VALUE_DICTIONARY_HEADER((byte) 39) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      final int version = source.readInt();
      if (version != ValueDictionaryHeaderNode.VERSION) {
        // A resource written by a newer build must make this one DECLINE, never misparse: the
        // payload behind an unknown version cannot be interpreted, so stop reading here. The
        // source is scoped to this record (NodeStorageEngineReader#getDataRecord), so the
        // unread remainder harms nothing.
        return ValueDictionaryHeaderNode.unknownLayout(recordID, version);
      }
      final int entryCount = source.readInt();
      final long forwardRootKey = source.readLong();
      final long reverseRootKey = source.readLong();
      final int generation = source.readInt();
      // The ordered-prefix boundary is APPENDED and read DEFENSIVELY, which is the whole mechanism
      // that lets a P2 build read a pre-P2 resource without a version bump. The slot is exactly
      // sized (PageLayout#allocateHeap allocates precisely `size`; getRecordOnlyLength returns the
      // record's own byte length), so a header written before this field leaves remaining() == 0
      // here, and 0 is its semantically correct value: the streaming mint's ids are in intern order,
      // so its ordered prefix is genuinely empty.
      // KILL-SWITCH MACHINERY, NOT COMPATIBILITY MACHINERY — and the distinction decides when this
      // goes away. Reading pre-P2 resources is NOT why it exists: the project has no users and no
      // installed base, so a layout may change outright and the databases get rebuilt. It exists so
      // that with -Dsirix.projection.globalDict.rank=false a dictionary the pass never touched is
      // BYTE-IDENTICAL to what this arm wrote before segment 1, which is what makes "lands disabled"
      // provable rather than merely asserted. Eliminating the delta beats accounting for it.
      // EXPIRY: when segment 1 is enabled by default and the kill switch retires, this trailer's
      // remaining job is gone — collapse it to an unconditional write and read. Do not copy the
      // pattern onto new fields; nothing else here needs it.
      // The two fields are read as a PAIR because reading them independently would misparse a zero
      // prefix count followed by a non-zero index key. That pairing has already earned itself: a
      // database written one layout earlier declined LOUDLY on the header invariant instead of
      // silently reporting an intern-ordered dictionary as fully ordered.
      final boolean hasRankTrailer = source.remaining() >= Integer.BYTES + Long.BYTES;
      final int orderedPrefixCount = hasRankTrailer
          ? source.readInt()
          : 0;
      final long blockIndexKey = hasRankTrailer
          ? source.readLong()
          : 0L;
      return new ValueDictionaryHeaderNode(recordID, version, entryCount, forwardRootKey, reverseRootKey, generation,
          orderedPrefixCount, blockIndexKey);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final ValueDictionaryHeaderNode node = (ValueDictionaryHeaderNode) record;
      if (!node.isCurrentLayout()) {
        throw new IllegalStateException(
            "refusing to re-serialize a value dictionary header with unknown layout version " + node.getVersion()
                + " — this build cannot reconstruct its payload");
      }
      sink.writeInt(node.getVersion());
      sink.writeInt(node.getEntryCount());
      sink.writeLong(node.getForwardRootKey());
      sink.writeLong(node.getReverseRootKey());
      sink.writeInt(node.getGeneration());
      // Written UNCONDITIONALLY against a DEFENSIVE read. The asymmetry is deliberate and is what
      // buys pre-P2 compatibility without a version bump: old readers are gone (this is the only
      // reader), old data is short and reads as 0.
      // Emitted only when the dictionary has something to say, so the kill switch's "off" state is
      // byte-for-byte what this arm wrote before the rank pass existed.
      if (node.getOrderedPrefixCount() != 0 || node.getBlockIndexKey() != 0L) {
        sink.writeInt(node.getOrderedPrefixCount());
        sink.writeLong(node.getBlockIndexKey());
      }
    }
  },

  VALUE_DICTIONARY_SEGMENT((byte) 40) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      return new ValueDictionarySegmentNode(recordID, source.readLong(), source.readInt(), source.readInt(),
          source.readLong(), source.readLong(), source.readInt());
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final ValueDictionarySegmentNode node = (ValueDictionarySegmentNode) record;
      sink.writeLong(node.getPreviousSegmentKey());
      sink.writeInt(node.getFirstId());
      sink.writeInt(node.getEntryCount());
      sink.writeLong(node.getEntryBase());
      sink.writeLong(node.getDirectoryBase());
      sink.writeInt(node.getDirectoryBlockCount());
    }
  },

  VALUE_DICTIONARY_RADIX((byte) 41) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      final byte indexKind = source.readByte();
      final byte depth = source.readByte();
      final int count = Short.toUnsignedInt(source.readShort());
      // count <= 0 matches the three sibling dictionary kinds: an empty radix node is never
      // written (an empty dictionary has no radix at all), so zero is corruption, and accepting
      // it yielded a silent ID_ABSENT instead of a loud refusal.
      if (count <= 0 || count > ValueDictionaryRadixNode.FANOUT
          || (long) count * (Byte.BYTES + Long.BYTES) > source.remaining()) {
        throw new IllegalStateException("invalid value dictionary radix child count " + count);
      }
      final byte[] childSlots = new byte[count];
      final long[] childKeys = new long[count];
      for (int i = 0; i < count; i++) {
        childSlots[i] = source.readByte();
        childKeys[i] = source.readLong();
      }
      return new ValueDictionaryRadixNode(recordID, indexKind, depth, childSlots, childKeys);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final ValueDictionaryRadixNode node = (ValueDictionaryRadixNode) record;
      sink.writeByte(node.getIndexKind());
      sink.writeByte(node.getDepth());
      final byte[] childSlots = node.getChildSlots();
      final long[] childKeys = node.getSparseChildKeys();
      sink.writeShort((short) childKeys.length);
      for (int index = 0; index < childKeys.length; index++) {
        sink.writeByte(childSlots[index]);
        sink.writeLong(childKeys[index]);
      }
    }
  },

  VALUE_DICTIONARY_HASH_BUCKET((byte) 42) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      final int bucket = source.readInt();
      final byte secondaryDepth = source.readByte();
      final long secondaryPrefix = source.readLong();
      final long nextBucketKey = source.readLong();
      final int count = source.readInt();
      if (count <= 0 || count > 128 || (long) count * (Long.BYTES + Integer.BYTES) > source.remaining()) {
        throw new IllegalStateException("invalid value dictionary hash bucket size");
      }
      final long[] hashes = new long[count];
      final int[] ids = new int[count];
      for (int i = 0; i < count; i++)
        hashes[i] = source.readLong();
      for (int i = 0; i < count; i++)
        ids[i] = source.readInt();
      return new ValueDictionaryHashBucketNode(recordID, bucket, secondaryDepth, secondaryPrefix, nextBucketKey, hashes,
          ids);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final ValueDictionaryHashBucketNode node = (ValueDictionaryHashBucketNode) record;
      final long[] hashes = node.getHashes();
      final int[] ids = node.getIds();
      sink.writeInt(node.getBucket());
      sink.writeByte(node.getSecondaryDepth());
      sink.writeLong(node.getSecondaryPrefix());
      sink.writeLong(node.getNextBucketKey());
      sink.writeInt(hashes.length);
      for (final long hash : hashes)
        sink.writeLong(hash);
      for (final int id : ids)
        sink.writeInt(id);
    }
  },

  VALUE_DICTIONARY_VALUE_BUCKET((byte) 43) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      final int firstId = source.readInt();
      final int count = source.readInt();
      final int blockCount = source.readInt();
      final int spillCount = source.readInt();
      if (firstId <= 0 || count <= 0 || count > ValueDictionaryValueBucketNode.VALUES_PER_BUCKET || blockCount < 0
          || spillCount < 0 || blockCount > count || spillCount > count
          || (long) firstId + count - 1L > Integer.MAX_VALUE) {
        throw new IllegalStateException("invalid value dictionary value bucket header");
      }
      // Bound the claimed directory against what remains BEFORE allocating anything.
      if ((long) blockCount * (2L * Integer.BYTES + Long.BYTES)
          + (long) spillCount * (Integer.BYTES + Long.BYTES) > source.remaining()) {
        throw new IllegalStateException("value dictionary bucket directory overruns its record");
      }
      final int[] blockFirstIds = new int[blockCount];
      final int[] blockCounts = new int[blockCount];
      final long[] blockKeys = new long[blockCount];
      for (int i = 0; i < blockCount; i++) {
        blockFirstIds[i] = source.readInt();
        blockCounts[i] = source.readInt();
        blockKeys[i] = source.readLong();
      }
      final int[] spillIds = new int[spillCount];
      final long[] spillKeys = new long[spillCount];
      for (int i = 0; i < spillCount; i++) {
        spillIds[i] = source.readInt();
        spillKeys[i] = source.readLong();
      }
      return ValueDictionaryValueBucketNode.takeOwnership(recordID, firstId, count, blockFirstIds, blockCounts,
          blockKeys, spillIds, spillKeys);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final ValueDictionaryValueBucketNode node = (ValueDictionaryValueBucketNode) record;
      final int blockCount = node.blockCount();
      final int spillCount = node.spillCount();
      sink.writeInt(node.getFirstId());
      sink.writeInt(node.size());
      sink.writeInt(blockCount);
      sink.writeInt(spillCount);
      for (int i = 0; i < blockCount; i++) {
        sink.writeInt(node.blockFirstId(i));
        sink.writeInt(node.blockIdCount(i));
        sink.writeLong(node.blockKey(i));
      }
      for (int i = 0; i < spillCount; i++) {
        sink.writeInt(node.spillId(i));
        sink.writeLong(node.spillKeyAt(i));
      }
    }
  },

  /**
   * One bounded, immutable packed sub-block of a reverse value bucket — see
   * {@link ValueDictionaryValueBlockNode} for why a bucket holds several of these rather than one
   * record per id or one capped blob.
   */
  VALUE_DICTIONARY_VALUE_BLOCK((byte) 59) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      final int firstId = source.readInt();
      final int count = source.readInt();
      // A NEGATIVE count discriminates the front-coded + LZ77 form, a NEGATIVE byteLength the
      // front-coded one; both are decoded by ValueDictionaryValueBlockCodec into the same
      // (offsets, bytes) shape this arm has always produced. Intercepted before the plain form's
      // validation because that validation is what the sign would otherwise trip.
      if (count < 0 && count != Integer.MIN_VALUE && firstId > 0
          && -count <= ValueDictionaryValueBlockNode.MAX_BLOCK_VALUES) {
        return ValueDictionaryValueBlockCodec.deserializeCompressed(source, recordID, firstId, -count);
      }
      final int byteLength = source.readInt();
      if (firstId <= 0 || count <= 0 || count > ValueDictionaryValueBlockNode.MAX_BLOCK_VALUES
          || byteLength == Integer.MIN_VALUE || Math.abs(byteLength) > ValueDictionaryValueBlockNode.MAX_BLOCK_BYTES) {
        throw new IllegalStateException("invalid value dictionary value block header");
      }
      // The id range must fit an int: a corrupt firstId near MAX_VALUE would otherwise construct a
      // block whose last id wraps negative and silently stops covering anything.
      if ((long) firstId + count - 1L > Integer.MAX_VALUE) {
        throw new IllegalStateException("value dictionary value block overruns the id space");
      }
      if (byteLength < 0) {
        return ValueDictionaryValueBlockCodec.deserializeFrontCoded(source, recordID, firstId, count, -byteLength);
      }
      // Bound the claimed sizes against what the source can actually still hold BEFORE allocating,
      // so a corrupt or truncated record cannot make this reserve arbitrary memory.
      if ((long) count * Integer.BYTES + byteLength > source.remaining()) {
        throw new IllegalStateException("value dictionary value block overruns its record");
      }
      final int[] offsets = new int[count + 1];
      for (int i = 1; i <= count; i++) {
        offsets[i] = source.readInt();
      }
      final byte[] bytes = new byte[byteLength];
      if (byteLength > 0) {
        source.read(bytes);
      }
      return ValueDictionaryValueBlockNode.takeOwnership(recordID, firstId, offsets, bytes);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final ValueDictionaryValueBlockNode node = (ValueDictionaryValueBlockNode) record;
      // Election is per block, by measured size; with the kill switch off no candidate is built and
      // this arm emits exactly the plain form it always did.
      if (ValueDictionaryValueBlockCodec.ENABLED && ValueDictionaryValueBlockCodec.serializeIfSmaller(sink, node)) {
        return;
      }
      final byte[] bytes = node.rawBytes();
      final int count = node.size();
      sink.writeInt(node.getFirstId());
      sink.writeInt(count);
      sink.writeInt(bytes.length);
      // Indexed, not copied: offsets[0] is always 0 and is reconstructed on read.
      for (int i = 1; i <= count; i++) {
        sink.writeInt(node.offsetAt(i));
      }
      sink.write(bytes);
    }
  },

  /**
   * The separator array over a rank-ordered dictionary's value blocks — see
   * {@link ValueDictionaryBlockIndexNode} for why a sorted-string dictionary needs one.
   */
  VALUE_DICTIONARY_BLOCK_INDEX((byte) 60) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      final int count = source.readInt();
      if (count <= 0 || count > ValueDictionaryBlockIndexNode.MAX_ENTRIES) {
        throw new IllegalStateException("invalid value dictionary block index count " + count);
      }
      // Two ints per entry is the floor of the varint framing, so a claimed count that cannot fit
      // in what remains is refused BEFORE the arrays are sized.
      if ((long) count * 2L > source.remaining()) {
        throw new IllegalStateException("value dictionary block index overruns its record");
      }
      final int[] firstIds = new int[count];
      final int[] offsets = new int[count + 1];
      int firstId = 0;
      long separatorBytes = 0;
      for (int i = 0; i < count; i++) {
        firstId += readUnsignedVarInt(source);
        firstIds[i] = firstId;
        final int length = readUnsignedVarInt(source);
        separatorBytes += length;
        if (separatorBytes > source.remaining()) {
          throw new IllegalStateException("value dictionary block index separators overrun its record");
        }
        offsets[i + 1] = (int) separatorBytes;
      }
      final byte[] separators = new byte[(int) separatorBytes];
      if (separatorBytes > 0) {
        source.read(separators, 0, (int) separatorBytes);
      }
      return ValueDictionaryBlockIndexNode.takeOwnership(recordID, firstIds, separators, offsets);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final ValueDictionaryBlockIndexNode node = (ValueDictionaryBlockIndexNode) record;
      final int count = node.size();
      sink.writeInt(count);
      int previousFirstId = 0;
      for (int i = 0; i < count; i++) {
        writeUnsignedVarInt(sink, node.firstId(i) - previousFirstId);
        previousFirstId = node.firstId(i);
        writeUnsignedVarInt(sink, node.separatorLength(i));
      }
      final byte[] separators = node.separatorBytes();
      if (separators.length > 0) {
        sink.write(separators);
      }
    }

    @Override
    public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },

  PROJECTION_INDEX_LEAF((byte) 44) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      final int length = source.readInt();
      if (length < 0 || (long) length > source.remaining()) {
        throw new IllegalStateException("Negative PROJECTION_INDEX_LEAF payload length: " + length);
      }
      final byte[] payload = new byte[length];
      if (length > 0)
        source.read(payload);
      return new ProjectionIndexRowGroupRecord(recordID, payload);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final ProjectionIndexRowGroupRecord leaf = (ProjectionIndexRowGroupRecord) record;
      final byte[] payload = leaf.getPayload();
      sink.writeInt(payload.length);
      if (payload.length > 0)
        sink.write(payload);
    }

    @Override
    public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },

  VALUE_DICTIONARY_COLLISION((byte) 45) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      if (source.remaining() < 2L * Integer.BYTES + 2L * Long.BYTES) {
        throw new IllegalStateException("truncated value dictionary collision node");
      }
      return new ValueDictionaryCollisionNode(recordID, source.readInt(), source.readInt(), source.readLong(),
          source.readLong());
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final ValueDictionaryCollisionNode node = (ValueDictionaryCollisionNode) record;
      sink.writeInt(node.getId());
      sink.writeInt(node.getHeight());
      sink.writeLong(node.getLeftKey());
      sink.writeLong(node.getRightKey());
    }
  },

  /**
   * HNSW vector graph node storing an embedding vector and per-layer neighbor lists.
   */
  VECTOR_NODE((byte) 56) {
    /** Current serialization format version. */
    private static final byte CURRENT_VERSION = 1;

    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      // Version byte (added in version 1).
      final byte version = source.readByte();
      if (version != 1) {
        throw new IllegalStateException("Unknown VECTOR_NODE version: " + version);
      }
      // Document node key (delta from recordID).
      final long documentNodeKey = recordID + getVarLong(source);
      // Vector dimension and raw float data.
      final int dimension = (int) getVarLong(source);
      final float[] vector = new float[dimension];
      for (int i = 0; i < dimension; i++) {
        vector[i] = source.readFloat();
      }
      // HNSW layer.
      final int maxLayer = source.readByte() & 0xFF;
      if (maxLayer > 30) {
        throw new IllegalStateException("Invalid maxLayer value: " + maxLayer + " (max allowed: 30)");
      }
      // Per-layer neighbor lists (delta-encoded, sorted).
      final long[][] neighbors = new long[maxLayer + 1][];
      final int[] neighborCounts = new int[maxLayer + 1];
      for (int layer = 0; layer <= maxLayer; layer++) {
        final int count = (int) getVarLong(source);
        neighborCounts[layer] = count;
        if (count > 0) {
          final long[] keys = new long[count];
          long prev = 0;
          for (int i = 0; i < count; i++) {
            prev += getVarLong(source);
            keys[i] = prev;
          }
          neighbors[layer] = keys;
        }
      }
      // Revision number.
      final int previousRevision = (int) getVarLong(source);
      // Deleted flag (version 1).
      final boolean deleted = source.readByte() != 0;
      return new VectorNode(recordID, documentNodeKey, vector, maxLayer, neighbors, neighborCounts, previousRevision,
          deleted);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final VectorNode node = (VectorNode) record;
      final long nodeKey = node.getNodeKey();
      // Version byte.
      sink.writeByte(CURRENT_VERSION);
      // Document node key (delta from nodeKey).
      putVarLong(sink, node.getDocumentNodeKey() - nodeKey);
      // Vector dimension and raw float data.
      final float[] vector = node.getVector();
      final int dimension = vector.length;
      putVarLong(sink, dimension);
      for (int i = 0; i < dimension; i++) {
        sink.writeFloat(vector[i]);
      }
      // HNSW layer.
      final int maxLayer = node.getMaxLayer();
      sink.writeByte((byte) maxLayer);
      // Per-layer neighbor lists (delta-encoded, sorted).
      for (int layer = 0; layer <= maxLayer; layer++) {
        final int count = node.getNeighborCount(layer);
        putVarLong(sink, count);
        if (count > 0) {
          final long[] keys = node.getNeighbors(layer);
          // Sort a copy for delta encoding.
          final long[] sorted = Arrays.copyOf(keys, count);
          Arrays.sort(sorted);
          long prev = 0;
          for (int i = 0; i < count; i++) {
            putVarLong(sink, sorted[i] - prev);
            prev = sorted[i];
          }
        }
      }
      // Revision number.
      putVarLong(sink, node.getPreviousRevisionNumber());
      // Deleted flag.
      sink.writeByte(node.isDeleted()
          ? (byte) 1
          : (byte) 0);
    }

    @Override
    public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },

  /**
   * HNSW vector index metadata node (always at nodeKey 0). Stores graph-level metadata: entry point,
   * max level, dimension, distance type, node count.
   */
  VECTOR_INDEX_METADATA((byte) 58) {
    /** Current serialization format version. */
    private static final byte CURRENT_VERSION = 1;

    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      // Version byte.
      final byte version = source.readByte();
      if (version != 1) {
        throw new IllegalStateException("Unknown VECTOR_INDEX_METADATA version: " + version);
      }
      final long entryPointKey = source.readLong();
      final int maxLevel = (int) getVarLong(source);
      final int dimension = (int) getVarLong(source);
      // Distance type: length-prefixed UTF-8.
      final int dtLen = (int) getVarLong(source);
      final byte[] dtBytes = new byte[dtLen];
      source.read(dtBytes, 0, dtLen);
      final String distanceType = new String(dtBytes, StandardCharsets.UTF_8);
      final long nodeCount = source.readLong();
      final int previousRevision = (int) getVarLong(source);
      return new VectorIndexMetadataNode(recordID, entryPointKey, maxLevel, dimension, distanceType, nodeCount,
          previousRevision);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final VectorIndexMetadataNode node = (VectorIndexMetadataNode) record;
      // Version byte.
      sink.writeByte(CURRENT_VERSION);
      sink.writeLong(node.getEntryPointKey());
      putVarLong(sink, node.getMaxLevel());
      putVarLong(sink, node.getDimension());
      // Distance type: length-prefixed UTF-8.
      final byte[] dtBytes = node.getDistanceType().getBytes(StandardCharsets.UTF_8);
      putVarLong(sink, dtBytes.length);
      sink.write(dtBytes, 0, dtBytes.length);
      sink.writeLong(node.getNodeCount());
      putVarLong(sink, node.getPreviousRevisionNumber());
    }

    @Override
    public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },

  /**
   * Node type not known.
   */
  UNKNOWN((byte) 22) {
    @Override
    public DataRecord deserialize(final BytesIn<?> source, final long recordID, final byte[] deweyID,
        final ResourceConfiguration resourceConfiguration) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  };

  /**
   * Identifier.
   */
  private final byte id;

  /**
   * Mapping of keys -> nodes. Sized to cover the full unsigned-byte id space so the lookup in
   * {@link #getKind(byte)} needs no range check — unmapped slots stay {@code null} and are rejected
   * there.
   */
  private static final NodeKind[] INSTANCEFORID = new NodeKind[256];

  /**
   * Shared empty-name placeholder; the real name is resolved from the name page via the name keys.
   * {@link QNm} is immutable, so sharing one instance is safe and avoids an allocation per
   * deserialized/created name node.
   */
  public static final QNm EMPTY_QNM = new QNm("");

  static {
    for (final NodeKind node : values()) {
      INSTANCEFORID[node.id & 0xFF] = node;
    }
  }

  /**
   * Constructor.
   *
   * @param id unique identifier
   */
  NodeKind(final byte id) {
    this.id = id;
  }

  /**
   * Get the nodeKind.
   *
   * @return the unique kind
   */
  public byte getId() {
    return id;
  }

  /**
   * Public method to get the related node based on the identifier.
   *
   * @param id the identifier for the node
   * @return the related node
   * @throws IllegalStateException if the id maps to no known kind (a record written by a newer
   *         version of the format, or a corrupt kind byte)
   */
  public static NodeKind getKind(final byte id) {
    final NodeKind kind = INSTANCEFORID[id & 0xFF];
    if (kind == null) {
      throw unknownKind(id);
    }
    return kind;
  }

  private static IllegalStateException unknownKind(final byte id) {
    return new IllegalStateException("Unknown NodeKind id: " + (id & 0xFF)
        + " — record written by a newer version of the storage format, or corrupt data");
  }

  /**
   * True for all fused {@code OBJECT_NAMED_*} kinds — records that carry a field nameKey +
   * pathNodeKey and play the "object field" role in diff/index/scan code.
   *
   * <p>
   * Phase 4 — the legacy {@code OBJECT_KEY} branch was removed; the predicate now covers exclusively
   * the 6 fused kinds (48-53). Naming retained for source-compatibility with call sites; semantics
   * now mean "is a fused named record". Predicate remains a {@code ||} chain of identity comparisons
   * (zero-alloc, no autoboxing).
   */
  public boolean playsObjectKeyRole() {
    return this == OBJECT_NAMED_BOOLEAN || this == OBJECT_NAMED_NUMBER || this == OBJECT_NAMED_STRING
        || this == OBJECT_NAMED_NULL || this == OBJECT_NAMED_OBJECT || this == OBJECT_NAMED_ARRAY;
  }

  /**
   * True for the iter#30 fused leaf kinds (primitive payload only). Excludes the Phase 1
   * structural-fused kinds, since callers of this predicate dispatch into per-leaf-type switches
   * (e.g. removeName) that would need new cases for OBJECT/ARRAY structural shape.
   */
  public boolean isFusedObjectNamed() {
    return this == OBJECT_NAMED_BOOLEAN || this == OBJECT_NAMED_NUMBER || this == OBJECT_NAMED_STRING
        || this == OBJECT_NAMED_NULL;
  }

  /**
   * True for the Phase 1 fused structural kinds (OBJECT_NAMED_OBJECT, OBJECT_NAMED_ARRAY). Phase 1
   * reserves the kindIds 52/53 and the recognizer; no runtime code path emits them yet, so this
   * predicate returns true only when fed an explicit constant.
   */
  public boolean isFusedStructural() {
    return this == OBJECT_NAMED_OBJECT || this == OBJECT_NAMED_ARRAY;
  }

  /**
   * True for any fused named record (primitive leaves 48-51 OR structural 52-53). Useful for
   * predicates that classify "any record carrying both a fieldname and an inline payload or sub-tree"
   * without caring about the payload shape.
   */
  public boolean isFusedAnyNamed() {
    return this == OBJECT_NAMED_BOOLEAN || this == OBJECT_NAMED_NUMBER || this == OBJECT_NAMED_STRING
        || this == OBJECT_NAMED_NULL || this == OBJECT_NAMED_OBJECT || this == OBJECT_NAMED_ARRAY;
  }

  @Override
  public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
    return null;
  }

  @Override
  public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
      ResourceConfiguration resourceConfig) {}

  private static long getHash(final BytesIn<?> source, final ResourceConfiguration resourceConfiguration) {
    final long hashCode;
    if (resourceConfiguration.hashType == HashType.NONE)
      hashCode = 0L;
    else
      hashCode = source.readLong();
    return hashCode;
  }

  private static void serializeStructNodeJsonValueNode(BytesOut<?> sink, StructNode node) {
    // Write fixed-size longs instead of variable-length encoded values
    sink.writeLong(node.getRightSiblingKey());
    sink.writeLong(node.getLeftSiblingKey());
  }

  private static StructNodeDelegate deserializeStructNodeJsonValueNode(BytesIn<?> source, NodeDelegate nodeDel) {
    // Read fixed-size longs directly instead of variable-length values
    final long rightSibling = source.readLong();
    final long leftSibling = source.readLong();
    final long firstChild = Fixed.NULL_NODE_KEY.getStandardProperty();
    final long lastChild = Fixed.NULL_NODE_KEY.getStandardProperty();
    final long childCount = 0;
    final long descendantCount = 0;

    return new StructNodeDelegate(nodeDel, firstChild, lastChild, rightSibling, leftSibling, childCount,
        descendantCount);
  }

  // Removed: JSON nodes are now fully MemorySegment-based and don't use StructNodeDelegate
  // private static StructNodeDelegate deserializeObjectOrArrayStructDelegate(BytesIn<?> source,
  // ResourceConfiguration config, NodeDelegate nodeDel) {
  // return new StructNodeDelegate(nodeDel, (MemorySegment) source.getUnderlying(),
  // nodeDel.getNodeKey(), config);
  // }

  private static void serializeJsonObjectOrArrayStructDelegate(BytesOut<?> sink, StructNode node,
      ResourceConfiguration config) {
    final boolean storeChildCount = config.storeChildCount();

    sink.writeLong(node.getRightSiblingKey());
    sink.writeLong(node.getLeftSiblingKey());
    sink.writeLong(node.getFirstChildKey());
    sink.writeLong(node.getLastChildKey());
    if (storeChildCount) {
      sink.writeLong(node.getChildCount());
    }
    if (config.hashType != HashType.NONE) {
      sink.writeLong(node.getDescendantCount());
    }
  }

  private static NodeDelegate deserializeNodeDelegateWithoutIDs(final BytesIn<?> source, final long recordID,
      final ResourceConfiguration resourceConfiguration) {
    // Read variable-length encoded offset value
    final long parentKey = recordID - getVarLong(source);
    final int previousRevision = source.readInt();
    final int lastModifiedRevision = source.readInt();
    final LongHashFunction hashFunction = resourceConfiguration.nodeHashFunction;
    return new NodeDelegate(recordID, parentKey, hashFunction, previousRevision, lastModifiedRevision,
        (SirixDeweyID) null);
  }

  private static NodeDelegate deserializeNodeDelegate(final BytesIn<?> source, final long recordID, final byte[] id,
      final ResourceConfiguration resourceConfiguration) {
    // Read fixed-size long instead of variable-length encoded value
    final long parentKey = source.readLong();
    final int previousRevision = source.readInt();
    final int lastModifiedRevision = source.readInt();
    final LongHashFunction hashFunction = resourceConfiguration.nodeHashFunction;
    return new NodeDelegate(recordID, parentKey, hashFunction, previousRevision, lastModifiedRevision, id);
  }

  private static void serializeDelegate(final NodeDelegate nodeDel, final BytesOut<?> sink) {
    sink.writeLong(nodeDel.getParentKey());
    sink.writeInt(nodeDel.getPreviousRevisionNumber());
    sink.writeInt(nodeDel.getLastModifiedRevisionNumber());
  }

  /**
   * Serialize node delegate without IDs, writing parent key as offset from node key. This matches
   * deserializeNodeDelegateWithoutIDs which expects: parentKey = recordID - offset Uses
   * variable-length encoding for the offset.
   */
  private static void serializeDelegateWithoutIDs(final NodeDelegate nodeDel, final BytesOut<?> sink) {
    final long offset = nodeDel.getNodeKey() - nodeDel.getParentKey();
    putVarLong(sink, offset);
    sink.writeInt(nodeDel.getPreviousRevisionNumber());
    sink.writeInt(nodeDel.getLastModifiedRevisionNumber());
  }

  /**
   * Get a properly-sized MemorySegment slice for a node from the current position. Uses UNALIGNED
   * value layouts, so no alignment requirements.
   *
   * @param source the BytesIn source
   * @param size the exact size of the node data in bytes
   * @return a MemorySegment slice of the specified size
   */
  private static MemorySegment getSegmentSlice(final BytesIn<?> source, final long size) {
    MemorySegment fullSegment = (MemorySegment) source.getUnderlying();
    // The underlying segment is already sliced to start at current position
    // So we just need to slice it further to the exact node size
    long availableSize = fullSegment.byteSize();
    if (size > availableSize) {
      throw new IllegalStateException(
          "Calculated node size " + size + " exceeds available segment size " + availableSize);
    }
    MemorySegment slice = fullSegment.asSlice(0, size);
    // Advance the source position by the node size
    source.position(source.position() + size);
    return slice;
  }

  private static void serializeStructDelegate(final NodeKind kind, final StructNodeDelegate nodeDel,
      final BytesOut<?> sink, final ResourceConfiguration config) {
    final var isValueNode = kind == NodeKind.NUMBER_VALUE || kind == NodeKind.STRING_VALUE
        || kind == NodeKind.BOOLEAN_VALUE || kind == NodeKind.NULL_VALUE;

    final boolean storeChildCount = config.storeChildCount();

    sink.writeLong(nodeDel.getRightSiblingKey());
    sink.writeLong(nodeDel.getLeftSiblingKey());

    if (!isValueNode) {
      sink.writeLong(nodeDel.getFirstChildKey());
      sink.writeLong(nodeDel.getLastChildKey());
      if (storeChildCount) {
        sink.writeLong(nodeDel.getChildCount());
      }

      if (config.hashType != HashType.NONE) {
        sink.writeLong(nodeDel.getDescendantCount());
      }
    }
  }

  private static StructNodeDelegate deserializeStructDel(final NodeKind kind, final NodeDelegate nodeDel,
      final BytesIn<?> source, final ResourceConfiguration config) {
    final boolean storeChildNodes = config.storeChildCount();

    final var isValueNode = kind == NodeKind.NUMBER_VALUE || kind == NodeKind.STRING_VALUE
        || kind == NodeKind.BOOLEAN_VALUE || kind == NodeKind.NULL_VALUE;

    final long rightSibling;
    final long leftSibling;
    final long firstChild;
    final long lastChild;
    final long childCount;
    final long descendantCount;

    // Read fixed-size longs directly instead of variable-length values
    rightSibling = source.readLong();
    leftSibling = source.readLong();

    if (isValueNode) {
      firstChild = Fixed.NULL_NODE_KEY.getStandardProperty();
      lastChild = Fixed.NULL_NODE_KEY.getStandardProperty();
      childCount = 0;
      descendantCount = 0;
    } else {
      firstChild = source.readLong();
      lastChild = source.readLong();
      if (!storeChildNodes) {
        childCount = 0;
      } else {
        childCount = source.readLong();
      }
      if (config.hashType == HashType.NONE) {
        descendantCount = 0;
      } else {
        descendantCount = source.readLong();
      }
    }

    return new StructNodeDelegate(nodeDel, firstChild, lastChild, rightSibling, leftSibling, childCount,
        descendantCount);
  }


  /**
   * Serializing the {@link ValueNodeDelegate} instance.
   *
   * @param valueDel to be serialized
   * @param sink to serialize to
   */
  private static void serializeValDelegate(final ValueNodeDelegate valueDel, final BytesOut<?> sink) {
    final boolean isCompressed = valueDel.isCompressed();
    sink.writeByte(isCompressed
        ? (byte) 1
        : (byte) 0);
    final byte[] value = isCompressed
        ? valueDel.getCompressed()
        : valueDel.getRawValue();
    sink.writeInt(value.length);
    sink.write(value);
  }

  private static void writeHash(final BytesOut<?> sink, final long hashCode) {
    sink.writeLong(hashCode);
  }

  /**
   * Serializes a Number value to a BytesOut sink using varint encoding for integers. Supports Double,
   * Float, Integer, Long, BigInteger, and BigDecimal.
   * 
   * <h2>Encoding Format</h2>
   * <ul>
   * <li>Type 0: Double (8 bytes) - full IEEE 754 precision needed</li>
   * <li>Type 1: Float (4 bytes) - full IEEE 754 precision needed</li>
   * <li>Type 2: Integer (varint, 1-5 bytes) - zigzag encoded for signed values</li>
   * <li>Type 3: Long (varint, 1-10 bytes) - zigzag encoded for signed values</li>
   * <li>Type 4: BigInteger (length-prefixed bytes)</li>
   * <li>Type 5: BigDecimal (BigInteger + varint scale)</li>
   * </ul>
   * 
   * <h2>Space Savings</h2>
   * <ul>
   * <li>Small integers (-64 to 63): 2 bytes instead of 5 bytes (60% savings)</li>
   * <li>Medium integers (-8192 to 8191): 3 bytes instead of 5 bytes (40% savings)</li>
   * <li>Small longs (-64 to 63): 2 bytes instead of 9 bytes (78% savings)</li>
   * </ul>
   * 
   * @param value the number to serialize
   * @param sink the sink to write to
   */
  public static void serializeNumber(final Number value, final BytesOut<?> sink) {
    switch (value) {
      case final Double val -> {
        sink.writeByte((byte) 0);
        sink.writeDouble(val);
      }
      case final Float val -> {
        sink.writeByte((byte) 1);
        sink.writeFloat(val);
      }
      case final Integer val -> {
        sink.writeByte((byte) 2);
        // Use zigzag + varint encoding for compact representation
        DeltaVarIntCodec.encodeSigned(sink, val);
      }
      case final Long val -> {
        sink.writeByte((byte) 3);
        // Use zigzag + varint encoding for compact representation
        DeltaVarIntCodec.encodeSignedLong(sink, val);
      }
      case final BigInteger bigInteger -> {
        sink.writeByte((byte) 4);
        serializeBigInteger(sink, bigInteger);
      }
      case final BigDecimal bigDecimal -> {
        sink.writeByte((byte) 5);
        final BigInteger bigInt = bigDecimal.unscaledValue();
        final int scale = bigDecimal.scale();
        serializeBigInteger(sink, bigInt);
        // Use varint for scale (typically small positive integer)
        DeltaVarIntCodec.encodeSigned(sink, scale);
      }
      case null, default -> throw new AssertionError("Type not known.");
    }
  }

  /**
   * Deserializes a Number value from a BytesIn source. Supports Double, Float, Integer, Long,
   * BigInteger, and BigDecimal.
   * 
   * <p>
   * Integers and Longs are decoded from zigzag + varint encoding for compact storage.
   * 
   * @param source the source to read from
   * @return the deserialized Number
   */
  public static Number deserializeNumber(final BytesIn<?> source) {
    final var valueType = source.readByte();

    return switch (valueType) {
      case 0 -> source.readDouble();
      case 1 -> source.readFloat();
      case 2 -> DeltaVarIntCodec.decodeSigned(source); // Varint-encoded Integer
      case 3 -> DeltaVarIntCodec.decodeSignedLong(source); // Varint-encoded Long
      case 4 -> deserializeBigInteger(source);
      case 5 -> {
        final BigInteger bigInt = deserializeBigInteger(source);
        final int scale = DeltaVarIntCodec.decodeSigned(source); // Varint-encoded scale
        yield new BigDecimal(bigInt, scale);
      }
      default -> throw new AssertionError("Type not known.");
    };
  }

  private static void serializeBigInteger(final BytesOut<?> sink, final BigInteger bigInteger) {
    final byte[] bytes = bigInteger.toByteArray();
    sink.writeStopBit(bytes.length);
    sink.write(bytes);
  }

  private static BigInteger deserializeBigInteger(final BytesIn<?> source) {
    final byte[] bytes = new byte[(int) source.readStopBit()];
    source.read(bytes);
    return new BigInteger(bytes);
  }

  /**
   * Simple DumbNode just for testing the {@link KeyValueLeafPage}s.
   *
   * @param nodeKey Node key.
   * @author Sebastian Graf, University of Konstanz
   * @author Johannes Lichtenberger
   */
  public record DumbNode(long nodeKey) implements DataRecord {

    /**
     * Simple constructor.
     *
     * @param nodeKey to be set
     */
    public DumbNode {
    }

    @Override
    public NodeKind getKind() {
      return NodeKind.NULL;
    }

    @Override
    public long getNodeKey() {
      return nodeKey;
    }

    @Override
    public SirixDeweyID getDeweyID() {
      return null;
    }

    @Override
    public byte[] getDeweyIDAsBytes() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getLastModifiedRevisionNumber() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getPreviousRevisionNumber() {
      throw new UnsupportedOperationException();
    }
  }

  /** LEB128, shared by the arms that frame variable-width tables. */
  private static void writeUnsignedVarInt(final BytesOut<?> sink, final int value) {
    int remaining = value;
    while ((remaining & ~0x7F) != 0) {
      sink.writeByte((byte) ((remaining & 0x7F) | 0x80));
      remaining >>>= 7;
    }
    sink.writeByte((byte) remaining);
  }

  private static int readUnsignedVarInt(final BytesIn<?> source) {
    int result = 0;
    for (int shift = 0; shift <= 28; shift += 7) {
      final int read = source.readByte() & 0xFF;
      result |= (read & 0x7F) << shift;
      if ((read & 0x80) == 0) {
        if (result < 0) {
          throw new IllegalStateException("varint overflows an int");
        }
        return result;
      }
    }
    throw new IllegalStateException("varint is not terminated");
  }

}
