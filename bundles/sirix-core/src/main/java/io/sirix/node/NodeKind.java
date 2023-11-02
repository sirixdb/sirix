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

import io.sirix.access.trx.node.HashType;
import io.sirix.node.json.*;
import io.sirix.node.json.NullNode;
import io.sirix.node.xml.*;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.DeweyIdSerializer;
import io.sirix.node.interfaces.StructNode;
import io.sirix.service.xml.xpath.AtomicValue;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.hashing.LongHashFunction;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.module.Namespaces;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.PageReadOnlyTrx;
import io.sirix.index.AtomicUtil;
import io.sirix.index.path.summary.PathNode;
import io.sirix.index.redblacktree.RBNodeKey;
import io.sirix.index.redblacktree.RBNodeValue;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.node.delegates.NameNodeDelegate;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.delegates.StructNodeDelegate;
import io.sirix.node.delegates.ValueNodeDelegate;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static io.sirix.node.Utils.getVarLong;
import static io.sirix.node.Utils.putVarLong;

/**
 * Enumeration for different nodes. All nodes are determined by a unique id.
 *
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 */
@SuppressWarnings({ "DuplicatedCode", "unchecked" })
public enum NodeKind implements DeweyIdSerializer {

  /**
   * Node kind is element.
   */
  ELEMENT((byte) 1, ElementNode.class) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, pageReadTrx);

      // Struct delegate.
      final StructNodeDelegate structDel =
          deserializeStructDel(this, nodeDel, source, pageReadTrx.getResourceSession().getResourceConfig());

      final long hashCode = getHash(source, pageReadTrx);

      // Name delegate.
      final NameNodeDelegate nameDel = deserializeNameDelegate(nodeDel, source);

      // Attributes.
      final int attrCount = source.readInt();
      final LongList attrKeys = new LongArrayList(attrCount);
      for (int i = 0; i < attrCount; i++) {
        final long nodeKey = source.readLong();
        attrKeys.add(nodeKey);
      }

      // Namespaces.
      final int nsCount = source.readInt();
      final LongList namespaceKeys = new LongArrayList(nsCount);
      for (int i = 0; i < nsCount; i++) {
        namespaceKeys.add(source.readLong());
      }

      return new ElementNode(hashCode, structDel, nameDel, attrKeys, namespaceKeys, new QNm(""));
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      final ElementNode node = (ElementNode) record;
      serializeDelegate(node.getNodeDelegate(), sink);
      serializeStructDelegate(this,
                              node.getStructNodeDelegate(),
                              sink,
                              pageReadTrx.getResourceSession().getResourceConfig());
      if (pageReadTrx.getResourceSession().getResourceConfig().hashType != HashType.NONE) {
        writeHash(sink, node.getHash());
      }
      serializeNameDelegate(node.getNameNodeDelegate(), sink);
      sink.writeInt(node.getAttributeCount());
      for (int i = 0, attCount = node.getAttributeCount(); i < attCount; i++) {
        final long key = node.getAttributeKey(i);
        sink.writeLong(key);
      }
      sink.writeInt(node.getNamespaceCount());
      for (int i = 0, nspCount = node.getNamespaceCount(); i < nspCount; i++) {
        sink.writeLong(node.getNamespaceKey(i));
      }
    }
  },

  /**
   * Node kind is attribute.
   */
  ATTRIBUTE((byte) 2, AttributeNode.class) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, pageReadTrx);

      // Name delegate.
      final NameNodeDelegate nameDel = deserializeNameDelegate(nodeDel, source);

      // Val delegate.
      final boolean isCompressed = source.readByte() == (byte) 1;
      final byte[] vals = new byte[source.readInt()];
      source.read(vals, 0, vals.length);
      final ValueNodeDelegate valDel = new ValueNodeDelegate(nodeDel, vals, isCompressed);

      // Returning an instance.
      return new AttributeNode(nodeDel, nameDel, valDel, new QNm(""));
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      final AttributeNode node = (AttributeNode) record;
      serializeDelegate(node.getNodeDelegate(), sink);
      serializeNameDelegate(node.getNameNodeDelegate(), sink);
      serializeValDelegate(node.getValNodeDelegate(), sink);
    }
  },

  /**
   * Node kind is namespace.
   */
  NAMESPACE((byte) 13, NamespaceNode.class) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, pageReadTrx);

      // Name delegate.
      final NameNodeDelegate nameDel = deserializeNameDelegate(nodeDel, source);

      return new NamespaceNode(nodeDel, nameDel, null);
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      final NamespaceNode node = (NamespaceNode) record;
      serializeDelegate(node.getNodeDelegate(), sink);
      serializeNameDelegate(node.getNameNodeDelegate(), sink);
    }
  },

  /**
   * Node kind is text.
   */
  TEXT((byte) 3, TextNode.class) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, pageReadTrx);

      // Val delegate.
      final boolean isCompressed = source.readByte() == (byte) 1;
      final byte[] vals = new byte[source.readInt()];
      source.read(vals, 0, vals.length);
      final ValueNodeDelegate valDel = new ValueNodeDelegate(nodeDel, vals, isCompressed);

      // Struct delegate.
      final long nodeKey = nodeDel.getNodeKey();
      final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
                                                                  Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                  nodeKey - getVarLong(source),
                                                                  nodeKey - getVarLong(source),
                                                                  0L,
                                                                  0L);

      // Returning an instance.
      return new TextNode(valDel, structDel);
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      final TextNode node = (TextNode) record;
      serializeDelegate(node.getNodeDelegate(), sink);
      serializeValDelegate(node.getValNodeDelegate(), sink);
      final StructNodeDelegate del = node.getStructNodeDelegate();
      final long nodeKey = node.getNodeKey();
      putVarLong(sink, nodeKey - del.getRightSiblingKey());
      putVarLong(sink, nodeKey - del.getLeftSiblingKey());
    }
  },

  /**
   * Node kind is processing instruction.
   */
  PROCESSING_INSTRUCTION((byte) 7, PINode.class) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, pageReadTrx);

      // Struct delegate.
      final StructNodeDelegate structDel =
          deserializeStructDel(this, nodeDel, source, pageReadTrx.getResourceSession().getResourceConfig());

      // Name delegate.
      final NameNodeDelegate nameDel = deserializeNameDelegate(nodeDel, source);

      // Val delegate.
      final boolean isCompressed = source.readByte() == (byte) 1;
      final byte[] vals = new byte[source.readInt()];
      source.read(vals, 0, vals.length);
      final ValueNodeDelegate valDel = new ValueNodeDelegate(nodeDel, vals, isCompressed);

      // Returning an instance.
      return new PINode(structDel, nameDel, valDel, pageReadTrx);
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      final PINode node = (PINode) record;
      serializeDelegate(node.getNodeDelegate(), sink);
      serializeStructDelegate(this,
                              node.getStructNodeDelegate(),
                              sink,
                              pageReadTrx.getResourceSession().getResourceConfig());
      serializeNameDelegate(node.getNameNodeDelegate(), sink);
      serializeValDelegate(node.getValNodeDelegate(), sink);
    }
  },

  /**
   * Node kind is comment.
   */
  COMMENT((byte) 8, CommentNode.class) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, pageReadTrx);

      // Val delegate.
      final boolean isCompressed = source.readByte() == (byte) 1;
      final byte[] vals = new byte[source.readInt()];
      source.read(vals, 0, vals.length);
      final ValueNodeDelegate valDel = new ValueNodeDelegate(nodeDel, vals, isCompressed);

      // Struct delegate.
      final long nodeKey = nodeDel.getNodeKey();
      final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
                                                                  Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                  nodeKey - getVarLong(source),
                                                                  nodeKey - getVarLong(source),
                                                                  0L,
                                                                  0L);

      // Returning an instance.
      return new CommentNode(valDel, structDel);
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      final CommentNode node = (CommentNode) record;
      serializeDelegate(node.getNodeDelegate(), sink);
      serializeValDelegate(node.getValNodeDelegate(), sink);
      final StructNodeDelegate del = node.getStructNodeDelegate();
      final long nodeKey = node.getNodeKey();
      putVarLong(sink, nodeKey - del.getRightSiblingKey());
      putVarLong(sink, nodeKey - del.getLeftSiblingKey());
    }
  },

  /**
   * Node kind is document root.
   */
  // Virtualize document root node?
  XML_DOCUMENT((byte) 9, XmlDocumentRootNode.class) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      final var revisionNumber = pageReadTrx.getRevisionNumber();
      final LongHashFunction hashFunction = pageReadTrx.getResourceSession().getResourceConfig().nodeHashFunction;

      final NodeDelegate nodeDel = new NodeDelegate(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(),
                                                    Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                    hashFunction,
                                                    Constants.NULL_REVISION_NUMBER,
                                                    revisionNumber,
                                                    SirixDeweyID.newRootID().toBytes());
      final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
                                                                  getVarLong(source),
                                                                  Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                  Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                  source.readByte() == ((byte) 0) ? 0 : 1,
                                                                  pageReadTrx.getResourceSession()
                                                                             .getResourceConfig().hashType
                                                                      == HashType.NONE ? 0 : source.readLong());
      return new XmlDocumentRootNode(nodeDel, structDel);
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      final XmlDocumentRootNode node = (XmlDocumentRootNode) record;
      putVarLong(sink, node.getFirstChildKey());
      sink.writeByte(node.hasFirstChild() ? (byte) 1 : (byte) 0);
      if (pageReadTrx.getResourceSession().getResourceConfig().hashType != HashType.NONE)
        sink.writeLong(node.getDescendantCount());
    }

    @Override
    public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
      return null;
    }

    @Override
    public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
        ResourceConfiguration resourceConfig) {
    }
  },

  /**
   * Whitespace text.
   */
  WHITESPACE((byte) 4, null) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
      return null;
    }

    @Override
    public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
        ResourceConfiguration resourceConfig) {
    }
  },

  /**
   * Node kind is deleted node.
   */
  DELETE((byte) 5, DeletedNode.class) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      final var revisionNumber = pageReadTrx.getRevisionNumber();
      final LongHashFunction hashFunction = pageReadTrx.getResourceSession().getResourceConfig().nodeHashFunction;
      final NodeDelegate delegate = new NodeDelegate(recordID, 0, hashFunction, 0, revisionNumber, (SirixDeweyID) null);
      return new DeletedNode(delegate);
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
    }

    @Override
    public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
      return null;
    }

    @Override
    public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
        ResourceConfiguration resourceConfig) {
    }
  },

  /**
   * NullNode to support the Null Object pattern.
   */
  NULL((byte) 6, NullNode.class) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
      return null;
    }

    @Override
    public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
        ResourceConfiguration resourceConfig) {
    }
  },

  /**
   * Dumb node for testing.
   */
  DUMB((byte) 20, DumbNode.class) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      return new DumbNode(recordID);
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
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
  ATOMIC((byte) 15, AtomicValue.class) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
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
  PATH((byte) 16, PathNode.class) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegateWithoutIDs(source, recordID, pageReadTrx);

      // Struct delegate.
      final StructNodeDelegate structDel =
          deserializeStructDel(this, nodeDel, source, pageReadTrx.getResourceSession().getResourceConfig());

      // Name delegate.
      final NameNodeDelegate nameDel = deserializeNameDelegate(nodeDel, source);

      final NodeKind kind = NodeKind.getKind(source.readByte());

      return new PathNode(null, nodeDel, structDel, nameDel, kind, source.readInt(), source.readInt());
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      final PathNode node = (PathNode) record;
      serializeDelegate(node.getNodeDelegate(), sink);
      serializeStructDelegate(this,
                              node.getStructNodeDelegate(),
                              sink,
                              pageReadTrx.getResourceSession().getResourceConfig());
      serializeNameDelegate(node.getNameNodeDelegate(), sink);
      sink.writeByte(node.getPathKind().getId());
      sink.writeInt(node.getReferences());
      sink.writeInt(node.getLevel());
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
   * Node kind is a CAS-RB node.
   */
  CASRB((byte) 17, RBNodeKey.class) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      final int valueSize = source.readInt();
      final byte[] value = new byte[valueSize];
      source.read(value, 0, valueSize);
      final int typeSize = source.readInt();
      final byte[] type = new byte[typeSize];
      source.read(type, 0, typeSize);

      final Type atomicType = resolveType(new String(type, Constants.DEFAULT_ENCODING));

      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegateWithoutIDs(source, recordID, pageReadTrx);
      final long leftChild = getVarLong(source);
      final long rightChild = getVarLong(source);
      final long pathNodeKey = getVarLong(source);
      final boolean isChanged = source.readBoolean();
      final long valueNodeKey = source.readLong();
      final Atomic atomic = AtomicUtil.fromBytes(value, atomicType);
      final var node =
          new RBNodeKey<>(new CASValue(atomic, atomicType, pathNodeKey), valueNodeKey, nodeDel);

      node.setLeftChildKey(leftChild);
      node.setRightChildKey(rightChild);
      node.setChanged(isChanged);
      return node;
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      final RBNodeKey<CASValue> node = (RBNodeKey<CASValue>) record;
      final CASValue key = node.getKey();
      final byte[] textValue = key.getValue();
      assert textValue != null;
      sink.writeInt(textValue.length);
      sink.write(textValue);
      final byte[] type = key.getType().toString().getBytes(Constants.DEFAULT_ENCODING);
      sink.writeInt(type.length);
      sink.write(type);

      serializeDelegate(node.getNodeDelegate(), sink);
      putVarLong(sink, node.getLeftChildKey());
      putVarLong(sink, node.getRightChildKey());
      putVarLong(sink, key.getPathNodeKey());
      sink.writeBoolean(node.isChanged());
      sink.writeLong(node.getValueNodeKey());
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

    private Type resolveType(final String s) {
      final QNm name =
          new QNm(Namespaces.XS_NSURI, Namespaces.XS_PREFIX, s.substring(Namespaces.XS_PREFIX.length() + 1));
      for (final Type type : Type.builtInTypes) {
        if (type.getName().getLocalName().equals(name.getLocalName())) {
          return type;
        }
      }
      throw new IllegalStateException("Unknown content type: " + name);
    }
  },

  /**
   * Node kind is a PATH-RB node.
   */
  PATHRB((byte) 18, RBNodeKey.class) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      final long key = getVarLong(source);
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegateWithoutIDs(source, recordID, pageReadTrx);
      final long leftChild = getVarLong(source);
      final long rightChild = getVarLong(source);
      final boolean isChanged = source.readBoolean();
      final long valueNodeKey = source.readLong();
      final RBNodeKey<Long> node = new RBNodeKey<>(key, valueNodeKey, nodeDel);
      node.setLeftChildKey(leftChild);
      node.setRightChildKey(rightChild);
      node.setChanged(isChanged);
      return node;
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      final RBNodeKey<Long> node = (RBNodeKey<Long>) record;
      putVarLong(sink, node.getKey());
      serializeDelegate(node.getNodeDelegate(), sink);
      putVarLong(sink, node.getLeftChildKey());
      putVarLong(sink, node.getRightChildKey());
      sink.writeBoolean(node.isChanged());
      sink.writeLong(node.getValueNodeKey());
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
   * Node kind is a PATH-RB node.
   */
  NAMERB((byte) 19, RBNodeKey.class) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      final byte[] nspBytes = new byte[source.readInt()];
      source.read(nspBytes);
      final byte[] prefixBytes = new byte[source.readInt()];
      source.read(prefixBytes);
      final byte[] localNameBytes = new byte[source.readInt()];
      source.read(localNameBytes);
      final QNm name = new QNm(new String(nspBytes, Constants.DEFAULT_ENCODING),
                               new String(prefixBytes, Constants.DEFAULT_ENCODING),
                               new String(localNameBytes, Constants.DEFAULT_ENCODING));
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegateWithoutIDs(source, recordID, pageReadTrx);
      final long leftChild = getVarLong(source);
      final long rightChild = getVarLong(source);
      final boolean isChanged = source.readBoolean();
      final long valueNodeKey = source.readLong();
      final RBNodeKey<QNm> node = new RBNodeKey<>(name, valueNodeKey, nodeDel);
      node.setLeftChildKey(leftChild);
      node.setRightChildKey(rightChild);
      node.setChanged(isChanged);
      return node;
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      final RBNodeKey<QNm> node = (RBNodeKey<QNm>) record;
      final byte[] nspBytes = node.getKey().getNamespaceURI().getBytes();
      sink.writeInt(nspBytes.length);
      sink.write(nspBytes);
      final byte[] prefixBytes = node.getKey().getPrefix().getBytes();
      sink.writeInt(prefixBytes.length);
      sink.write(prefixBytes);
      final byte[] localNameBytes = node.getKey().getLocalName().getBytes();
      sink.writeInt(localNameBytes.length);
      sink.write(localNameBytes);
      serializeDelegate(node.getNodeDelegate(), sink);
      putVarLong(sink, node.getLeftChildKey());
      putVarLong(sink, node.getRightChildKey());
      sink.writeBoolean(node.isChanged());
      sink.writeLong(node.getValueNodeKey());
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
   * Node kind is a value red black tree node.
   */
  RB_NODE_VALUE((byte) 55, RBNodeValue.class) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      final var nodeKeys = deserializeNodeReferences(source);
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegateWithoutIDs(source, recordID, pageReadTrx);
      return new RBNodeValue<>(new NodeReferences(nodeKeys), nodeDel);
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      final RBNodeValue<NodeReferences> node = (RBNodeValue<NodeReferences>) record;
      final NodeReferences value = node.getValue();
      final Roaring64Bitmap nodeKeys = value.getNodeKeys();
      serializeNodeReferences(sink, nodeKeys);
      serializeDelegate(node.getNodeDelegate(), sink);
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
  DEWEYIDMAPPING((byte) 23, DeweyIDMappingNode.class) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
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
  OBJECT((byte) 24, ObjectNode.class) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      var config = pageReadTrx.getResourceSession().getResourceConfig();
      final long hashCode = getHash(source, pageReadTrx);

      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, pageReadTrx);
      final StructNodeDelegate structDel = deserializeObjectOrArrayStructDelegate(source, config, nodeDel);

      // Returning an instance.
      return new ObjectNode(hashCode, structDel);
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      final ObjectNode node = (ObjectNode) record;
      var config = pageReadTrx.getResourceSession().getResourceConfig();
      if (config.hashType != HashType.NONE) {
        writeHash(sink, node.getHash());
      }
      serializeDelegate(node.getNodeDelegate(), sink);
      serializeJsonObjectOrArrayStructDelegate(sink, node, config);
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
  ARRAY((byte) 25, ArrayNode.class) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      var config = pageReadTrx.getResourceSession().getResourceConfig();
      final long hashCode = getHash(source, pageReadTrx);

      final long pathNodeKey = source.readLong();

      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, pageReadTrx);
      final StructNodeDelegate structDel = deserializeObjectOrArrayStructDelegate(source, config, nodeDel);

      // Returning an instance.
      return new ArrayNode(hashCode, structDel, pathNodeKey);
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      final ArrayNode node = (ArrayNode) record;
      var config = pageReadTrx.getResourceSession().getResourceConfig();
      if (config.hashType != HashType.NONE)
        writeHash(sink, node.getHash());
      sink.writeLong(node.getPathNodeKey());
      serializeDelegate(node.getNodeDelegate(), sink);
      serializeJsonObjectOrArrayStructDelegate(sink, node, config);
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
   * JSON object key node.
   */
  OBJECT_KEY((byte) 26, ObjectKeyNode.class) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      final long hashCode = getHash(source, pageReadTrx);

      final int nameKey = source.readInt();
      final long pathNodeKey = getVarLong(source);

      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, pageReadTrx);

      final var currKey = nodeDel.getNodeKey();

      var rightSibling = currKey - getVarLong(source);
      var leftSibling = currKey - getVarLong(source);
      var firstChild = currKey - getVarLong(source);

      var config = pageReadTrx.getResourceSession().getResourceConfig();

      final long descendantCount;

      if (config.hashType == HashType.NONE) {
        descendantCount = 0;
      } else {
        descendantCount = getVarLong(source) + 1;
      }

      // Struct delegate.
      final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
                                                                  firstChild,
                                                                  rightSibling,
                                                                  leftSibling,
                                                                  config.storeChildCount() ? 1 : 0,
                                                                  descendantCount);

      final String name = nameKey == -1 ? "" : pageReadTrx.getName(nameKey, NodeKind.OBJECT_KEY);

      // Name can be null for removed nodes (the previous record page still has the ObjectKeyNode).

      // Returning an instance.
      return new ObjectKeyNode(hashCode, structDel, nameKey, name, pathNodeKey);
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      final ObjectKeyNode node = (ObjectKeyNode) record;
      var config = pageReadTrx.getResourceSession().getResourceConfig();
      if (config.hashType != HashType.NONE) {
        writeHash(sink, node.getHash());
      }
      sink.writeInt(node.getNameKey());
      putVarLong(sink, node.getPathNodeKey());
      serializeDelegate(node.getNodeDelegate(), sink);
      final var nodeKey = node.getNodeKey();
      putVarLong(sink, nodeKey - node.getRightSiblingKey());
      putVarLong(sink, nodeKey - node.getLeftSiblingKey());
      putVarLong(sink, nodeKey - node.getFirstChildKey());
      if (config.hashType != HashType.NONE) {
        putVarLong(sink, node.getDescendantCount() - node.getChildCount());
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
   * JSON string value node.
   */
  OBJECT_STRING_VALUE((byte) 40, ObjectStringNode.class) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, pageReadTrx);

      // Val delegate.
      final boolean isCompressed = source.readByte() == (byte) 1;
      final byte[] vals = new byte[source.readInt()];
      source.read(vals, 0, vals.length);
      final ValueNodeDelegate valDel = new ValueNodeDelegate(nodeDel, vals, isCompressed);

      // Struct delegate.
      final StructNodeDelegate structDelegate = new StructNodeDelegate(nodeDel,
                                                                       Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                       Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                       Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                       Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                       0,
                                                                       0);

      // Returning an instance.
      return new ObjectStringNode(valDel, structDelegate);
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      final ObjectStringNode node = (ObjectStringNode) record;
      serializeDelegate(node.getNodeDelegate(), sink);
      serializeValDelegate(node.getValNodeDelegate(), sink);
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
  OBJECT_BOOLEAN_VALUE((byte) 41, ObjectBooleanNode.class) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      final boolean boolValue = source.readBoolean();
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, pageReadTrx);

      // Struct delegate.
      final StructNodeDelegate structDelegate = new StructNodeDelegate(nodeDel,
                                                                       Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                       Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                       Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                       Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                       0,
                                                                       0);

      // Returning an instance.
      return new ObjectBooleanNode(boolValue, structDelegate);
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      final ObjectBooleanNode node = (ObjectBooleanNode) record;
      sink.writeBoolean(node.getValue());
      serializeDelegate(node.getNodeDelegate(), sink);
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
  OBJECT_NUMBER_VALUE((byte) 42, ObjectNumberNode.class) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      final byte valueType = source.readByte();
      final Number number;

      switch (valueType) {
        case 0 -> number = source.readDouble();
        case 1 -> number = source.readFloat();
        case 2 -> number = source.readInt();
        case 3 -> number = source.readLong();
        case 4 -> number = deserializeBigInteger(source);
        case 5 -> {
          final BigInteger bigInt = deserializeBigInteger(source);
          final int scale = source.readInt();
          number = new BigDecimal(bigInt, scale);
        }
        default -> throw new AssertionError("Type not known.");
      }

      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, pageReadTrx);

      // Struct delegate.
      final StructNodeDelegate structDelegate = new StructNodeDelegate(nodeDel,
                                                                       Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                       Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                       Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                       Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                       0,
                                                                       0);

      // Returning an instance.
      return new ObjectNumberNode(number, structDelegate);
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      final ObjectNumberNode node = (ObjectNumberNode) record;
      final Number number = node.getValue();

      switch (number) {
        case Double ignored -> {
          sink.writeByte((byte) 0);
          sink.writeDouble(number.doubleValue());
        }
        case Float ignored -> {
          sink.writeByte((byte) 1);
          sink.writeFloat(number.floatValue());
        }
        case Integer ignored -> {
          sink.writeByte((byte) 2);
          sink.writeInt(number.intValue());
        }
        case Long ignored -> {
          sink.writeByte((byte) 3);
          sink.writeLong(number.longValue());
        }
        case BigInteger bigInt -> {
          sink.writeByte((byte) 4);
          sink.writeBigInteger(bigInt);
        }
        case final BigDecimal value -> {
          sink.writeByte((byte) 5);
          final BigInteger bigInt = value.unscaledValue();
          final int scale = value.scale();
          sink.writeBigInteger(bigInt);
          sink.writeInt(scale);
        }
        case null, default -> throw new AssertionError("Type not known.");
      }

      serializeDelegate(node.getNodeDelegate(), sink);
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
  OBJECT_NULL_VALUE((byte) 43, ObjectNullNode.class) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, pageReadTrx);

      // Struct delegate.
      final StructNodeDelegate structDelegate = new StructNodeDelegate(nodeDel,
                                                                       Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                       Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                       Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                       Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                       0,
                                                                       0);

      // Returning an instance.
      return new ObjectNullNode(structDelegate);
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      final ObjectNullNode node = (ObjectNullNode) record;
      serializeDelegate(node.getNodeDelegate(), sink);
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
  STRING_VALUE((byte) 30, StringNode.class) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, pageReadTrx);

      // Val delegate.
      final boolean isCompressed = source.readByte() == (byte) 1;
      final byte[] vals = new byte[source.readInt()];
      source.read(vals, 0, vals.length);
      final ValueNodeDelegate valDel = new ValueNodeDelegate(nodeDel, vals, isCompressed);

      // Struct delegate.
      final StructNodeDelegate structDel = deserializeStructNodeJsonValueNode(source, nodeDel);

      // Returning an instance.
      return new StringNode(valDel, structDel);
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      final StringNode node = (StringNode) record;
      serializeDelegate(node.getNodeDelegate(), sink);
      serializeValDelegate(node.getValNodeDelegate(), sink);
      serializeStructNodeJsonValueNode(sink, node);
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
  BOOLEAN_VALUE((byte) 27, BooleanNode.class) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      final boolean boolValue = source.readBoolean();
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, pageReadTrx);

      // Struct delegate.
      final StructNodeDelegate structDel = deserializeStructNodeJsonValueNode(source, nodeDel);

      // Returning an instance.
      return new BooleanNode(boolValue, structDel);
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      final BooleanNode node = (BooleanNode) record;
      sink.writeBoolean(node.getValue());
      serializeDelegate(node.getNodeDelegate(), sink);
      serializeStructNodeJsonValueNode(sink, node);
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
  NUMBER_VALUE((byte) 28, NumberNode.class) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      final byte valueType = source.readByte();
      final Number number = switch (valueType) {
        case 0 -> source.readDouble();
        case 1 -> source.readFloat();
        case 2 -> source.readInt();
        case 3 -> source.readLong();
        case 4 -> deserializeBigInteger(source);
        case 5 -> {
          final BigInteger bigInt = deserializeBigInteger(source);
          final int scale = source.readInt();
          yield new BigDecimal(bigInt, scale);
        }
        default -> throw new AssertionError("Type not known.");
      };

      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, pageReadTrx);
      final StructNodeDelegate structDel = deserializeStructNodeJsonValueNode(source, nodeDel);

      // Returning an instance.
      return new NumberNode(number, structDel);
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      final NumberNode node = (NumberNode) record;
      final Number number = node.getValue();

      switch (number) {
        case Double ignored -> {
          sink.writeByte((byte) 0);
          sink.writeDouble(number.doubleValue());
        }
        case Float ignored1 -> {
          sink.writeByte((byte) 1);
          sink.writeFloat(number.floatValue());
        }
        case Integer ignored2 -> {
          sink.writeByte((byte) 2);
          sink.writeInt(number.intValue());
        }
        case Long ignored3 -> {
          sink.writeByte((byte) 3);
          sink.writeLong(number.longValue());
        }
        case BigInteger bigInteger -> {
          sink.writeByte((byte) 4);
          serializeBigInteger(sink, bigInteger);
        }
        case final BigDecimal value -> {
          sink.writeByte((byte) 5);
          final BigInteger bigInt = value.unscaledValue();
          final int scale = value.scale();
          serializeBigInteger(sink, bigInt);
          sink.writeInt(scale);
        }
        case null, default -> throw new AssertionError("Type not known.");
      }

      serializeDelegate(node.getNodeDelegate(), sink);
      serializeStructNodeJsonValueNode(sink, node);
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
  NULL_VALUE((byte) 29, NullNode.class) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, pageReadTrx);

      // Struct delegate.
      final StructNodeDelegate structDel = deserializeStructNodeJsonValueNode(source, nodeDel);

      // Returning an instance.
      return new NullNode(structDel);
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      final NullNode node = (NullNode) record;
      serializeDelegate(node.getNodeDelegate(), sink);
      serializeStructNodeJsonValueNode(sink, node);
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
  JSON_DOCUMENT((byte) 31, JsonDocumentRootNode.class) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      final var revisionNumber = pageReadTrx.getRevisionNumber();
      final LongHashFunction hashFunction = pageReadTrx.getResourceSession().getResourceConfig().nodeHashFunction;

      final NodeDelegate nodeDel = new NodeDelegate(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(),
                                                    Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                    hashFunction,
                                                    Constants.NULL_REVISION_NUMBER,
                                                    revisionNumber,
                                                    SirixDeweyID.newRootID().toBytes());
      final long firstChildKey = getVarLong(source);
      final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
                                                                  firstChildKey,
                                                                  firstChildKey,
                                                                  Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                  Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                  firstChildKey
                                                                      == Fixed.NULL_NODE_KEY.getStandardProperty()
                                                                      ? 0
                                                                      : 1,
                                                                  source.readLong());
      return new JsonDocumentRootNode(nodeDel, structDel);
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      final JsonDocumentRootNode node = (JsonDocumentRootNode) record;
      putVarLong(sink, node.getFirstChildKey());
      sink.writeLong(node.getDescendantCount());
    }

    @Override
    public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
      return null;
    }

    @Override
    public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
        ResourceConfiguration resourceConfig) {
    }
  },

  HASH_ENTRY((byte) 32, HashEntryNode.class) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      return new HashEntryNode(recordID, source.readInt(), source.readUtf8());
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      final HashEntryNode node = (HashEntryNode) record;
      sink.writeInt(node.getKey());
      sink.writeUtf8(node.getValue());
    }

    @Override
    public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
      return null;
    }

    @Override
    public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
        ResourceConfiguration resourceConfig) {
    }
  },

  HASH_NAME_COUNT_TO_NAME_ENTRY((byte) 33, HashCountEntryNode.class) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      return new HashCountEntryNode(recordID, source.readInt());
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      final HashCountEntryNode node = (HashCountEntryNode) record;
      sink.writeInt(node.getValue());
    }

    @Override
    public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
      return null;
    }

    @Override
    public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
        ResourceConfiguration resourceConfig) {
    }
  },

  DEWEY_ID_NODE((byte) 34, DeweyIDNode.class) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      return new DeweyIDNode(recordID, new SirixDeweyID(deweyID));
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
    }

    @Override
    public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
      return null;
    }

    @Override
    public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
        ResourceConfiguration resourceConfig) {
    }
  },

  REVISION_REFERENCES_NODE((byte) 35, RevisionReferencesNode.class) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      final boolean isCompressed = source.readBoolean();
      final var length = source.readByte();
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
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
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
      sink.writeByte((byte) compressedRevisions.length);
      for (int compressedRevision : compressedRevisions) {
        sink.writeInt(compressedRevision);
      }
    }

    @Override
    public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
      return null;
    }

    @Override
    public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
        ResourceConfiguration resourceConfig) {
    }
  },

  /**
   * Node type not known.
   */
  UNKNOWN((byte) 22, null) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final PageReadOnlyTrx pageReadTrx) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serialize(final BytesOut<ByteBuffer> sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
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

  private static void serializeNodeReferences(BytesOut<ByteBuffer> sink, Roaring64Bitmap nodeKeys) {
    try (var outputStream = new DataOutputStream(sink.outputStream())) {
      nodeKeys.serialize(outputStream);
    } catch (IOException e) {
      throw new UncheckedIOException(e.getMessage(), e);
    }
  }

  @NonNull
  private static Roaring64Bitmap deserializeNodeReferences(BytesIn<?> source) {
    final var nodeKeys = new Roaring64Bitmap();
    try (var inputStream = new DataInputStream(source.inputStream())) {
      nodeKeys.deserialize(inputStream);
    } catch (IOException e) {
      throw new UncheckedIOException(e.getMessage(), e);
    }
    return nodeKeys;
  }

  /**
   * Identifier.
   */
  private final byte id;

  /**
   * Class.
   */
  private final Class<? extends DataRecord> clazz;

  /**
   * Mapping of keys -> nodes.
   */
  private static final Map<Byte, NodeKind> INSTANCEFORID = new HashMap<>();

  /**
   * Mapping of class -> nodes.
   */
  private static final Map<Class<? extends DataRecord>, NodeKind> INSTANCEFORCLASS = new HashMap<>();

  static {
    for (final NodeKind node : values()) {
      INSTANCEFORID.put(node.id, node);
      INSTANCEFORCLASS.put(node.clazz, node);
    }
  }

  /**
   * Constructor.
   *
   * @param id    unique identifier
   * @param clazz class
   */
  NodeKind(final byte id, final Class<? extends DataRecord> clazz) {
    this.id = id;
    this.clazz = clazz;
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
   */
  public static NodeKind getKind(final byte id) {
    return INSTANCEFORID.get(id);
  }

  /**
   * Public method to get the related node based on the class.
   *
   * @param clazz the class for the node
   * @return the related node
   */
  public static NodeKind getKind(final Class<? extends DataRecord> clazz) {
    return INSTANCEFORCLASS.get(clazz);
  }

  @Override
  public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
    return null;
  }

  @Override
  public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
      ResourceConfiguration resourceConfig) {
  }

  private static long getHash(final BytesIn<?> source, final PageReadOnlyTrx pageReadTrx) {
    final long hashCode;
    if (pageReadTrx.getResourceSession().getResourceConfig().hashType == HashType.NONE)
      hashCode = 0L;
    else
      hashCode = source.readLong();
    return hashCode;
  }

  private static void serializeStructNodeJsonValueNode(BytesOut<ByteBuffer> sink, StructNode node) {
    putVarLong(sink, node.getNodeKey() - node.getRightSiblingKey());
    putVarLong(sink, node.getNodeKey() - node.getLeftSiblingKey());
  }

  @NonNull
  private static StructNodeDelegate deserializeStructNodeJsonValueNode(BytesIn<?> source, NodeDelegate nodeDel) {
    var nodeKey = nodeDel.getNodeKey();

    final long rightSibling = nodeKey - getVarLong(source);
    final long leftSibling = nodeKey - getVarLong(source);
    final long firstChild = Fixed.NULL_NODE_KEY.getStandardProperty();
    final long lastChild = Fixed.NULL_NODE_KEY.getStandardProperty();
    final long childCount = 0;
    final long descendantCount = 0;

    return new StructNodeDelegate(nodeDel,
                                  firstChild,
                                  lastChild,
                                  rightSibling,
                                  leftSibling,
                                  childCount,
                                  descendantCount);
  }

  @NonNull
  private static StructNodeDelegate deserializeObjectOrArrayStructDelegate(BytesIn<?> source,
      ResourceConfiguration config, NodeDelegate nodeDel) {
    var nodeKey = nodeDel.getNodeKey();

    final long rightSibling;
    final long leftSibling;
    final long firstChild;
    final long lastChild;
    final long childCount;

    rightSibling = nodeKey - getVarLong(source);
    leftSibling = nodeKey - getVarLong(source);
    firstChild = nodeKey - getVarLong(source);
    lastChild = nodeKey - getVarLong(source);
    if (!config.storeChildCount()) {
      childCount = 0;
    } else {
      childCount = nodeKey - getVarLong(source);
    }

    final long descendantCount;

    if (config.hashType == HashType.NONE) {
      descendantCount = 0;
    } else {
      descendantCount = getVarLong(source) + childCount;
    }

    return new StructNodeDelegate(nodeDel,
                                  firstChild,
                                  lastChild,
                                  rightSibling,
                                  leftSibling,
                                  childCount,
                                  descendantCount);
  }

  private static void serializeJsonObjectOrArrayStructDelegate(BytesOut<ByteBuffer> sink, StructNode node,
      ResourceConfiguration config) {
    final boolean storeChildCount = config.storeChildCount();

    putVarLong(sink, node.getNodeKey() - node.getRightSiblingKey());
    putVarLong(sink, node.getNodeKey() - node.getLeftSiblingKey());

    putVarLong(sink, node.getNodeKey() - node.getFirstChildKey());
    putVarLong(sink, node.getNodeKey() - node.getLastChildKey());
    if (storeChildCount) {
      putVarLong(sink, node.getNodeKey() - node.getChildCount());
    }

    if (config.hashType != HashType.NONE) {
      putVarLong(sink, node.getDescendantCount() - node.getChildCount());
    }
  }

  private static NodeDelegate deserializeNodeDelegateWithoutIDs(final BytesIn<?> source,
      final @NonNegative long recordID, final PageReadOnlyTrx pageReadTrx) {
    final long parentKey = recordID - getVarLong(source);
    final int previousRevision = source.readInt();
    final int lastModifiedRevision = source.readInt();
    final LongHashFunction hashFunction = pageReadTrx.getResourceSession().getResourceConfig().nodeHashFunction;
    return new NodeDelegate(recordID,
                            parentKey,
                            hashFunction,
                            previousRevision,
                            lastModifiedRevision,
                            (SirixDeweyID) null);
  }

  private static NodeDelegate deserializeNodeDelegate(final BytesIn<?> source, final @NonNegative long recordID,
      final byte[] id, final PageReadOnlyTrx pageReadTrx) {
    final long parentKey = recordID - getVarLong(source);
    final int previousRevision = source.readInt();
    final int lastModifiedRevision = source.readInt();
    final LongHashFunction hashFunction = pageReadTrx.getResourceSession().getResourceConfig().nodeHashFunction;
    return new NodeDelegate(recordID, parentKey, hashFunction, previousRevision, lastModifiedRevision, id);
  }

  private static void serializeDelegate(final NodeDelegate nodeDel, final BytesOut<ByteBuffer> sink) {
    putVarLong(sink, nodeDel.getNodeKey() - nodeDel.getParentKey());
    sink.writeInt(nodeDel.getPreviousRevisionNumber());
    sink.writeInt(nodeDel.getLastModifiedRevisionNumber());
  }

  private static void serializeStructDelegate(final NodeKind kind, final StructNodeDelegate nodeDel,
      final BytesOut<ByteBuffer> sink, final ResourceConfiguration config) {
    final var isValueNode =
        kind == NodeKind.NUMBER_VALUE || kind == NodeKind.STRING_VALUE || kind == NodeKind.BOOLEAN_VALUE
            || kind == NodeKind.NULL_VALUE;

    final boolean storeChildCount = config.storeChildCount();

    putVarLong(sink, nodeDel.getNodeKey() - nodeDel.getRightSiblingKey());
    putVarLong(sink, nodeDel.getNodeKey() - nodeDel.getLeftSiblingKey());

    if (!isValueNode) {
      putVarLong(sink, nodeDel.getNodeKey() - nodeDel.getFirstChildKey());
      putVarLong(sink, nodeDel.getNodeKey() - nodeDel.getLastChildKey());
      if (storeChildCount) {
        putVarLong(sink, nodeDel.getNodeKey() - nodeDel.getChildCount());
      }

      if (config.hashType != HashType.NONE) {
        putVarLong(sink, nodeDel.getDescendantCount() - nodeDel.getChildCount());
      }
    }
  }

  private static StructNodeDelegate deserializeStructDel(final NodeKind kind, final NodeDelegate nodeDel,
      final BytesIn<?> source, final ResourceConfiguration config) {
    final long currKey = nodeDel.getNodeKey();
    final boolean storeChildNodes = config.storeChildCount();

    final var isValueNode =
        kind == NodeKind.NUMBER_VALUE || kind == NodeKind.STRING_VALUE || kind == NodeKind.BOOLEAN_VALUE
            || kind == NodeKind.NULL_VALUE;

    final long rightSibling;
    final long leftSibling;
    final long firstChild;
    final long lastChild;
    final long childCount;

    rightSibling = currKey - getVarLong(source);
    leftSibling = currKey - getVarLong(source);

    if (isValueNode) {
      firstChild = Fixed.NULL_NODE_KEY.getStandardProperty();
      lastChild = Fixed.NULL_NODE_KEY.getStandardProperty();
      childCount = 0;
    } else {
      firstChild = currKey - getVarLong(source);
      lastChild = currKey - getVarLong(source);
      if (!storeChildNodes) {
        childCount = 0;
      } else {
        childCount = currKey - getVarLong(source);
      }
    }

    final long descendantCount;

    if (config.hashType == HashType.NONE || isValueNode) {
      descendantCount = 0;
    } else {
      descendantCount = getVarLong(source) + childCount;
    }

    if (isValueNode) {
      return new StructNodeDelegate(nodeDel,
                                    firstChild,
                                    lastChild,
                                    rightSibling,
                                    leftSibling,
                                    childCount,
                                    descendantCount);
    }

    return new StructNodeDelegate(nodeDel, firstChild, rightSibling, leftSibling, childCount, descendantCount);
  }

  private static NameNodeDelegate deserializeNameDelegate(final NodeDelegate nodeDel, final BytesIn<?> source) {
    final int uriKey = source.readInt();
    int prefixKey = source.readInt();
    int localNameKey = source.readInt();
    return new NameNodeDelegate(nodeDel, uriKey, prefixKey, localNameKey, getVarLong(source));
  }

  /**
   * Serializing the {@link NameNodeDelegate} instance.
   *
   * @param nameDel {@link NameNodeDelegate} instance
   * @param sink    to serialize to
   */
  private static void serializeNameDelegate(final NameNodeDelegate nameDel, final BytesOut<ByteBuffer> sink) {
    sink.writeInt(nameDel.getURIKey());
    sink.writeInt(nameDel.getPrefixKey());
    sink.writeInt(nameDel.getLocalNameKey());
    putVarLong(sink, nameDel.getPathNodeKey());
  }

  /**
   * Serializing the {@link ValueNodeDelegate} instance.
   *
   * @param valueDel to be serialized
   * @param sink     to serialize to
   */
  private static void serializeValDelegate(final ValueNodeDelegate valueDel, final BytesOut<ByteBuffer> sink) {
    final boolean isCompressed = valueDel.isCompressed();
    sink.writeByte(isCompressed ? (byte) 1 : (byte) 0);
    final byte[] value = isCompressed ? valueDel.getCompressed() : valueDel.getRawValue();
    sink.writeInt(value.length);
    sink.write(value);
  }

  private static void writeHash(final BytesOut<ByteBuffer> sink, final long hashCode) {
    sink.writeLong(hashCode);
  }

  private static void serializeBigInteger(final BytesOut<ByteBuffer> sink, final BigInteger bigInteger) {
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
}
