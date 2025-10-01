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

import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.module.Namespaces;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
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
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.DeweyIdSerializer;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.json.*;
import io.sirix.node.json.NullNode;
import io.sirix.node.xml.*;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import net.openhft.hashing.LongHashFunction;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.math.BigInteger;

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
  ELEMENT((byte) 1) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, resourceConfiguration);

      // Struct delegate.
      final StructNodeDelegate structDel = deserializeStructDel(this, nodeDel, source, resourceConfiguration);

      final long hashCode = getHash(source, resourceConfiguration);

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
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final ElementNode node = (ElementNode) record;
      serializeDelegate(node.getNodeDelegate(), sink);
      serializeStructDelegate(this, node.getStructNodeDelegate(), sink, resourceConfiguration);
      if (resourceConfiguration.hashType != HashType.NONE) {
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
  ATTRIBUTE((byte) 2) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, resourceConfiguration);

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
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final AttributeNode node = (AttributeNode) record;
      serializeDelegate(node.getNodeDelegate(), sink);
      serializeNameDelegate(node.getNameNodeDelegate(), sink);
      serializeValDelegate(node.getValNodeDelegate(), sink);
    }
  },

  /**
   * Node kind is namespace.
   */
  NAMESPACE((byte) 13) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, resourceConfiguration);

      // Name delegate.
      final NameNodeDelegate nameDel = deserializeNameDelegate(nodeDel, source);

      return new NamespaceNode(nodeDel, nameDel, null);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final NamespaceNode node = (NamespaceNode) record;
      serializeDelegate(node.getNodeDelegate(), sink);
      serializeNameDelegate(node.getNameNodeDelegate(), sink);
    }
  },

  /**
   * Node kind is text.
   */
  TEXT((byte) 3) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, resourceConfiguration);

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
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
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
  PROCESSING_INSTRUCTION((byte) 7) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, resourceConfiguration);

      // Struct delegate.
      final StructNodeDelegate structDel = deserializeStructDel(this, nodeDel, source, resourceConfiguration);

      // Name delegate.
      final NameNodeDelegate nameDel = deserializeNameDelegate(nodeDel, source);

      // Val delegate.
      final boolean isCompressed = source.readByte() == (byte) 1;
      final byte[] vals = new byte[source.readInt()];
      source.read(vals, 0, vals.length);
      final ValueNodeDelegate valDel = new ValueNodeDelegate(nodeDel, vals, isCompressed);

      // Returning an instance.
      return new PINode(structDel, nameDel, valDel);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final PINode node = (PINode) record;
      serializeDelegate(node.getNodeDelegate(), sink);
      serializeStructDelegate(this, node.getStructNodeDelegate(), sink, resourceConfiguration);
      serializeNameDelegate(node.getNameNodeDelegate(), sink);
      serializeValDelegate(node.getValNodeDelegate(), sink);
    }
  },

  /**
   * Node kind is comment.
   */
  COMMENT((byte) 8) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, resourceConfiguration);

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
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
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
  XML_DOCUMENT((byte) 9) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
      final LongHashFunction hashFunction = resourceConfiguration.nodeHashFunction;

      final NodeDelegate nodeDel = new NodeDelegate(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(),
                                                    Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                    hashFunction,
                                                    Constants.NULL_REVISION_NUMBER,
                                                    Constants.NULL_REVISION_NUMBER,
                                                    SirixDeweyID.newRootID().toBytes());
      final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
                                                                  getVarLong(source),
                                                                  Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                  Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                  source.readByte() == ((byte) 0) ? 0 : 1,
                                                                  resourceConfiguration.hashType == HashType.NONE
                                                                      ? 0
                                                                      : source.readLong());
      return new XmlDocumentRootNode(nodeDel, structDel);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final XmlDocumentRootNode node = (XmlDocumentRootNode) record;
      putVarLong(sink, node.getFirstChildKey());
      sink.writeByte(node.hasFirstChild() ? (byte) 1 : (byte) 0);
      if (resourceConfiguration.hashType != HashType.NONE)
        sink.writeLong(node.getDescendantCount());
    }
  },

  /**
   * Whitespace text.
   */
  WHITESPACE((byte) 4) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
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
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
      final LongHashFunction hashFunction = resourceConfiguration.nodeHashFunction;
      final NodeDelegate delegate = new NodeDelegate(recordID, 0, hashFunction, 0, 0, (SirixDeweyID) null);
      return new DeletedNode(delegate);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
    }
  },

  /**
   * NullNode to support the Null Object pattern.
   */
  NULL((byte) 6) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
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
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
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
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
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
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegateWithoutIDs(source, recordID, resourceConfiguration);

      // Struct delegate.
      final StructNodeDelegate structDel = deserializeStructDel(this, nodeDel, source, resourceConfiguration);

      // Name delegate.
      final NameNodeDelegate nameDel = deserializeNameDelegate(nodeDel, source);

      final NodeKind kind = NodeKind.getKind(source.readByte());

      return new PathNode(null, nodeDel, structDel, nameDel, kind, source.readInt(), source.readInt());
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final PathNode node = (PathNode) record;
      serializeDelegate(node.getNodeDelegate(), sink);
      serializeStructDelegate(this, node.getStructNodeDelegate(), sink, resourceConfiguration);
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
  CASRB((byte) 17) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
      final int valueSize = source.readInt();
      final byte[] value = new byte[valueSize];
      source.read(value, 0, valueSize);
      final int typeSize = source.readInt();
      final byte[] type = new byte[typeSize];
      source.read(type, 0, typeSize);

      final Type atomicType = resolveType(new String(type, Constants.DEFAULT_ENCODING));

      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegateWithoutIDs(source, recordID, resourceConfiguration);
      final long leftChild = getVarLong(source);
      final long rightChild = getVarLong(source);
      final long pathNodeKey = getVarLong(source);
      final boolean isChanged = source.readBoolean();
      final long valueNodeKey = source.readLong();
      final Atomic atomic = AtomicUtil.fromBytes(value, atomicType);
      final var node = new RBNodeKey<>(new CASValue(atomic, atomicType, pathNodeKey), valueNodeKey, nodeDel);

      node.setLeftChildKey(leftChild);
      node.setRightChildKey(rightChild);
      node.setChanged(isChanged);
      return node;
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
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
  PATHRB((byte) 18) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
      final long key = getVarLong(source);
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegateWithoutIDs(source, recordID, resourceConfiguration);
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
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
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
  NAMERB((byte) 19) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
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
      final NodeDelegate nodeDel = deserializeNodeDelegateWithoutIDs(source, recordID, resourceConfiguration);
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
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
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
  RB_NODE_VALUE((byte) 55) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
      final var nodeKeys = deserializeNodeReferences(source);
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegateWithoutIDs(source, recordID, resourceConfiguration);
      return new RBNodeValue<>(new NodeReferences(nodeKeys), nodeDel);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
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
  DEWEYIDMAPPING((byte) 23) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
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
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
      var segment = (MemorySegment) source.getUnderlying();
      var nodeDelegate = new NodeDelegate(segment.asSlice(0, NodeDelegate.NODE_DELEGATE_LAYOUT.byteSize()),
                                          recordID,
                                          resourceConfiguration,
                                          deweyID);
      var structNodeDelegateOffset = NodeDelegate.NODE_DELEGATE_LAYOUT.byteSize();
      var structNodeDelegateUpperRange = StructNodeDelegate.STRUCT_NODE_LAYOUT.byteSize();

      var structNodeDelegate = new StructNodeDelegate(nodeDelegate,
                                                      segment.asSlice(structNodeDelegateOffset,
                                                                      structNodeDelegateUpperRange),
                                                      recordID,
                                                      resourceConfiguration);

      var objectNodeOffset = structNodeDelegateOffset + structNodeDelegateUpperRange;
      return new ObjectNode(structNodeDelegate,
                            segment.asSlice(objectNodeOffset),
                            resourceConfiguration.hashType != HashType.NONE);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final ObjectNode node = (ObjectNode) record;
      var config = resourceConfiguration;
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
  ARRAY((byte) 25) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
      var segment = (MemorySegment) source.getUnderlying();
      var nodeDelegate = new NodeDelegate(segment.asSlice(0, NodeDelegate.NODE_DELEGATE_LAYOUT.byteSize()),
                                          recordID,
                                          resourceConfiguration,
                                          deweyID);
      var structNodeDelegateOffset = NodeDelegate.NODE_DELEGATE_LAYOUT.byteSize();
      var structNodeDelegateUpperRange = StructNodeDelegate.STRUCT_NODE_LAYOUT.byteSize();

      var structNodeDelegate = new StructNodeDelegate(nodeDelegate,
                                                      segment.asSlice(structNodeDelegateOffset,
                                                                      structNodeDelegateUpperRange),
                                                      recordID,
                                                      resourceConfiguration);

      var arrayNodeOffset = structNodeDelegateOffset + structNodeDelegateUpperRange;
      return new ArrayNode(structNodeDelegate,
                           segment.asSlice(arrayNodeOffset),
                           resourceConfiguration.hashType != HashType.NONE);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final ArrayNode node = (ArrayNode) record;
      serializeDelegate(node.getNodeDelegate(), sink);
      serializeJsonObjectOrArrayStructDelegate(sink, node, resourceConfiguration);
      sink.writeLong(node.getPathNodeKey());
      if (resourceConfiguration.hashType != HashType.NONE)
        writeHash(sink, node.getHash());
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
  OBJECT_KEY((byte) 26) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
      var segment = (MemorySegment) source.getUnderlying();
      
      // Deserialize NodeDelegate
      var nodeDelegate = new NodeDelegate(segment.asSlice(0, NodeDelegate.NODE_DELEGATE_LAYOUT.byteSize()),
                                          recordID,
                                          resourceConfiguration,
                                          deweyID);
      
      var structNodeDelegateOffset = NodeDelegate.NODE_DELEGATE_LAYOUT.byteSize();
      var structNodeDelegateUpperRange = StructNodeDelegate.STRUCT_NODE_LAYOUT.byteSize();
      
      // Deserialize StructNodeDelegate
      var structNodeDelegate = new StructNodeDelegate(nodeDelegate,
                                                      segment.asSlice(structNodeDelegateOffset,
                                                                      structNodeDelegateUpperRange),
                                                      recordID,
                                                      resourceConfiguration);
      
      var objectKeyNodeOffset = structNodeDelegateOffset + structNodeDelegateUpperRange;
      
      // Return ObjectKeyNode with segment slice starting at object key data offset
      return new ObjectKeyNode(segment.asSlice(objectKeyNodeOffset),
                               structNodeDelegate,
                               recordID,
                               resourceConfiguration);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final ObjectKeyNode node = (ObjectKeyNode) record;
      var config = resourceConfiguration;
      serializeDelegate(node.getNodeDelegate(), sink);
      if (config.hashType != HashType.NONE) {
        writeHash(sink, node.getHash());
      }
      sink.writeInt(node.getNameKey());
      sink.writeLong(node.getPathNodeKey());
      sink.writeLong(node.getRightSiblingKey());
      sink.writeLong(node.getLeftSiblingKey());
      sink.writeLong(node.getFirstChildKey());
      if (config.hashType != HashType.NONE) {
        sink.writeLong(node.getDescendantCount());
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
  OBJECT_STRING_VALUE((byte) 40) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
      var segment = (MemorySegment) source.getUnderlying();
      
      // Deserialize NodeDelegate
      var nodeDelegate = new NodeDelegate(segment.asSlice(0, NodeDelegate.NODE_DELEGATE_LAYOUT.byteSize()),
                                          recordID,
                                          resourceConfiguration,
                                          deweyID);
      
      var valueNodeDelegateOffset = NodeDelegate.NODE_DELEGATE_LAYOUT.byteSize();
      
      // Create ValueNodeDelegate with MemorySegment for lazy reading
      var valueNodeDelegate = new ValueNodeDelegate(nodeDelegate, segment.asSlice(valueNodeDelegateOffset));
      
      // Struct delegate (value nodes don't have children)
      var structDelegate = new StructNodeDelegate(nodeDelegate,
                                                  Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                  Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                  Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                  Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                  0,
                                                  0);
      
      // Return ObjectStringNode - value will be read lazily from MemorySegment
      return new ObjectStringNode(valueNodeDelegate, structDelegate);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
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
  OBJECT_BOOLEAN_VALUE((byte) 41) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
      var segment = (MemorySegment) source.getUnderlying();
      
      // Deserialize NodeDelegate
      var nodeDelegate = new NodeDelegate(segment.asSlice(0, NodeDelegate.NODE_DELEGATE_LAYOUT.byteSize()),
                                          recordID,
                                          resourceConfiguration,
                                          deweyID);
      
      var booleanNodeOffset = NodeDelegate.NODE_DELEGATE_LAYOUT.byteSize();
      
      // Struct delegate (value nodes don't have children)
      var structDelegate = new StructNodeDelegate(nodeDelegate,
                                                  Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                  Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                  Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                  Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                  0,
                                                  0);
      
      // Return ObjectBooleanNode with segment slice starting at boolean data offset
      // The boolean value will be read lazily from this segment
      return new ObjectBooleanNode(segment.asSlice(booleanNodeOffset), structDelegate);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final ObjectBooleanNode node = (ObjectBooleanNode) record;
      serializeDelegate(node.getNodeDelegate(), sink);
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
  OBJECT_NUMBER_VALUE((byte) 42) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {

      var segment = (MemorySegment) source.getUnderlying();
      
      // Deserialize NodeDelegate
      var nodeDelegate = new NodeDelegate(segment.asSlice(0, NodeDelegate.NODE_DELEGATE_LAYOUT.byteSize()),
                                          recordID,
                                          resourceConfiguration,
                                          deweyID);
      
      var structNodeDelegateOffset = NodeDelegate.NODE_DELEGATE_LAYOUT.byteSize();
      // For value nodes, struct delegate only contains rightSibling and leftSibling (2 longs = 16 bytes)
      var structNodeDelegateSize = 16L;
      
      // Deserialize StructNodeDelegate
      var structNodeDelegate = new StructNodeDelegate(nodeDelegate,
                                                      segment.asSlice(structNodeDelegateOffset,
                                                                      structNodeDelegateSize),
                                                      recordID,
                                                      resourceConfiguration);
      
      var numberNodeOffset = structNodeDelegateOffset + structNodeDelegateSize;
      
      // Return ObjectNumberNode with segment slice starting at number data offset
      // The number will be read lazily from this segment
      return new ObjectNumberNode(segment.asSlice(numberNodeOffset), structNodeDelegate);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final ObjectNumberNode node = (ObjectNumberNode) record;
      
      serializeDelegate(node.getNodeDelegate(), sink);
      serializeStructNodeJsonValueNode(sink, node);

      final Number number = node.getValue();

      switch (number) {
        case Double _ -> {
          sink.writeByte((byte) 0);
          sink.writeDouble(number.doubleValue());
        }
        case Float _ -> {
          sink.writeByte((byte) 1);
          sink.writeFloat(number.floatValue());
        }
        case Integer _ -> {
          sink.writeByte((byte) 2);
          sink.writeInt(number.intValue());
        }
        case Long _ -> {
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
  OBJECT_NULL_VALUE((byte) 43) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
      var segment = (MemorySegment) source.getUnderlying();
      
      // Deserialize NodeDelegate
      var nodeDelegate = new NodeDelegate(segment.asSlice(0, NodeDelegate.NODE_DELEGATE_LAYOUT.byteSize()),
                                          recordID,
                                          resourceConfiguration,
                                          deweyID);
      
      // Struct delegate (value nodes don't have children)
      var structDelegate = new StructNodeDelegate(nodeDelegate,
                                                  Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                  Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                  Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                  Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                  0,
                                                  0);
      
      // Return ObjectNullNode
      return new ObjectNullNode(structDelegate);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
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
  STRING_VALUE((byte) 30) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
      var segment = (MemorySegment) source.getUnderlying();
      
      // Deserialize NodeDelegate
      var nodeDelegate = new NodeDelegate(segment.asSlice(0, NodeDelegate.NODE_DELEGATE_LAYOUT.byteSize()),
                                          recordID,
                                          resourceConfiguration,
                                          deweyID);
      
      var valueNodeDelegateOffset = NodeDelegate.NODE_DELEGATE_LAYOUT.byteSize();
      
      // Create ValueNodeDelegate with MemorySegment for lazy reading
      var valueNodeDelegate = new ValueNodeDelegate(nodeDelegate, segment.asSlice(valueNodeDelegateOffset));
      
      // Calculate offset after NodeDelegate + ValueDelegate to find StructDelegate
      // ValueDelegate serializes: 1 byte (compressed flag) + 4 bytes (length) + value bytes
      var structNodeDelegateOffset = valueNodeDelegateOffset + 1; // compressed flag
      int valueLength = MemorySegmentUtils.readInt(segment, structNodeDelegateOffset);
      structNodeDelegateOffset += 4 + valueLength; // skip length field + actual value
      
      // For value nodes, struct delegate only contains rightSibling and leftSibling (2 longs = 16 bytes)
      var structNodeDelegateSize = 16L;
      
      // Deserialize StructNodeDelegate
      var structDelegate = new StructNodeDelegate(nodeDelegate,
                                                  segment.asSlice(structNodeDelegateOffset,
                                                                  structNodeDelegateSize),
                                                  recordID,
                                                  resourceConfiguration);
      
      // Return StringNode - value will be read lazily from MemorySegment
      return new StringNode(valueNodeDelegate, structDelegate);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
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
  BOOLEAN_VALUE((byte) 27) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
      var segment = (MemorySegment) source.getUnderlying();
      
      // Deserialize NodeDelegate
      var nodeDelegate = new NodeDelegate(segment.asSlice(0, NodeDelegate.NODE_DELEGATE_LAYOUT.byteSize()),
                                          recordID,
                                          resourceConfiguration,
                                          deweyID);
      
      var structNodeDelegateOffset = NodeDelegate.NODE_DELEGATE_LAYOUT.byteSize();
      // For value nodes, struct delegate only contains rightSibling and leftSibling (2 longs = 16 bytes)
      var structNodeDelegateSize = 16L;
      
      // Deserialize StructNodeDelegate
      var structNodeDelegate = new StructNodeDelegate(nodeDelegate,
                                                      segment.asSlice(structNodeDelegateOffset,
                                                                      structNodeDelegateSize),
                                                      recordID,
                                                      resourceConfiguration);
      
      var booleanNodeOffset = structNodeDelegateOffset + structNodeDelegateSize;
      
      // Return BooleanNode with segment slice starting at boolean data offset
      // The boolean value will be read lazily from this segment
      return new BooleanNode(segment.asSlice(booleanNodeOffset), structNodeDelegate);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final BooleanNode node = (BooleanNode) record;
      serializeDelegate(node.getNodeDelegate(), sink);
      serializeStructNodeJsonValueNode(sink, node);
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
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
      var segment = (MemorySegment) source.getUnderlying();
      
      // Deserialize NodeDelegate
      var nodeDelegate = new NodeDelegate(segment.asSlice(0, NodeDelegate.NODE_DELEGATE_LAYOUT.byteSize()),
                                          recordID,
                                          resourceConfiguration,
                                          deweyID);
      
      var structNodeDelegateOffset = NodeDelegate.NODE_DELEGATE_LAYOUT.byteSize();
      // For value nodes, struct delegate only contains rightSibling and leftSibling (2 longs = 16 bytes)
      var structNodeDelegateSize = 16L;
      
      // Deserialize StructNodeDelegate
      var structNodeDelegate = new StructNodeDelegate(nodeDelegate,
                                                      segment.asSlice(structNodeDelegateOffset,
                                                                      structNodeDelegateSize),
                                                      recordID,
                                                      resourceConfiguration);
      
      var numberNodeOffset = structNodeDelegateOffset + structNodeDelegateSize;
      
      // Return NumberNode with segment slice starting at number data offset
      // The number will be read lazily from this segment
      return new NumberNode(segment.asSlice(numberNodeOffset), structNodeDelegate);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final NumberNode node = (NumberNode) record;

      serializeDelegate(node.getNodeDelegate(), sink);
      serializeStructNodeJsonValueNode(sink, node);

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
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
      var segment = (MemorySegment) source.getUnderlying();
      
      // Deserialize NodeDelegate
      var nodeDelegate = new NodeDelegate(segment.asSlice(0, NodeDelegate.NODE_DELEGATE_LAYOUT.byteSize()),
                                          recordID,
                                          resourceConfiguration,
                                          deweyID);
      
      var structNodeDelegateOffset = NodeDelegate.NODE_DELEGATE_LAYOUT.byteSize();
      // For value nodes, struct delegate only contains rightSibling and leftSibling (2 longs = 16 bytes)
      var structNodeDelegateSize = 16L;
      
      // Deserialize StructNodeDelegate
      var structDelegate = new StructNodeDelegate(nodeDelegate,
                                                  segment.asSlice(structNodeDelegateOffset,
                                                                  structNodeDelegateSize),
                                                  recordID,
                                                  resourceConfiguration);
      
      // Return NullNode
      return new NullNode(structDelegate);
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
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
  JSON_DOCUMENT((byte) 31) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
      final LongHashFunction hashFunction = resourceConfiguration.nodeHashFunction;

      final NodeDelegate nodeDel = new NodeDelegate(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(),
                                                    Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                    hashFunction,
                                                    Constants.NULL_REVISION_NUMBER,
                                                    Constants.NULL_REVISION_NUMBER,
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

    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
      final JsonDocumentRootNode node = (JsonDocumentRootNode) record;
      putVarLong(sink, node.getFirstChildKey());
      sink.writeLong(node.getDescendantCount());
    }
  },

  HASH_ENTRY((byte) 32) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
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
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
      return new HashCountEntryNode(recordID, source.readInt());
    }

    public @NonNull DataRecord deserialize(final MemorySegment segment, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
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
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
      return new DeweyIDNode(recordID, new SirixDeweyID(deweyID));
    }

    public @NonNull DataRecord deserialize(final MemorySegment segment, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
      // DeweyIDNode uses simple constructor with recordID and deweyID
      return new DeweyIDNode(recordID, new SirixDeweyID(deweyID));
    }

    @Override
    public void serialize(final BytesOut<?> sink, final DataRecord record,
        final ResourceConfiguration resourceConfiguration) {
    }
  },

  REVISION_REFERENCES_NODE((byte) 35) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
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
      sink.writeByte((byte) compressedRevisions.length);
      for (int compressedRevision : compressedRevisions) {
        sink.writeInt(compressedRevision);
      }
    }
  },

  /**
   * Node type not known.
   */
  UNKNOWN((byte) 22) {
    @Override
    public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
        final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
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

  private static void serializeNodeReferences(BytesOut<?> sink, Roaring64Bitmap nodeKeys) {
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
      throw new RuntimeException(e);
    }
    return nodeKeys;
  }

  /**
   * Identifier.
   */
  private final byte id;

  /**
   * Mapping of keys -> nodes.
   */
  private static final NodeKind[] INSTANCEFORID = new NodeKind[128];

  static {
    for (final NodeKind node : values()) {
      INSTANCEFORID[node.id] = node;
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
   */
  public static NodeKind getKind(final byte id) {
    return INSTANCEFORID[id];
  }

  @Override
  public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
    return null;
  }

  @Override
  public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
      ResourceConfiguration resourceConfig) {
  }

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

  @NonNull
  private static StructNodeDelegate deserializeStructNodeJsonValueNode(BytesIn<?> source, NodeDelegate nodeDel) {
    // Read fixed-size longs directly instead of variable-length values
    final long rightSibling = source.readLong();
    final long leftSibling = source.readLong();
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
    return new StructNodeDelegate(nodeDel, (MemorySegment) source.getUnderlying(), nodeDel.getNodeKey(), config);
  }

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

  private static NodeDelegate deserializeNodeDelegateWithoutIDs(final BytesIn<?> source,
      final @NonNegative long recordID, final ResourceConfiguration resourceConfiguration) {
    // Read fixed-size long instead of variable-length encoded value
    final long parentKey = recordID - source.readLong();
    final int previousRevision = source.readInt();
    final int lastModifiedRevision = source.readInt();
    final LongHashFunction hashFunction = resourceConfiguration.nodeHashFunction;
    return new NodeDelegate(recordID,
                            parentKey,
                            hashFunction,
                            previousRevision,
                            lastModifiedRevision,
                            (SirixDeweyID) null);
  }

  private static NodeDelegate deserializeNodeDelegate(final BytesIn<?> source, final @NonNegative long recordID,
      final byte[] id, final ResourceConfiguration resourceConfiguration) {
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

  private static void serializeStructDelegate(final NodeKind kind, final StructNodeDelegate nodeDel,
      final BytesOut<?> sink, final ResourceConfiguration config) {
    final var isValueNode =
        kind == NodeKind.NUMBER_VALUE || kind == NodeKind.STRING_VALUE || kind == NodeKind.BOOLEAN_VALUE
            || kind == NodeKind.NULL_VALUE;

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

    final var isValueNode =
        kind == NodeKind.NUMBER_VALUE || kind == NodeKind.STRING_VALUE || kind == NodeKind.BOOLEAN_VALUE
            || kind == NodeKind.NULL_VALUE;

    final long rightSibling;
    final long leftSibling;
    final long firstChild;
    final long lastChild;
    final long childCount;

    // Read fixed-size longs directly instead of variable-length values
    rightSibling = source.readLong();
    leftSibling = source.readLong();

    if (isValueNode) {
      firstChild = Fixed.NULL_NODE_KEY.getStandardProperty();
      lastChild = Fixed.NULL_NODE_KEY.getStandardProperty();
      childCount = 0;
    } else {
      firstChild = source.readLong();
      lastChild = source.readLong();
      if (!storeChildNodes) {
        childCount = 0;
      } else {
        childCount = source.readLong();
      }
    }

    final long descendantCount;

    if (config.hashType == HashType.NONE || isValueNode) {
      descendantCount = 0;
    } else {
      descendantCount = source.readLong();
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
  private static void serializeNameDelegate(final NameNodeDelegate nameDel, final BytesOut<?> sink) {
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
  private static void serializeValDelegate(final ValueNodeDelegate valueDel, final BytesOut<?> sink) {
    final boolean isCompressed = valueDel.isCompressed();
    sink.writeByte(isCompressed ? (byte) 1 : (byte) 0);
    final byte[] value = isCompressed ? valueDel.getCompressed() : valueDel.getRawValue();
    sink.writeInt(value.length);
    sink.write(value);
  }

  private static void writeHash(final BytesOut<?> sink, final long hashCode) {
    sink.writeLong(hashCode);
  }

  private static void serializeBigInteger(final BytesOut<?> sink, final BigInteger bigInteger) {
    final byte[] bytes = bigInteger.toByteArray();
    sink.writeStopBit(bytes.length);
    sink.write(bytes);
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
