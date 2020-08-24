/*
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.sirix.node;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.hash.HashFunction;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.module.Namespaces;
import org.brackit.xquery.xdm.Type;
import org.sirix.access.ResourceConfiguration;
import org.sirix.access.trx.node.HashType;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.index.AtomicUtil;
import org.sirix.index.redblacktree.RBNode;
import org.sirix.index.redblacktree.keyvalue.CASValue;
import org.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.sirix.index.path.summary.PathNode;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.delegates.ValueNodeDelegate;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.node.interfaces.NodePersistenter;
import org.sirix.node.json.NullNode;
import org.sirix.node.json.*;
import org.sirix.node.xml.*;
import org.sirix.page.UnorderedKeyValuePage;
import org.sirix.service.xml.xpath.AtomicValue;
import org.sirix.settings.Constants;
import org.sirix.settings.Fixed;

import javax.annotation.Nonnegative;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

import static org.sirix.node.Utils.getVarLong;
import static org.sirix.node.Utils.putVarLong;

/**
 * Enumeration for different nodes. All nodes are determined by a unique id.
 *
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 */
public enum NodeKind implements NodePersistenter {

  /**
   * Node kind is element.
   */
  ELEMENT((byte) 1, ElementNode.class) {
    @Override
    public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
        final PageReadOnlyTrx pageReadTrx) throws IOException {
      final BigInteger hashCode = getHash(source, pageReadTrx);

      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, pageReadTrx);

      // Struct delegate.
      final StructNodeDelegate structDel =
          deserializeStructDel(this, nodeDel, source, pageReadTrx.getResourceManager().getResourceConfig());

      // Name delegate.
      final NameNodeDelegate nameDel = deserializeNameDelegate(nodeDel, source);

      // Attributes.
      final int attrCount = source.readInt();
      final List<Long> attrKeys = new ArrayList<>(attrCount);
      final BiMap<Long, Long> attrs = HashBiMap.create();
      for (int i = 0; i < attrCount; i++) {
        final long nodeKey = source.readLong();
        attrKeys.add(nodeKey);
        attrs.put(source.readLong(), nodeKey);
      }

      // Namespaces.
      final int nsCount = source.readInt();
      final List<Long> namespKeys = new ArrayList<>(nsCount);
      for (int i = 0; i < nsCount; i++) {
        namespKeys.add(source.readLong());
      }

      final String uri = pageReadTrx.getName(nameDel.getURIKey(), NodeKind.NAMESPACE);
      final int prefixKey = nameDel.getPrefixKey();
      final String prefix = prefixKey == -1 ? "" : pageReadTrx.getName(prefixKey, NodeKind.ELEMENT);
      final int localNameKey = nameDel.getLocalNameKey();
      final String localName = localNameKey == -1 ? "" : pageReadTrx.getName(localNameKey, NodeKind.ELEMENT);

      return new ElementNode(hashCode,
                             structDel,
                             nameDel,
                             attrKeys,
                             attrs,
                             namespKeys,
                             new QNm(uri, prefix, localName));
    }

    @Override
    public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx)
        throws IOException {
      final ElementNode node = (ElementNode) record;
      if (pageReadTrx.getResourceManager().getResourceConfig().hashType != HashType.NONE)
        writeHash(sink, node.getHash() == null ? BigInteger.ZERO : node.getHash());
      serializeDelegate(node.getNodeDelegate(), sink);
      serializeStructDelegate(this,
                              node.getStructNodeDelegate(),
                              sink,
                              pageReadTrx.getResourceManager().getResourceConfig());
      serializeNameDelegate(node.getNameNodeDelegate(), sink);
      sink.writeInt(node.getAttributeCount());
      for (int i = 0, attCount = node.getAttributeCount(); i < attCount; i++) {
        final long key = node.getAttributeKey(i);
        sink.writeLong(key);
        sink.writeLong(node.getAttributeNameKey(key).get());
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
    public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
        final PageReadOnlyTrx pageReadTrx) throws IOException {
      final BigInteger hashCode = getHash(source, pageReadTrx);

      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, pageReadTrx);

      // Name delegate.
      final NameNodeDelegate nameDel = deserializeNameDelegate(nodeDel, source);

      // Val delegate.
      final boolean isCompressed = source.readByte() == (byte) 1;
      final byte[] vals = new byte[source.readInt()];
      source.readFully(vals, 0, vals.length);
      final ValueNodeDelegate valDel = new ValueNodeDelegate(nodeDel, vals, isCompressed);

      final String uri = pageReadTrx.getName(nameDel.getURIKey(), NodeKind.NAMESPACE);
      final int prefixKey = nameDel.getPrefixKey();
      final String prefix = prefixKey == -1 ? "" : pageReadTrx.getName(prefixKey, NodeKind.ATTRIBUTE);
      final int localNameKey = nameDel.getLocalNameKey();
      final String localName = localNameKey == -1 ? "" : pageReadTrx.getName(localNameKey, NodeKind.ATTRIBUTE);

      final QNm name = new QNm(uri, prefix, localName);

      // Returning an instance.
      return new AttributeNode(hashCode, nodeDel, nameDel, valDel, name);
    }

    @Override
    public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx)
        throws IOException {
      final AttributeNode node = (AttributeNode) record;
      if (pageReadTrx.getResourceManager().getResourceConfig().hashType != HashType.NONE)
        writeHash(sink, node.getHash() == null ? BigInteger.ZERO : node.getHash());
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
    public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
        final PageReadOnlyTrx pageReadTrx) throws IOException {
      final BigInteger hashCode = getHash(source, pageReadTrx);

      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, pageReadTrx);

      // Name delegate.
      final NameNodeDelegate nameDel = deserializeNameDelegate(nodeDel, source);

      final String uri = pageReadTrx.getName(nameDel.getURIKey(), NodeKind.NAMESPACE);
      final int prefixKey = nameDel.getPrefixKey();
      final String prefix = prefixKey == -1 ? "" : pageReadTrx.getName(prefixKey, NodeKind.ELEMENT);
      final int localNameKey = nameDel.getLocalNameKey();
      final String localName = localNameKey == -1 ? "" : pageReadTrx.getName(localNameKey, NodeKind.ELEMENT);

      final QNm name = new QNm(uri, prefix, localName);

      return new NamespaceNode(hashCode, nodeDel, nameDel, name);
    }

    @Override
    public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx)
        throws IOException {
      final NamespaceNode node = (NamespaceNode) record;
      if (pageReadTrx.getResourceManager().getResourceConfig().hashType != HashType.NONE)
        writeHash(sink, node.getHash() == null ? BigInteger.ZERO : node.getHash());
      serializeDelegate(node.getNodeDelegate(), sink);
      serializeNameDelegate(node.getNameNodeDelegate(), sink);
    }
  },

  /**
   * Node kind is text.
   */
  TEXT((byte) 3, TextNode.class) {
    @Override
    public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
        final PageReadOnlyTrx pageReadTrx) throws IOException {
      final BigInteger hashCode = getHash(source, pageReadTrx);

      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, pageReadTrx);

      // Val delegate.
      final boolean isCompressed = source.readByte() == (byte) 1;
      final byte[] vals = new byte[source.readInt()];
      source.readFully(vals, 0, vals.length);
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
      return new TextNode(hashCode, valDel, structDel);
    }

    @Override
    public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx)
        throws IOException {
      final TextNode node = (TextNode) record;
      if (pageReadTrx.getResourceManager().getResourceConfig().hashType != HashType.NONE)
        writeHash(sink, node.getHash() == null ? BigInteger.ZERO : node.getHash());
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
    public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
        final PageReadOnlyTrx pageReadTrx) throws IOException {
      final BigInteger hashCode = getHash(source, pageReadTrx);

      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, pageReadTrx);

      // Struct delegate.
      final StructNodeDelegate structDel =
          deserializeStructDel(this, nodeDel, source, pageReadTrx.getResourceManager().getResourceConfig());

      // Name delegate.
      final NameNodeDelegate nameDel = deserializeNameDelegate(nodeDel, source);

      // Val delegate.
      final boolean isCompressed = source.readByte() == (byte) 1;
      final byte[] vals = new byte[source.readInt()];
      source.readFully(vals, 0, vals.length);
      final ValueNodeDelegate valDel = new ValueNodeDelegate(nodeDel, vals, isCompressed);

      // Returning an instance.
      return new PINode(hashCode, structDel, nameDel, valDel, pageReadTrx);
    }

    @Override
    public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx)
        throws IOException {
      final PINode node = (PINode) record;
      if (pageReadTrx.getResourceManager().getResourceConfig().hashType != HashType.NONE)
        writeHash(sink, node.getHash() == null ? BigInteger.ZERO : node.getHash());
      serializeDelegate(node.getNodeDelegate(), sink);
      serializeStructDelegate(this,
                              node.getStructNodeDelegate(),
                              sink,
                              pageReadTrx.getResourceManager().getResourceConfig());
      serializeNameDelegate(node.getNameNodeDelegate(), sink);
      serializeValDelegate(node.getValNodeDelegate(), sink);
    }
  },

  /**
   * Node kind is comment.
   */
  COMMENT((byte) 8, CommentNode.class) {
    @Override
    public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
        final PageReadOnlyTrx pageReadTrx) throws IOException {
      final BigInteger hashCode = getHash(source, pageReadTrx);

      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, pageReadTrx);

      // Val delegate.
      final boolean isCompressed = source.readByte() == (byte) 1;
      final byte[] vals = new byte[source.readInt()];
      source.readFully(vals, 0, vals.length);
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
      return new CommentNode(hashCode, valDel, structDel);
    }

    @Override
    public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx)
        throws IOException {
      final CommentNode node = (CommentNode) record;
      if (pageReadTrx.getResourceManager().getResourceConfig().hashType != HashType.NONE)
        writeHash(sink, node.getHash() == null ? BigInteger.ZERO : node.getHash());
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
    public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
        final PageReadOnlyTrx pageReadTrx) throws IOException {
      final HashFunction hashFunction = pageReadTrx.getResourceManager().getResourceConfig().nodeHashFunction;

      final NodeDelegate nodeDel = new NodeDelegate(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(),
                                                    Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                    hashFunction,
                                                    null,
                                                    getVarLong(source),
                                                    SirixDeweyID.newRootID());
      final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
                                                                  getVarLong(source),
                                                                  Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                  Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                  source.readByte() == ((byte) 0) ? 0 : 1,
                                                                  source.readLong());
      return new XmlDocumentRootNode(nodeDel, structDel);
    }

    @Override
    public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx)
        throws IOException {
      final XmlDocumentRootNode node = (XmlDocumentRootNode) record;
      // writeHash(sink, node.getHash());
      putVarLong(sink, node.getRevision());
      putVarLong(sink, node.getFirstChildKey());
      sink.writeByte(node.hasFirstChild() ? (byte) 1 : (byte) 0);
      sink.writeLong(node.getDescendantCount());
    }

    @Override
    public SirixDeweyID deserializeDeweyID(DataInput source, SirixDeweyID previousDeweyID,
        ResourceConfiguration resourceConfig) {
      return null;
    }

    @Override
    public void serializeDeweyID(DataOutput sink, SirixDeweyID deweyID, SirixDeweyID prevDeweyID,
        ResourceConfiguration resourceConfig) {
    }
  },

  /**
   * Whitespace text.
   */
  WHITESPACE((byte) 4, null) {
    @Override
    public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
        final PageReadOnlyTrx pageReadTrx) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      throw new UnsupportedOperationException();
    }

    @Override
    public SirixDeweyID deserializeDeweyID(DataInput source, SirixDeweyID previousDeweyID,
        ResourceConfiguration resourceConfig) {
      return null;
    }

    @Override
    public void serializeDeweyID(DataOutput sink, SirixDeweyID deweyID, SirixDeweyID prevDeweyID,
        ResourceConfiguration resourceConfig) {
    }
  },

  /**
   * Node kind is deleted node.
   */
  DELETE((byte) 5, DeletedNode.class) {
    @Override
    public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
        final PageReadOnlyTrx pageReadTrx) {
      final HashFunction hashFunction = pageReadTrx.getResourceManager().getResourceConfig().nodeHashFunction;
      final NodeDelegate delegate = new NodeDelegate(recordID, 0, hashFunction, null, 0, null);
      return new DeletedNode(delegate);
    }

    @Override
    public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
    }

    @Override
    public SirixDeweyID deserializeDeweyID(DataInput source, SirixDeweyID previousDeweyID,
        ResourceConfiguration resourceConfig) {
      return null;
    }

    @Override
    public void serializeDeweyID(DataOutput sink, SirixDeweyID deweyID, SirixDeweyID prevDeweyID,
        ResourceConfiguration resourceConfig) {
    }
  },

  /**
   * NullNode to support the Null Object pattern.
   */
  NULL((byte) 6, NullNode.class) {
    @Override
    public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
        final PageReadOnlyTrx pageReadTrx) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serialize(final DataOutput ink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      throw new UnsupportedOperationException();
    }

    @Override
    public SirixDeweyID deserializeDeweyID(DataInput source, SirixDeweyID previousDeweyID,
        ResourceConfiguration resourceConfig) {
      return null;
    }

    @Override
    public void serializeDeweyID(DataOutput sink, SirixDeweyID deweyID, SirixDeweyID prevDeweyID,
        ResourceConfiguration resourceConfig) {
    }
  },

  /**
   * Dumb node for testing.
   */
  DUMB((byte) 20, DumbNode.class) {
    @Override
    public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
        final PageReadOnlyTrx pageReadTrx) {
      return new DumbNode(recordID);
    }

    @Override
    public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      throw new UnsupportedOperationException();
    }

    @Override
    public SirixDeweyID deserializeDeweyID(DataInput source, SirixDeweyID previousDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(DataOutput sink, SirixDeweyID deweyID, SirixDeweyID prevDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },

  /**
   * AtomicKind.
   */
  ATOMIC((byte) 15, AtomicValue.class) {
    @Override
    public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
        final PageReadOnlyTrx pageReadTrx) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      throw new UnsupportedOperationException();
    }

    @Override
    public SirixDeweyID deserializeDeweyID(DataInput source, SirixDeweyID previousDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(DataOutput sink, SirixDeweyID deweyID, SirixDeweyID prevDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },

  /**
   * Node kind is path node.
   */
  PATH((byte) 16, PathNode.class) {
    @Override
    public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
        final PageReadOnlyTrx pageReadTrx) throws IOException {
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegateWithoutIDs(source, recordID, pageReadTrx);

      // Struct delegate.
      final StructNodeDelegate structDel =
          deserializeStructDel(this, nodeDel, source, pageReadTrx.getResourceManager().getResourceConfig());

      // Name delegate.
      final NameNodeDelegate nameDel = deserializeNameDelegate(nodeDel, source);

      final NodeKind kind = NodeKind.getKind(source.readByte());
      final String uri =
          kind == NodeKind.OBJECT_STRING_VALUE ? "" : pageReadTrx.getName(nameDel.getURIKey(), NodeKind.NAMESPACE);
      final int prefixKey = nameDel.getPrefixKey();
      final String prefix = prefixKey == -1 ? "" : pageReadTrx.getName(prefixKey, kind);
      final int localNameKey = nameDel.getLocalNameKey();
      final String localName = localNameKey == -1 ? "" : pageReadTrx.getName(localNameKey, kind);

      return new PathNode(new QNm(uri, prefix, localName),
                          nodeDel,
                          structDel,
                          nameDel,
                          kind,
                          source.readInt(),
                          source.readInt());
    }

    @Override
    public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx)
        throws IOException {
      final PathNode node = (PathNode) record;
      serializeDelegate(node.getNodeDelegate(), sink);
      serializeStructDelegate(this,
                              node.getStructNodeDelegate(),
                              sink,
                              pageReadTrx.getResourceManager().getResourceConfig());
      serializeNameDelegate(node.getNameNodeDelegate(), sink);
      sink.writeByte(node.getPathKind().getId());
      sink.writeInt(node.getReferences());
      sink.writeInt(node.getLevel());
    }

    @Override
    public SirixDeweyID deserializeDeweyID(DataInput source, SirixDeweyID previousDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(DataOutput sink, SirixDeweyID deweyID, SirixDeweyID prevDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },

  /**
   * Node kind is a CAS-RB node.
   */
  CASRB((byte) 17, RBNode.class) {
    @Override
    public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
        final PageReadOnlyTrx pageReadTrx) throws IOException {
      final int valueSize = source.readInt();
      final byte[] value = new byte[valueSize];
      source.readFully(value, 0, valueSize);
      final int typeSize = source.readInt();
      final byte[] type = new byte[typeSize];
      source.readFully(type, 0, typeSize);
      final int keySize = source.readInt();
      final Set<Long> nodeKeys = new HashSet<>(keySize);
      if (keySize > 0) {
        long key = getVarLong(source);
        nodeKeys.add(key);
        for (int i = 1; i < keySize; i++) {
          key += getVarLong(source);
          nodeKeys.add(key);
        }
      }
      final Type atomicType = resolveType(new String(type, Constants.DEFAULT_ENCODING));

      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegateWithoutIDs(source, recordID, pageReadTrx);
      final long leftChild = getVarLong(source);
      final long rightChild = getVarLong(source);
      final long pathNodeKey = getVarLong(source);
      final boolean isChanged = source.readBoolean();

      final Atomic atomic = AtomicUtil.fromBytes(value, atomicType);
      final var node =
          new RBNode<>(new CASValue(atomic, atomicType, pathNodeKey), new NodeReferences(nodeKeys), nodeDel);

      node.setLeftChildKey(leftChild);
      node.setRightChildKey(rightChild);
      node.setChanged(isChanged);
      return node;
    }

    @Override
    public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx)
        throws IOException {
      @SuppressWarnings("unchecked")
      final RBNode<CASValue, NodeReferences> node = (RBNode<CASValue, NodeReferences>) record;
      final CASValue key = node.getKey();
      final byte[] textValue = key.getValue();
      sink.writeInt(textValue.length);
      sink.write(textValue);
      final byte[] type = key.getType().toString().getBytes(Constants.DEFAULT_ENCODING);
      sink.writeInt(type.length);
      sink.write(type);
      final NodeReferences value = node.getValue();
      final Set<Long> nodeKeys = value.getNodeKeys();

      // Store in a list and sort the list.
      final List<Long> listNodeKeys = new ArrayList<>(nodeKeys);
      Collections.sort(listNodeKeys);
      sink.writeInt(listNodeKeys.size());
      if (!listNodeKeys.isEmpty()) {
        putVarLong(sink, listNodeKeys.get(0));
        for (int i = 0; i < listNodeKeys.size(); i++) {
          if (i + 1 < listNodeKeys.size()) {
            final long diff = listNodeKeys.get(i + 1) - listNodeKeys.get(i);
            putVarLong(sink, diff);
          }
        }
      }
      serializeDelegate(node.getNodeDelegate(), sink);
      putVarLong(sink, node.getLeftChildKey());
      putVarLong(sink, node.getRightChildKey());
      putVarLong(sink, key.getPathNodeKey());
      sink.writeBoolean(node.isChanged());
    }

    @Override
    public SirixDeweyID deserializeDeweyID(DataInput source, SirixDeweyID previousDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(DataOutput sink, SirixDeweyID deweyID, SirixDeweyID prevDeweyID,
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
  PATHRB((byte) 18, RBNode.class) {
    @Override
    public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
        final PageReadOnlyTrx pageReadTrx) throws IOException {
      final long key = getVarLong(source);
      final int keySize = source.readInt();
      final Set<Long> nodeKeys = new HashSet<>(keySize);
      for (int i = 0; i < keySize; i++) {
        nodeKeys.add(source.readLong());
      }
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegateWithoutIDs(source, recordID, pageReadTrx);
      final long leftChild = getVarLong(source);
      final long rightChild = getVarLong(source);
      final boolean isChanged = source.readBoolean();
      final RBNode<Long, NodeReferences> node = new RBNode<>(key, new NodeReferences(nodeKeys), nodeDel);
      node.setLeftChildKey(leftChild);
      node.setRightChildKey(rightChild);
      node.setChanged(isChanged);
      return node;
    }

    @Override
    public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx)
        throws IOException {
      @SuppressWarnings("unchecked")
      final RBNode<Long, NodeReferences> node = (RBNode<Long, NodeReferences>) record;
      putVarLong(sink, node.getKey().longValue());
      final NodeReferences value = node.getValue();
      final Set<Long> nodeKeys = value.getNodeKeys();
      sink.writeInt(nodeKeys.size());
      for (final long nodeKey : nodeKeys) {
        sink.writeLong(nodeKey);
      }
      serializeDelegate(node.getNodeDelegate(), sink);
      putVarLong(sink, node.getLeftChildKey());
      putVarLong(sink, node.getRightChildKey());
      sink.writeBoolean(node.isChanged());
    }

    @Override
    public SirixDeweyID deserializeDeweyID(DataInput source, SirixDeweyID previousDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(DataOutput sink, SirixDeweyID deweyID, SirixDeweyID prevDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },

  /**
   * Node kind is a PATH-RB node.
   */
  NAMERB((byte) 19, RBNode.class) {
    @Override
    public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
        final PageReadOnlyTrx pageReadTrx) throws IOException {
      final byte[] nspBytes = new byte[source.readInt()];
      source.readFully(nspBytes);
      final byte[] prefixBytes = new byte[source.readInt()];
      source.readFully(prefixBytes);
      final byte[] localNameBytes = new byte[source.readInt()];
      source.readFully(localNameBytes);
      final QNm name = new QNm(new String(nspBytes, Constants.DEFAULT_ENCODING),
                               new String(prefixBytes, Constants.DEFAULT_ENCODING),
                               new String(localNameBytes, Constants.DEFAULT_ENCODING));
      final int keySize = source.readInt();
      final Set<Long> nodeKeys = new HashSet<>(keySize);
      for (int i = 0; i < keySize; i++) {
        nodeKeys.add(source.readLong());
      }
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegateWithoutIDs(source, recordID, pageReadTrx);
      final long leftChild = getVarLong(source);
      final long rightChild = getVarLong(source);
      final boolean isChanged = source.readBoolean();
      final RBNode<QNm, NodeReferences> node = new RBNode<>(name, new NodeReferences(nodeKeys), nodeDel);
      node.setLeftChildKey(leftChild);
      node.setRightChildKey(rightChild);
      node.setChanged(isChanged);
      return node;
    }

    @Override
    public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx)
        throws IOException {
      @SuppressWarnings("unchecked")
      final RBNode<QNm, NodeReferences> node = (RBNode<QNm, NodeReferences>) record;
      final byte[] nspBytes = node.getKey().getNamespaceURI().getBytes();
      sink.writeInt(nspBytes.length);
      sink.write(nspBytes);
      final byte[] prefixBytes = node.getKey().getPrefix().getBytes();
      sink.writeInt(prefixBytes.length);
      sink.write(prefixBytes);
      final byte[] localNameBytes = node.getKey().getLocalName().getBytes();
      sink.writeInt(localNameBytes.length);
      sink.write(localNameBytes);
      final NodeReferences value = node.getValue();
      final Set<Long> nodeKeys = value.getNodeKeys();
      sink.writeInt(nodeKeys.size());
      for (final long nodeKey : nodeKeys) {
        sink.writeLong(nodeKey);
      }
      serializeDelegate(node.getNodeDelegate(), sink);
      putVarLong(sink, node.getLeftChildKey());
      putVarLong(sink, node.getRightChildKey());
      sink.writeBoolean(node.isChanged());
    }

    @Override
    public SirixDeweyID deserializeDeweyID(DataInput source, SirixDeweyID previousDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(DataOutput sink, SirixDeweyID deweyID, SirixDeweyID prevDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },

  /**
   * Node includes a deweyID &lt;=&gt; nodeKey mapping.
   */
  DEWEYIDMAPPING((byte) 23, DeweyIDMappingNode.class) {
    @Override
    public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
        final PageReadOnlyTrx pageReadTrx) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      throw new UnsupportedOperationException();
    }

    @Override
    public SirixDeweyID deserializeDeweyID(DataInput source, SirixDeweyID previousDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(DataOutput sink, SirixDeweyID deweyID, SirixDeweyID prevDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },

  /**
   * JSON object node.
   */
  OBJECT((byte) 24, ObjectNode.class) {
    @Override
    public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
        final PageReadOnlyTrx pageReadTrx) throws IOException {
      final BigInteger hashCode = getHash(source, pageReadTrx);

      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, pageReadTrx);

      // Struct delegate.
      final StructNodeDelegate structDel =
          deserializeStructDel(this, nodeDel, source, pageReadTrx.getResourceManager().getResourceConfig());

      // Returning an instance.
      return new ObjectNode(hashCode, structDel);
    }

    @Override
    public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx)
        throws IOException {
      final ObjectNode node = (ObjectNode) record;
      if (pageReadTrx.getResourceManager().getResourceConfig().hashType != HashType.NONE) {
        writeHash(sink, node.getHash() == null ? BigInteger.ZERO : node.getHash());
      }
      serializeDelegate(node.getNodeDelegate(), sink);
      serializeStructDelegate(this,
                              node.getStructNodeDelegate(),
                              sink,
                              pageReadTrx.getResourceManager().getResourceConfig());
    }

    @Override
    public SirixDeweyID deserializeDeweyID(DataInput source, SirixDeweyID previousDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(DataOutput sink, SirixDeweyID deweyID, SirixDeweyID prevDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },

  /**
   * JSON array node.
   */
  ARRAY((byte) 25, ArrayNode.class) {
    @Override
    public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
        final PageReadOnlyTrx pageReadTrx) throws IOException {
      final BigInteger hashCode = getHash(source, pageReadTrx);

      final long pathNodeKey = source.readLong();

      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, pageReadTrx);

      // Struct delegate.
      final StructNodeDelegate structDel =
          deserializeStructDel(this, nodeDel, source, pageReadTrx.getResourceManager().getResourceConfig());

      // Returning an instance.
      return new ArrayNode(hashCode, structDel, pathNodeKey);
    }

    @Override
    public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx)
        throws IOException {
      final ArrayNode node = (ArrayNode) record;
      if (pageReadTrx.getResourceManager().getResourceConfig().hashType != HashType.NONE)
        writeHash(sink, node.getHash() == null ? BigInteger.ZERO : node.getHash());
      sink.writeLong(node.getPathNodeKey());
      serializeDelegate(node.getNodeDelegate(), sink);
      serializeStructDelegate(this,
                              node.getStructNodeDelegate(),
                              sink,
                              pageReadTrx.getResourceManager().getResourceConfig());
    }

    @Override
    public SirixDeweyID deserializeDeweyID(DataInput source, SirixDeweyID previousDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(DataOutput sink, SirixDeweyID deweyID, SirixDeweyID prevDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },

  /**
   * JSON array node.
   */
  OBJECT_KEY((byte) 26, ObjectKeyNode.class) {
    @Override
    public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
        final PageReadOnlyTrx pageReadTrx) throws IOException {
      final BigInteger hashCode = getHash(source, pageReadTrx);

      final int nameKey = source.readInt();
      final long pathNodeKey = getVarLong(source);

      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, pageReadTrx);

      // Struct delegate.
      final StructNodeDelegate structDel =
          deserializeStructDel(this, nodeDel, source, pageReadTrx.getResourceManager().getResourceConfig());

      final String name = nameKey == -1 ? "" : pageReadTrx.getName(nameKey, NodeKind.OBJECT_KEY);

      // Name can be null for removed nodes (the previous record page still has the ObjectKeyNode).

      // Returning an instance.
      return new ObjectKeyNode(hashCode, structDel, nameKey, name, pathNodeKey);
    }

    @Override
    public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx)
        throws IOException {
      final ObjectKeyNode node = (ObjectKeyNode) record;
      if (pageReadTrx.getResourceManager().getResourceConfig().hashType != HashType.NONE)
        writeHash(sink, node.getHash() == null ? BigInteger.ZERO : node.getHash());
      sink.writeInt(node.getNameKey());
      putVarLong(sink, node.getPathNodeKey());
      serializeDelegate(node.getNodeDelegate(), sink);
      serializeStructDelegate(this,
                              node.getStructNodeDelegate(),
                              sink,
                              pageReadTrx.getResourceManager().getResourceConfig());
    }

    @Override
    public SirixDeweyID deserializeDeweyID(DataInput source, SirixDeweyID previousDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(DataOutput sink, SirixDeweyID deweyID, SirixDeweyID prevDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },

  /**
   * JSON string value node.
   */
  OBJECT_STRING_VALUE((byte) 40, ObjectStringNode.class) {
    @Override
    public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
        final PageReadOnlyTrx pageReadTrx) throws IOException {
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, pageReadTrx);

      // Val delegate.
      final boolean isCompressed = source.readByte() == (byte) 1;
      final byte[] vals = new byte[source.readInt()];
      source.readFully(vals, 0, vals.length);
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
    public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx)
        throws IOException {
      final ObjectStringNode node = (ObjectStringNode) record;
      serializeDelegate(node.getNodeDelegate(), sink);
      serializeValDelegate(node.getValNodeDelegate(), sink);
    }

    @Override
    public SirixDeweyID deserializeDeweyID(DataInput source, SirixDeweyID previousDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(DataOutput sink, SirixDeweyID deweyID, SirixDeweyID prevDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },

  /**
   * JSON boolean value node.
   */
  OBJECT_BOOLEAN_VALUE((byte) 41, ObjectBooleanNode.class) {
    @Override
    public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
        final PageReadOnlyTrx pageReadTrx) throws IOException {
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
    public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx)
        throws IOException {
      final ObjectBooleanNode node = (ObjectBooleanNode) record;
      sink.writeBoolean(node.getValue());
      serializeDelegate(node.getNodeDelegate(), sink);
    }

    @Override
    public SirixDeweyID deserializeDeweyID(DataInput source, SirixDeweyID previousDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(DataOutput sink, SirixDeweyID deweyID, SirixDeweyID prevDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },

  /**
   * JSON number value node.
   */
  OBJECT_NUMBER_VALUE((byte) 42, ObjectNumberNode.class) {
    @Override
    public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
        final PageReadOnlyTrx pageReadTrx) throws IOException {
      final byte valueType = source.readByte();
      final Number number;

      switch (valueType) {
        case 0:
          number = source.readDouble();
          break;
        case 1:
          number = source.readFloat();
          break;
        case 2:
          number = source.readInt();
          break;
        case 3:
          number = source.readLong();
          break;
        case 4:
          number = deserializeBigInteger(source);
          break;
        case 5:
          final BigInteger bigInt = deserializeBigInteger(source);
          final int scale = source.readInt();
          number = new BigDecimal(bigInt, scale);
          break;
        default:
          throw new AssertionError("Type not known.");
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

    private BigInteger deserializeBigInteger(final DataInput source) throws IOException {
      final byte[] bytes = new byte[source.readInt()];
      source.readFully(bytes);
      return new BigInteger(bytes);
    }

    @Override
    public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx)
        throws IOException {
      final ObjectNumberNode node = (ObjectNumberNode) record;
      final Number number = node.getValue();

      if (number instanceof Double) {
        sink.writeByte(0);
        sink.writeDouble(number.doubleValue());
      } else if (number instanceof Float) {
        sink.writeByte(1);
        sink.writeFloat(number.floatValue());
      } else if (number instanceof Integer) {
        sink.writeByte(2);
        sink.writeInt(number.intValue());
      } else if (number instanceof Long) {
        sink.writeByte(3);
        sink.writeLong(number.longValue());
      } else if (number instanceof BigInteger) {
        sink.writeByte(4);
        serializeBigInteger(sink, (BigInteger) number);
      } else if (number instanceof BigDecimal) {
        sink.writeByte(5);
        final BigDecimal value = (BigDecimal) number;
        final BigInteger bigInt = value.unscaledValue();
        final int scale = value.scale();
        serializeBigInteger(sink, bigInt);
        sink.writeInt(scale);
      } else {
        throw new AssertionError("Type not known.");
      }

      serializeDelegate(node.getNodeDelegate(), sink);
    }

    private void serializeBigInteger(final DataOutput sink, final BigInteger bigInteger) throws IOException {
      final byte[] bytes = bigInteger.toByteArray();
      sink.writeInt(bytes.length);
      sink.write(bytes);
    }

    @Override
    public SirixDeweyID deserializeDeweyID(DataInput source, SirixDeweyID previousDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(DataOutput sink, SirixDeweyID deweyID, SirixDeweyID prevDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },
  /**
   * JSON null node.
   */
  OBJECT_NULL_VALUE((byte) 43, ObjectNullNode.class) {
    @Override
    public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
        final PageReadOnlyTrx pageReadTrx) throws IOException {
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
    public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx)
        throws IOException {
      final ObjectNullNode node = (ObjectNullNode) record;
      serializeDelegate(node.getNodeDelegate(), sink);
    }

    @Override
    public SirixDeweyID deserializeDeweyID(DataInput source, SirixDeweyID previousDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(DataOutput sink, SirixDeweyID deweyID, SirixDeweyID prevDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },

  /**
   * JSON string value node.
   */
  STRING_VALUE((byte) 30, StringNode.class) {
    @Override
    public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
        final PageReadOnlyTrx pageReadTrx) throws IOException {
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, pageReadTrx);

      // Val delegate.
      final boolean isCompressed = source.readByte() == (byte) 1;
      final byte[] vals = new byte[source.readInt()];
      source.readFully(vals, 0, vals.length);
      final ValueNodeDelegate valDel = new ValueNodeDelegate(nodeDel, vals, isCompressed);

      // Struct delegate.
      final StructNodeDelegate structDel =
          deserializeStructDel(this, nodeDel, source, pageReadTrx.getResourceManager().getResourceConfig());

      // Returning an instance.
      return new StringNode(valDel, structDel);
    }

    @Override
    public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx)
        throws IOException {
      final StringNode node = (StringNode) record;
      serializeDelegate(node.getNodeDelegate(), sink);
      serializeValDelegate(node.getValNodeDelegate(), sink);
      serializeStructDelegate(this,
                              node.getStructNodeDelegate(),
                              sink,
                              pageReadTrx.getResourceManager().getResourceConfig());
    }

    @Override
    public SirixDeweyID deserializeDeweyID(DataInput source, SirixDeweyID previousDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(DataOutput sink, SirixDeweyID deweyID, SirixDeweyID prevDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },

  /**
   * JSON boolean value node.
   */
  BOOLEAN_VALUE((byte) 27, BooleanNode.class) {
    @Override
    public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
        final PageReadOnlyTrx pageReadTrx) throws IOException {
      final boolean boolValue = source.readBoolean();
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, pageReadTrx);

      // Struct delegate.
      final StructNodeDelegate structDel =
          deserializeStructDel(this, nodeDel, source, pageReadTrx.getResourceManager().getResourceConfig());

      // Returning an instance.
      return new BooleanNode(boolValue, structDel);
    }

    @Override
    public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx)
        throws IOException {
      final BooleanNode node = (BooleanNode) record;
      sink.writeBoolean(node.getValue());
      serializeDelegate(node.getNodeDelegate(), sink);
      serializeStructDelegate(this,
                              node.getStructNodeDelegate(),
                              sink,
                              pageReadTrx.getResourceManager().getResourceConfig());
    }

    @Override
    public SirixDeweyID deserializeDeweyID(DataInput source, SirixDeweyID previousDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(DataOutput sink, SirixDeweyID deweyID, SirixDeweyID prevDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },

  /**
   * JSON number value node.
   */
  NUMBER_VALUE((byte) 28, NumberNode.class) {
    @Override
    public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
        final PageReadOnlyTrx pageReadTrx) throws IOException {
      final byte valueType = source.readByte();
      final Number number;

      switch (valueType) {
        case 0:
          number = source.readDouble();
          break;
        case 1:
          number = source.readFloat();
          break;
        case 2:
          number = source.readInt();
          break;
        case 3:
          number = source.readLong();
          break;
        case 4:
          number = deserializeBigInteger(source);
          break;
        case 5:
          final BigInteger bigInt = deserializeBigInteger(source);
          final int scale = source.readInt();
          number = new BigDecimal(bigInt, scale);
          break;
        default:
          throw new AssertionError("Type not known.");
      }

      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, pageReadTrx);

      // Struct delegate.
      final StructNodeDelegate structDel =
          deserializeStructDel(this, nodeDel, source, pageReadTrx.getResourceManager().getResourceConfig());

      // Returning an instance.
      return new NumberNode(number, structDel);
    }

    private BigInteger deserializeBigInteger(final DataInput source) throws IOException {
      final byte[] bytes = new byte[source.readInt()];
      source.readFully(bytes);
      return new BigInteger(bytes);
    }

    @Override
    public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx)
        throws IOException {
      final NumberNode node = (NumberNode) record;
      final Number number = node.getValue();

      if (number instanceof Double) {
        sink.writeByte(0);
        sink.writeDouble(number.doubleValue());
      } else if (number instanceof Float) {
        sink.writeByte(1);
        sink.writeFloat(number.floatValue());
      } else if (number instanceof Integer) {
        sink.writeByte(2);
        sink.writeInt(number.intValue());
      } else if (number instanceof Long) {
        sink.writeByte(3);
        sink.writeLong(number.longValue());
      } else if (number instanceof BigInteger) {
        sink.writeByte(4);
        serializeBigInteger(sink, (BigInteger) number);
      } else if (number instanceof BigDecimal) {
        sink.writeByte(5);
        final BigDecimal value = (BigDecimal) number;
        final BigInteger bigInt = value.unscaledValue();
        final int scale = value.scale();
        serializeBigInteger(sink, bigInt);
        sink.writeInt(scale);
      } else {
        throw new AssertionError("Type not known.");
      }

      serializeDelegate(node.getNodeDelegate(), sink);
      serializeStructDelegate(this,
                              node.getStructNodeDelegate(),
                              sink,
                              pageReadTrx.getResourceManager().getResourceConfig());
    }

    private void serializeBigInteger(final DataOutput sink, final BigInteger bigInteger) throws IOException {
      final byte[] bytes = bigInteger.toByteArray();
      sink.writeInt(bytes.length);
      sink.write(bytes);
    }

    @Override
    public SirixDeweyID deserializeDeweyID(DataInput source, SirixDeweyID previousDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(DataOutput sink, SirixDeweyID deweyID, SirixDeweyID prevDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  },
  /**
   * JSON null node.
   */
  NULL_VALUE((byte) 29, NullNode.class) {
    @Override
    public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
        final PageReadOnlyTrx pageReadTrx) throws IOException {
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID, deweyID, pageReadTrx);

      // Struct delegate.
      final StructNodeDelegate structDel =
          deserializeStructDel(this, nodeDel, source, pageReadTrx.getResourceManager().getResourceConfig());

      // Returning an instance.
      return new NullNode(structDel);
    }

    @Override
    public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx)
        throws IOException {
      final NullNode node = (NullNode) record;
      serializeDelegate(node.getNodeDelegate(), sink);
      serializeStructDelegate(this,
                              node.getStructNodeDelegate(),
                              sink,
                              pageReadTrx.getResourceManager().getResourceConfig());
    }

    @Override
    public SirixDeweyID deserializeDeweyID(DataInput source, SirixDeweyID previousDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(DataOutput sink, SirixDeweyID deweyID, SirixDeweyID prevDeweyID,
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
    public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
        final PageReadOnlyTrx pageReadTrx) throws IOException {

      final HashFunction hashFunction = pageReadTrx.getResourceManager().getResourceConfig().nodeHashFunction;

      final NodeDelegate nodeDel = new NodeDelegate(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(),
                                                    Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                    hashFunction,
                                                    null,
                                                    getVarLong(source),
                                                    SirixDeweyID.newRootID());
      final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
                                                                  getVarLong(source),
                                                                  getVarLong(source),
                                                                  Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                  Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                  source.readByte() == ((byte) 0) ? 0 : 1,
                                                                  source.readLong());
      return new JsonDocumentRootNode(nodeDel, structDel);
    }

    @Override
    public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx)
        throws IOException {
      final JsonDocumentRootNode node = (JsonDocumentRootNode) record;
      putVarLong(sink, node.getRevision());
      putVarLong(sink, node.getFirstChildKey());
      putVarLong(sink, node.getLastChildKey());
      sink.writeByte(node.hasFirstChild() ? (byte) 1 : (byte) 0);
      sink.writeByte(node.hasLastChild() ? (byte) 1 : (byte) 0);
      sink.writeLong(node.getDescendantCount());
    }

    @Override
    public SirixDeweyID deserializeDeweyID(DataInput source, SirixDeweyID previousDeweyID,
        ResourceConfiguration resourceConfig) {
      return null;
    }

    @Override
    public void serializeDeweyID(DataOutput sink, SirixDeweyID deweyID, SirixDeweyID prevDeweyID,
        ResourceConfiguration resourceConfig) {
    }
  },

  HASH_ENTRY((byte) 32, HashEntryNode.class) {
    @Override
    public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
        final PageReadOnlyTrx pageReadTrx) throws IOException {
      return new HashEntryNode(recordID, source.readInt(), source.readUTF());
    }

    @Override
    public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx)
        throws IOException {
      final HashEntryNode node = (HashEntryNode) record;
      sink.writeInt(node.getKey());
      sink.writeUTF(node.getValue());
    }

    @Override
    public SirixDeweyID deserializeDeweyID(DataInput source, SirixDeweyID previousDeweyID,
        ResourceConfiguration resourceConfig) {
      return null;
    }

    @Override
    public void serializeDeweyID(DataOutput sink, SirixDeweyID deweyID, SirixDeweyID prevDeweyID,
        ResourceConfiguration resourceConfig) {
    }
  },

  HASH_NAME_COUNT_TO_NAME_ENTRY((byte) 33, HashCountEntryNode.class) {
    @Override
    public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
        final PageReadOnlyTrx pageReadTrx) throws IOException {
      return new HashCountEntryNode(recordID, source.readInt());
    }

    @Override
    public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx)
        throws IOException {
      final HashCountEntryNode node = (HashCountEntryNode) record;
      sink.writeInt(node.getValue());
    }

    @Override
    public SirixDeweyID deserializeDeweyID(DataInput source, SirixDeweyID previousDeweyID,
        ResourceConfiguration resourceConfig) {
      return null;
    }

    @Override
    public void serializeDeweyID(DataOutput sink, SirixDeweyID deweyID, SirixDeweyID prevDeweyID,
        ResourceConfiguration resourceConfig) {
    }
  },

  DEWEY_ID_NODE((byte) 34, DeweyIDNode.class) {
    @Override
    public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
        final PageReadOnlyTrx pageReadTrx) {
      return new DeweyIDNode(recordID, deweyID);
    }

    @Override
    public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
    }

    @Override
    public SirixDeweyID deserializeDeweyID(DataInput source, SirixDeweyID previousDeweyID,
        ResourceConfiguration resourceConfig) {
      return null;
    }

    @Override
    public void serializeDeweyID(DataOutput sink, SirixDeweyID deweyID, SirixDeweyID prevDeweyID,
        ResourceConfiguration resourceConfig) {
    }
  },

  /**
   * Node type not known.
   */
  UNKNOWN((byte) 22, null) {
    @Override
    public DataRecord deserialize(final DataInput source, final @Nonnegative long recordID, final SirixDeweyID deweyID,
        final PageReadOnlyTrx pageReadTrx) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serialize(final DataOutput sink, final DataRecord record, final PageReadOnlyTrx pageReadTrx) {
      throw new UnsupportedOperationException();
    }

    @Override
    public SirixDeweyID deserializeDeweyID(DataInput source, SirixDeweyID previousDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeDeweyID(DataOutput sink, SirixDeweyID deweyID, SirixDeweyID prevDeweyID,
        ResourceConfiguration resourceConfig) {
      throw new UnsupportedOperationException();
    }
  };

  /**
   * Identifier.
   */
  private final byte mId;

  /**
   * Class.
   */
  private final Class<? extends DataRecord> mClass;

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
      INSTANCEFORID.put(node.mId, node);
      INSTANCEFORCLASS.put(node.mClass, node);
    }
  }

  /**
   * Constructor.
   *
   * @param id    unique identifier
   * @param clazz class
   */
  NodeKind(final byte id, final Class<? extends DataRecord> clazz) {
    mId = id;
    mClass = clazz;
  }

  /**
   * Get the nodeKind.
   *
   * @return the unique kind
   */
  public byte getId() {
    return mId;
  }

  /**
   * Get class of node.
   *
   * @return class of node
   */
  public Class<? extends DataRecord> getNodeClass() {
    return mClass;
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
  public SirixDeweyID deserializeDeweyID(DataInput source, SirixDeweyID previousDeweyID,
      ResourceConfiguration resourceConfig) throws IOException {
    return null;
  }

  @Override
  public void serializeDeweyID(DataOutput sink, SirixDeweyID deweyID, SirixDeweyID nextDeweyID,
      ResourceConfiguration resourceConfig) throws IOException {
  }

  private static final BigInteger getHash(final DataInput source, final PageReadOnlyTrx pageReadTrx)
      throws IOException {
    final BigInteger hashCode;
    if (pageReadTrx.getResourceManager().getResourceConfig().hashType == HashType.NONE)
      hashCode = null;
    else
      hashCode = readHash(source);
    return hashCode;
  }

  private static final NodeDelegate deserializeNodeDelegateWithoutIDs(final DataInput source,
      final @Nonnegative long recordID, final PageReadOnlyTrx pageReadTrx) throws IOException {
    final long nodeKey = recordID;
    final long parentKey = nodeKey - getVarLong(source);
    final long revision = getVarLong(source);
    final HashFunction hashFunction = pageReadTrx.getResourceManager().getResourceConfig().nodeHashFunction;
    return new NodeDelegate(nodeKey, parentKey, hashFunction, null, revision, null);
  }

  private static final NodeDelegate deserializeNodeDelegate(final DataInput source, final @Nonnegative long recordID,
      final SirixDeweyID id, final PageReadOnlyTrx pageReadTrx) throws IOException {
    final long nodeKey = recordID;
    final long parentKey = nodeKey - getVarLong(source);
    final long revision = getVarLong(source);
    final HashFunction hashFunction = pageReadTrx.getResourceManager().getResourceConfig().nodeHashFunction;
    return new NodeDelegate(nodeKey, parentKey, hashFunction, null, revision, id);
  }

  private static final void serializeDelegate(final NodeDelegate nodeDel, final DataOutput sink) throws IOException {
    putVarLong(sink, nodeDel.getNodeKey() - nodeDel.getParentKey());
    putVarLong(sink, nodeDel.getRevision());
  }

  private static final void serializeStructDelegate(final NodeKind kind, final StructNodeDelegate nodeDel,
      final DataOutput sink, final ResourceConfiguration config) throws IOException {
    final var isValueNode =
        kind == NodeKind.NUMBER_VALUE || kind == NodeKind.STRING_VALUE || kind == NodeKind.BOOLEAN_VALUE
            || kind == NodeKind.NULL_VALUE;

    final boolean storeChildCount = config.getStoreChildCount();

    putVarLong(sink, nodeDel.getNodeKey() - nodeDel.getRightSiblingKey());
    putVarLong(sink, nodeDel.getNodeKey() - nodeDel.getLeftSiblingKey());

    if (!isValueNode) {
      putVarLong(sink, nodeDel.getNodeKey() - nodeDel.getFirstChildKey());
      putVarLong(sink, nodeDel.getNodeKey() - nodeDel.getLastChildKey());
      if (storeChildCount) {
        putVarLong(sink, nodeDel.getNodeKey() - nodeDel.getChildCount());
      }

      if (config.hashType != HashType.NONE)
        putVarLong(sink, nodeDel.getDescendantCount() - nodeDel.getChildCount());
    }
  }

  private static final StructNodeDelegate deserializeStructDel(final NodeKind kind, final NodeDelegate nodeDel,
      final DataInput source, final ResourceConfiguration config) throws IOException {
    final long currKey = nodeDel.getNodeKey();
    final boolean storeChildNodes = config.getStoreChildCount();

    final var isValueNode =
        kind == NodeKind.NUMBER_VALUE || kind == NodeKind.STRING_VALUE || kind == NodeKind.BOOLEAN_VALUE
            || kind == NodeKind.NULL_VALUE;

    final var isJsonNode =
        kind == NodeKind.OBJECT || kind == NodeKind.ARRAY || kind == NodeKind.OBJECT_KEY || isValueNode;

    final long rightSibl;
    final long leftSibl;
    final long firstChild;
    final long lastChild;
    final long childCount;

    rightSibl = currKey - getVarLong(source);
    leftSibl = currKey - getVarLong(source);

    if (isValueNode) {
      firstChild = Fixed.NULL_NODE_KEY.getStandardProperty();
      lastChild = Fixed.NULL_NODE_KEY.getStandardProperty();
    } else {
      firstChild = currKey - getVarLong(source);
      lastChild = currKey - getVarLong(source);
    }

    if (isValueNode || !storeChildNodes)
      childCount = 0;
    else
      childCount = currKey - getVarLong(source);

    final long descendantCount;

    if (config.hashType == HashType.NONE || isValueNode) {
      descendantCount = 0;
    } else {
      descendantCount = getVarLong(source) + childCount;
    }

    if (isJsonNode)
      return new StructNodeDelegate(nodeDel, firstChild, lastChild, rightSibl, leftSibl, childCount, descendantCount);

    return new StructNodeDelegate(nodeDel, firstChild, rightSibl, leftSibl, childCount, descendantCount);
  }

  private static final NameNodeDelegate deserializeNameDelegate(final NodeDelegate nodeDel, final DataInput source)
      throws IOException {
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
  private static final void serializeNameDelegate(final NameNodeDelegate nameDel, final DataOutput sink)
      throws IOException {
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
  private static final void serializeValDelegate(final ValueNodeDelegate valueDel, final DataOutput sink)
      throws IOException {
    final boolean isCompressed = valueDel.isCompressed();
    sink.writeByte(isCompressed ? (byte) 1 : (byte) 0);
    final byte[] value = isCompressed ? valueDel.getCompressed() : valueDel.getRawValue();
    sink.writeInt(value.length);
    sink.write(value);
  }

  private static BigInteger readHash(final DataInput source) throws IOException {
    final byte[] hashBytes = new byte[source.readByte()];
    source.readFully(hashBytes);
    return new BigInteger(1, hashBytes);
  }

  private static void writeHash(final DataOutput sink, final BigInteger hashCode) throws IOException {
    final byte[] bigIntegerBytes = hashCode.toByteArray();
    final List<Byte> bytes = new ArrayList<>();
    final int maxLength = Math.min(bigIntegerBytes.length, 17);

    for (int i = 1; i < maxLength; i++) {
      bytes.add(bigIntegerBytes[i]);
    }

    assert bytes.size() < 17;

    sink.writeByte(bigIntegerBytes.length);
    sink.write(bigIntegerBytes);
  }

  /**
   * Simple DumbNode just for testing the {@link UnorderedKeyValuePage}s.
   *
   * @author Sebastian Graf, University of Konstanz
   * @author Johannes Lichtenberger
   */
  public static class DumbNode implements DataRecord {

    /**
     * Node key.
     */
    private final long nodeKey;

    /**
     * Simple constructor.
     *
     * @param nodeKey to be set
     */
    public DumbNode(final long nodeKey) {
      this.nodeKey = nodeKey;
    }

    @Override
    public long getNodeKey() {
      return nodeKey;
    }

    @Override
    public NodeKind getKind() {
      return NodeKind.NULL;
    }

    @Override
    public long getRevision() {
      return 0;
    }

    @Override
    public SirixDeweyID getDeweyID() {
      return null;
    }
  }
}
