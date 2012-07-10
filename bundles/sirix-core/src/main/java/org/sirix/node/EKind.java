/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
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
package org.sirix.node;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.indexes.PathNode;
import org.sirix.io.ITTSink;
import org.sirix.io.ITTSource;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.delegates.ValNodeDelegate;
import org.sirix.node.interfaces.IKind;
import org.sirix.node.interfaces.INode;
import org.sirix.service.xml.xpath.AtomicValue;
import org.sirix.settings.EFixed;

/**
 * Enumeration for different nodes. All nodes are determined by a unique id.
 * 
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public enum EKind implements IKind {

  /** Unknown kind. */
  UNKOWN((byte)0, null) {
    @Override
    public INode deserialize(final ITTSource pSource) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serialize(final ITTSink pSink, final INode pToSerialize) {
      throw new UnsupportedOperationException();
    }
  },

  /** Node kind is element. */
  ELEMENT((byte)1, ElementNode.class) {
    @Override
    public INode deserialize(final ITTSource pSource) {
      final List<Long> attrKeys = new ArrayList<>();
      final BiMap<Integer, Long> attrs = HashBiMap.<Integer, Long> create();
      final List<Long> namespKeys = new ArrayList<>();

      // Node delegate.
      final NodeDelegate nodeDel =
        new NodeDelegate(pSource.readLong(), pSource.readLong(), pSource
          .readLong(), pSource.readLong());

      // Struct delegate.
      final StructNodeDelegate structDel =
        deserializeStructDel(nodeDel, pSource);

      // Name delegate.
      final NameNodeDelegate nameDel =
        new NameNodeDelegate(nodeDel, pSource.readInt(), pSource.readInt());

      // Attributes.
      int attrCount = pSource.readInt();
      for (int i = 0; i < attrCount; i++) {
        final long nodeKey = pSource.readLong();
        attrKeys.add(nodeKey);
        attrs.put(pSource.readInt(), nodeKey);
      }

      // Namespaces.
      int nsCount = pSource.readInt();
      for (int i = 0; i < nsCount; i++) {
        namespKeys.add(pSource.readLong());
      }

      return new ElementNode(nodeDel, structDel, nameDel, attrKeys, attrs,
        namespKeys);
    }

    @Override
    public void serialize(final ITTSink pSink, final INode pToSerialize) {
      final ElementNode node = (ElementNode)pToSerialize;
      serializeDelegate(node.getNodeDelegate(), pSink);
      pSink.writeLong(node.getPCR());
      serializeStrucDelegate(node.getStructNodeDelegate(), pSink);
      serializeNameDelegate(node.getNameNodeDelegate(), pSink);
      pSink.writeInt(node.getAttributeCount());
      for (int i = 0, attCount = node.getAttributeCount(); i < attCount; i++) {
        pSink.writeLong(node.getAttributeKey(i));
        pSink.writeInt(node.getNameKey());
      }
      pSink.writeInt(node.getNamespaceCount());
      for (int i = 0, nspCount = node.getNamespaceCount(); i < nspCount; i++) {
        pSink.writeLong(node.getNamespaceKey(i));
      }
    }
  },

  /** Node kind is attribute. */
  ATTRIBUTE((byte)2, AttributeNode.class) {
    @Override
    public INode deserialize(final ITTSource pSource) {
      // Node delegate.
      final NodeDelegate nodeDel =
        new NodeDelegate(pSource.readLong(), pSource.readLong(), pSource
          .readLong(), pSource.readLong());
      // Name delegate.
      final NameNodeDelegate nameDel =
        new NameNodeDelegate(nodeDel, pSource.readInt(), pSource.readInt());
      // Val delegate.
      final boolean isCompressed = pSource.readByte() == (byte)1 ? true : false;
      final byte[] vals = new byte[pSource.readInt()];
      for (int i = 0; i < vals.length; i++) {
        vals[i] = pSource.readByte();
      }
      final ValNodeDelegate valDel =
        new ValNodeDelegate(nodeDel, vals, isCompressed);

      return new AttributeNode(nodeDel, nameDel, valDel);
    }

    @Override
    public void serialize(final ITTSink pSink, final INode pToSerialize) {
      AttributeNode node = (AttributeNode)pToSerialize;
      serializeDelegate(node.getNodeDelegate(), pSink);
      pSink.writeLong(node.getPCR());
      serializeNameDelegate(node.getNameNodeDelegate(), pSink);
      serializeValDelegate(node.getValNodeDelegate(), pSink);
    }

  },

  /** Node kind is text. */
  TEXT((byte)3, TextNode.class) {
    @Override
    public INode deserialize(final ITTSource pSource) {
      // Node delegate.
      final NodeDelegate nodeDel =
        new NodeDelegate(pSource.readLong(), pSource.readLong(), pSource
          .readLong(), 0);
      // Val delegate.
      final boolean isCompressed = pSource.readByte() == (byte)1 ? true : false;
      final byte[] vals = new byte[pSource.readInt()];
      for (int i = 0; i < vals.length; i++) {
        vals[i] = pSource.readByte();
      }
      final ValNodeDelegate valDel =
        new ValNodeDelegate(nodeDel, vals, isCompressed);
      // Struct delegate.
      final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, EFixed.NULL_NODE_KEY
          .getStandardProperty(), pSource.readLong(), pSource.readLong(), 0L,
          0L);
      // Returning an instance.
      return new TextNode(nodeDel, valDel, structDel);
    }

    @Override
    public void serialize(final ITTSink pSink, final INode pToSerialize) {
      TextNode node = (TextNode)pToSerialize;
      serializeDelegate(node.getNodeDelegate(), pSink);
      serializeValDelegate(node.getValNodeDelegate(), pSink);
      final StructNodeDelegate del = node.getStructNodeDelegate();
      pSink.writeLong(del.getRightSiblingKey());
      pSink.writeLong(del.getLeftSiblingKey());
    }
  },

  /** Node kind is namespace. */
  NAMESPACE((byte)13, NamespaceNode.class) {
    @Override
    public INode deserialize(final ITTSource pSource) {
      // Node delegate.
      final NodeDelegate nodeDel =
        new NodeDelegate(pSource.readLong(), pSource.readLong(), pSource
          .readLong(), 0);
      // Name delegate.
      final NameNodeDelegate nameDel =
        new NameNodeDelegate(nodeDel, pSource.readInt(), pSource.readInt());
      return new NamespaceNode(nodeDel, nameDel);
    }

    @Override
    public void serialize(final ITTSink pSink, final INode pToSerialize) {
      NamespaceNode node = (NamespaceNode)pToSerialize;
      serializeDelegate(node.getNodeDelegate(), pSink);
      serializeNameDelegate(node.getNameNodeDelegate(), pSink);
    }

  },

  /** Node kind is processing instruction. */
  PROCESSING((byte)7, null) {
    @Override
    public INode deserialize(final ITTSource parapSource) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serialize(final ITTSink pSink, final INode pToSerialize) {
      throw new UnsupportedOperationException();
    }

  },

  /** Node kind is comment. */
  COMMENT((byte)8, null) {
    @Override
    public INode deserialize(final ITTSource pSource) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serialize(final ITTSink pSink, final INode pToSerialize) {
      throw new UnsupportedOperationException();
    }

  },

  /** Node kind is document root. */
  DOCUMENT_ROOT((byte)9, DocumentRootNode.class) {
    @Override
    public INode deserialize(final ITTSource pSource) {
      final NodeDelegate nodeDel =
        new NodeDelegate(EFixed.ROOT_NODE_KEY.getStandardProperty(),
          EFixed.NULL_NODE_KEY.getStandardProperty(), pSource.readLong(), 0);
      final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, pSource.readLong(),
          EFixed.NULL_NODE_KEY.getStandardProperty(), EFixed.NULL_NODE_KEY
            .getStandardProperty(), pSource.readByte() == ((byte)0) ? 0 : 1,
          pSource.readLong());
      return new DocumentRootNode(nodeDel, structDel);
    }

    @Override
    public void serialize(final ITTSink pSink, final INode pToSerialize) {
      DocumentRootNode node = (DocumentRootNode)pToSerialize;
      pSink.writeLong(node.getHash());
      pSink.writeLong(node.getFirstChildKey());
      pSink.writeByte(node.hasFirstChild() ? (byte)1 : (byte)0);
      pSink.writeLong(node.getDescendantCount());
    }
  },

  /** Whitespace text. */
  WHITESPACE((byte)4, null) {
    @Override
    public INode deserialize(final ITTSource pSource) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serialize(final ITTSink pSink, final INode pToSerialize) {
      throw new UnsupportedOperationException();
    }

  },

  /** Node kind is deleted node. */
  DELETE((byte)5, DeletedNode.class) {
    @Override
    public INode deserialize(final ITTSource pSource) {
      final NodeDelegate delegate =
        new NodeDelegate(pSource.readLong(), pSource.readLong(), pSource
          .readLong(), 0);
      final DeletedNode node = new DeletedNode(delegate);
      return node;
    }

    @Override
    public void serialize(final ITTSink pSink, final INode pToSerialize) {
      DeletedNode node = (DeletedNode)pToSerialize;
      serializeDelegate(node.getNodeDelegate(), pSink);
    }
  },

  /** NullNode to support the Null Object pattern. */
  NULL((byte)6, NullNode.class) {
    @Override
    public INode deserialize(final ITTSource pSource) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serialize(final ITTSink pSink, final INode pToSerialize) {
      throw new UnsupportedOperationException();
    }
  },

  /** AtomicKind. */
  ATOMIC((byte)15, AtomicValue.class) {
    @Override
    public INode deserialize(final ITTSource pSource) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serialize(final ITTSink pSink, final INode pToSerialize) {
      throw new UnsupportedOperationException();
    }
  },

  /** Node kind is path node. */
  PATH((byte)16, PathNode.class) {
    @Override
    public INode deserialize(final ITTSource pSource) {
      // Node delegate.
      final NodeDelegate nodeDel =
        new NodeDelegate(pSource.readLong(), pSource.readLong(), pSource
          .readLong(), pSource.readLong());

      // Struct delegate.
      final StructNodeDelegate structDel =
        deserializeStructDel(nodeDel, pSource);

      // Name delegate.
      final NameNodeDelegate nameDel =
        new NameNodeDelegate(nodeDel, pSource.readInt(), pSource.readInt());

      return new PathNode(EKind.getKind(pSource.readByte()), nodeDel, structDel, nameDel);
    }

    @Override
    public void serialize(final ITTSink pSink, final INode pToSerialize) {
      final PathNode node = (PathNode)pToSerialize;
      serializeDelegate(node.getNodeDelegate(), pSink);
      pSink.writeLong(node.getPCR());
      serializeStrucDelegate(node.getStructNodeDelegate(), pSink);
      serializeNameDelegate(node.getNameNodeDelegate(), pSink);
      pSink.writeByte(node.getPathKind().getId());
    };
  };

  /** Identifier. */
  private final byte mId;

  /** Class. */
  private final Class<? extends INode> mClass;

  /** Mapping of keys -> nodes. */
  private static final Map<Byte, EKind> INSTANCEFORID = new HashMap<>();

  /** Mapping of class -> nodes. */
  private static final Map<Class<? extends INode>, EKind> INSTANCEFORCLASS =
    new HashMap<>();

  static {
    for (final EKind node : values()) {
      INSTANCEFORID.put(node.mId, node);
      INSTANCEFORCLASS.put(node.mClass, node);
    }
  }

  /**
   * Constructor.
   * 
   * @param pId
   *          unique identifier
   * @param pClass
   *          class
   */
  private EKind(final byte pId, final Class<? extends INode> pClass) {
    mId = pId;
    mClass = pClass;
  }

  /**
   * Deserialize struct delegate.
   * 
   * @param nodeDel
   *          node delegate
   * @param pSource
   *          input source
   * @return {@link StructNodeDelegate} instance
   */
  protected StructNodeDelegate deserializeStructDel(
    @Nonnull final NodeDelegate nodeDel, @Nonnull final ITTSource pSource) {
    final long currKey = nodeDel.getNodeKey();
    final long rightSibl = readPointer(pSource, currKey);
    final long leftSibl = readPointer(pSource, currKey);
    final long firstChild = readPointer(pSource, currKey);
    final long childCount = pSource.readLong();
    final long descendantCount = pSource.readLong() + childCount;
    return new StructNodeDelegate(nodeDel, firstChild, rightSibl, leftSibl,
      childCount, descendantCount);
  }

  @Override
  public byte getId() {
    return mId;
  }

  @Override
  public Class<? extends INode> getNodeClass() {
    return mClass;
  }

  /**
   * Public method to get the related node based on the identifier.
   * 
   * @param pId
   *          the identifier for the node
   * @return the related node
   */
  public static EKind getKind(final byte pId) {
    return INSTANCEFORID.get(pId);
  }

  /**
   * Public method to get the related node based on the class.
   * 
   * @param pClass
   *          the class for the node
   * @return the related node
   */
  public static EKind getKind(@Nonnull final Class<? extends INode> pClass) {
    return INSTANCEFORCLASS.get(pClass);
  }

  /**
   * Serializing the {@link NodeDelegate} instance.
   * 
   * @param pDel
   *          to be serialize
   * @param pSink
   *          to serialize to.
   */
  private static final void serializeDelegate(@Nonnull final NodeDelegate pDel,
    @Nonnull final ITTSink pSink) {
    pSink.writeLong(pDel.getNodeKey());
    pSink.writeLong(pDel.getParentKey());
    pSink.writeLong(pDel.getHash());
  }

  /**
   * Serializing the {@link StructNodeDelegate} instance.
   * 
   * @param pDel
   *          to be serialize
   * @param pSink
   *          to serialize to.
   */
  private static final void serializeStrucDelegate(
    @Nonnull final StructNodeDelegate pDel, @Nonnull final ITTSink pSink) {
    writePointer(pSink, pDel.getNodeKey(), pDel.getRightSiblingKey());
    writePointer(pSink, pDel.getNodeKey(), pDel.getLeftSiblingKey());
    writePointer(pSink, pDel.getNodeKey(), pDel.getFirstChildKey());
    pSink.writeLong(pDel.getChildCount());
    pSink.writeLong(pDel.getDescendantCount() - pDel.getChildCount());
  }

  /**
   * Deserialize a structural node pointer (ranges).
   * 
   * @param pSource
   *          input source
   * @param pSelf
   *          the pointer
   * @return the pointer
   */
  private static long readPointer(@Nonnull final ITTSource pSource,
    final long pSelf) {
    boolean isNullKey = pSource.readByte() == (byte)1 ? true : false;
    if (isNullKey) {
      return EFixed.NULL_NODE_KEY.getStandardProperty();
    } else {
      final long pointer = pSource.readLong();
      assert pointer != 0 : "May never be 0!";
      return pointer > 0 ? pSelf - pointer : Math.abs(pointer - pSelf);
    }
  }

  /**
   * Write a structural node pointer (ranges).
   * 
   * @param pSink
   *          output sink
   * @param pSelf
   *          the pointer
   * @param pPointer
   *          the pointer
   */
  private static final void writePointer(@Nonnull final ITTSink pSink,
    @Nonnegative final long pSelf, @Nonnegative final long pPointer) {
    boolean isNullNodeKey =
      pPointer == EFixed.NULL_NODE_KEY.getStandardProperty() ? true : false;
    pSink.writeByte(isNullNodeKey ? (byte)1 : (byte)0);
    if (!isNullNodeKey) {
      final long toStore = pSelf - pPointer;
      assert toStore != 0 : "May never be 0!";
      pSink.writeLong(toStore);
    }
  }

  /**
   * Serializing the {@link NameNodeDelegate} instance.
   * 
   * @param pDel
   *          to be serialized
   * @param pSink
   *          to serialize to
   */
  private static final void serializeNameDelegate(
    @Nonnull final NameNodeDelegate pDel, @Nonnull final ITTSink pSink) {
    pSink.writeInt(pDel.getNameKey());
    pSink.writeInt(pDel.getURIKey());
  }

  /**
   * Serializing the {@link ValNodeDelegate} instance.
   * 
   * @param pDel
   *          to be serialized
   * @param pSink
   *          to serialize to
   */
  private static final void serializeValDelegate(
    @Nonnull final ValNodeDelegate pDel, @Nonnull final ITTSink pSink) {
    final boolean isCompressed = pDel.isCompressed();
    pSink.writeByte(isCompressed ? (byte)1 : (byte)0);
    final byte[] value =
      isCompressed ? pDel.getCompressed() : pDel.getRawValue();
    pSink.writeInt(value.length);
    for (final byte byteVal : value) {
      pSink.writeByte(byteVal);
    }
  }
}
