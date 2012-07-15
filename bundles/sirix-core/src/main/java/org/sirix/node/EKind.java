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
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.sirix.index.path.PathNode;
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
    public INode deserialize(@Nonnull final ByteArrayDataInput pSource) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serialize(@Nonnull final ByteArrayDataOutput pSink,
      @Nonnull final INode pToSerialize) {
      throw new UnsupportedOperationException();
    }
  },

  /** Node kind is element. */
  ELEMENT((byte)1, ElementNode.class) {
    @Override
    public INode deserialize(@Nonnull final ByteArrayDataInput pSource) {
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(pSource);

      // Struct delegate.
      final StructNodeDelegate structDel =
        deserializeStructDel(nodeDel, pSource);

      // Name delegate.
      final NameNodeDelegate nameDel =
        deserializeNameDelegate(nodeDel, pSource);

      // Attributes.
      int attrCount = pSource.readInt();
      final List<Long> attrKeys = new ArrayList<>(attrCount);
      final BiMap<Integer, Long> attrs = HashBiMap.<Integer, Long> create();
      for (int i = 0; i < attrCount; i++) {
        final long nodeKey = pSource.readLong();
        attrKeys.add(nodeKey);
        attrs.put(pSource.readInt(), nodeKey);
      }

      // Namespaces.
      int nsCount = pSource.readInt();
      final List<Long> namespKeys = new ArrayList<>(nsCount);
      for (int i = 0; i < nsCount; i++) {
        namespKeys.add(pSource.readLong());
      }

      return new ElementNode(nodeDel, structDel, nameDel, attrKeys, attrs,
        namespKeys);
    }

    @Override
    public void serialize(@Nonnull final ByteArrayDataOutput pSink,
      @Nonnull final INode pToSerialize) {
      final ElementNode node = (ElementNode)pToSerialize;
      serializeDelegate(node.getNodeDelegate(), pSink);
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
    public INode deserialize(@Nonnull final ByteArrayDataInput pSource) {
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(pSource);

      // Name delegate.
      final NameNodeDelegate nameDel =
        deserializeNameDelegate(nodeDel, pSource);

      // Val delegate.
      final boolean isCompressed = pSource.readByte() == (byte)1 ? true : false;
      final byte[] vals = new byte[pSource.readInt()];
      pSource.readFully(vals, 0, vals.length);
      final ValNodeDelegate valDel =
        new ValNodeDelegate(nodeDel, vals, isCompressed);

      // Returning an instance.
      return new AttributeNode(nodeDel, nameDel, valDel);
    }

    @Override
    public void serialize(@Nonnull final ByteArrayDataOutput pSink,
      @Nonnull final INode pToSerialize) {
      AttributeNode node = (AttributeNode)pToSerialize;
      serializeDelegate(node.getNodeDelegate(), pSink);
      serializeNameDelegate(node.getNameNodeDelegate(), pSink);
      serializeValDelegate(node.getValNodeDelegate(), pSink);
    }
  },

  /** Node kind is namespace. */
  NAMESPACE((byte)13, NamespaceNode.class) {
    @Override
    public INode deserialize(@Nonnull final ByteArrayDataInput pSource) {
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(pSource);

      // Name delegate.
      final NameNodeDelegate nameDel =
        deserializeNameDelegate(nodeDel, pSource);

      return new NamespaceNode(nodeDel, nameDel);
    }

    @Override
    public void serialize(@Nonnull final ByteArrayDataOutput pSink,
      @Nonnull final INode pToSerialize) {
      NamespaceNode node = (NamespaceNode)pToSerialize;
      serializeDelegate(node.getNodeDelegate(), pSink);
      serializeNameDelegate(node.getNameNodeDelegate(), pSink);
    }
  },

  /** Node kind is text. */
  TEXT((byte)3, TextNode.class) {
    @Override
    public INode deserialize(@Nonnull final ByteArrayDataInput pSource) {
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(pSource);

      // Val delegate.
      final boolean isCompressed = pSource.readByte() == (byte)1 ? true : false;
      final byte[] vals = new byte[pSource.readInt()];
      pSource.readFully(vals, 0, vals.length);
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
    public void serialize(@Nonnull final ByteArrayDataOutput pSink,
      @Nonnull final INode pToSerialize) {
      TextNode node = (TextNode)pToSerialize;
      serializeDelegate(node.getNodeDelegate(), pSink);
      serializeValDelegate(node.getValNodeDelegate(), pSink);
      final StructNodeDelegate del = node.getStructNodeDelegate();
      pSink.writeLong(del.getRightSiblingKey());
      pSink.writeLong(del.getLeftSiblingKey());
    }
  },

  /** Node kind is processing instruction. */
  PROCESSING((byte)7, null) {
    @Override
    public INode deserialize(@Nonnull final ByteArrayDataInput pSource) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serialize(@Nonnull final ByteArrayDataOutput pSink,
      @Nonnull final INode pToSerialize) {
      throw new UnsupportedOperationException();
    }

  },

  /** Node kind is comment. */
  COMMENT((byte)8, null) {
    @Override
    public INode deserialize(@Nonnull final ByteArrayDataInput pSource) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serialize(@Nonnull final ByteArrayDataOutput pSink,
      @Nonnull final INode pToSerialize) {
      throw new UnsupportedOperationException();
    }
  },

  /** Node kind is document root. */
  DOCUMENT_ROOT((byte)9, DocumentRootNode.class) {
    @Override
    public INode deserialize(@Nonnull final ByteArrayDataInput pSource) {
      final NodeDelegate nodeDel =
        new NodeDelegate(EFixed.DOCUMENT_NODE_KEY.getStandardProperty(),
          EFixed.NULL_NODE_KEY.getStandardProperty(), pSource.readLong());
      final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, pSource.readLong(),
          EFixed.NULL_NODE_KEY.getStandardProperty(), EFixed.NULL_NODE_KEY
            .getStandardProperty(), pSource.readByte() == ((byte)0) ? 0 : 1,
          pSource.readLong());
      return new DocumentRootNode(nodeDel, structDel);
    }

    @Override
    public void serialize(@Nonnull final ByteArrayDataOutput pSink,
      @Nonnull final INode pToSerialize) {
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
    public INode deserialize(@Nonnull final ByteArrayDataInput pSource) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serialize(@Nonnull final ByteArrayDataOutput pSink,
      @Nonnull final INode pToSerialize) {
      throw new UnsupportedOperationException();
    }
  },

  /** Node kind is deleted node. */
  DELETE((byte)5, DeletedNode.class) {
    @Override
    public INode deserialize(@Nonnull final ByteArrayDataInput pSource) {
      final NodeDelegate delegate = deserializeNodeDelegate(pSource);
      return new DeletedNode(delegate);
    }

    @Override
    public void serialize(@Nonnull final ByteArrayDataOutput pSink,
      @Nonnull final INode pToSerialize) {
      DeletedNode node = (DeletedNode)pToSerialize;
      serializeDelegate(node.getNodeDelegate(), pSink);
    }
  },

  /** NullNode to support the Null Object pattern. */
  NULL((byte)6, NullNode.class) {
    @Override
    public INode deserialize(@Nonnull final ByteArrayDataInput pSource) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serialize(@Nonnull final ByteArrayDataOutput pSink,
      @Nonnull final INode pToSerialize) {
      throw new UnsupportedOperationException();
    }
  },

  /** AtomicKind. */
  ATOMIC((byte)15, AtomicValue.class) {
    @Override
    public INode deserialize(@Nonnull final ByteArrayDataInput pSource) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serialize(@Nonnull final ByteArrayDataOutput pSink,
      @Nonnull final INode pToSerialize) {
      throw new UnsupportedOperationException();
    }
  },

  /** Node kind is path node. */
  PATH((byte)16, PathNode.class) {
    @Override
    public INode deserialize(@Nonnull final ByteArrayDataInput pSource) {
      // Node delegate.
      final NodeDelegate nodeDel = deserializeNodeDelegate(pSource);

      // Struct delegate.
      final StructNodeDelegate structDel =
        deserializeStructDel(nodeDel, pSource);

      // Name delegate.
      final NameNodeDelegate nameDel =
        deserializeNameDelegate(nodeDel, pSource);

      return new PathNode(nodeDel, structDel, nameDel, EKind.getKind(pSource
        .readByte()), pSource.readInt(), pSource.readInt());
    }

    @Override
    public void serialize(@Nonnull final ByteArrayDataOutput pSink,
      @Nonnull final INode pToSerialize) {
      final PathNode node = (PathNode)pToSerialize;
      serializeDelegate(node.getNodeDelegate(), pSink);
      serializeStrucDelegate(node.getStructNodeDelegate(), pSink);
      serializeNameDelegate(node.getNameNodeDelegate(), pSink);
      pSink.writeByte(node.getPathKind().getId());
      pSink.writeInt(node.getReferences());
      pSink.writeInt(node.getLevel());
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
  private EKind(final byte pId, @Nonnull final Class<? extends INode> pClass) {
    mId = pId;
    mClass = pClass;
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
   * Deserialize node delegate.
   * 
   * @param pUsePCR
   *          determines if PCR is saved (for attributes, namespaces and elements) or not
   * @param pSource
   *          source to read from
   * @return {@link NodeDelegate} instance
   */
  private static final NodeDelegate deserializeNodeDelegate(
    @Nonnull final ByteArrayDataInput pSource) {
    final long nodeKey = pSource.readLong();
    final long parentKey = nodeKey - pSource.readLong();
    final long hash = pSource.readLong();
    return new NodeDelegate(nodeKey, parentKey, hash);
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
    @Nonnull final ByteArrayDataOutput pSink) {
    pSink.writeLong(pDel.getNodeKey());
    pSink.writeLong(pDel.getNodeKey() - pDel.getParentKey());
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
    @Nonnull final StructNodeDelegate pDel, @Nonnull final ByteArrayDataOutput pSink) {
    pSink.writeLong(pDel.getNodeKey() - pDel.getRightSiblingKey());
    pSink.writeLong(pDel.getNodeKey() - pDel.getLeftSiblingKey());
    pSink.writeLong(pDel.getNodeKey() - pDel.getFirstChildKey());
    pSink.writeLong(pDel.getChildCount());
    pSink.writeLong(pDel.getDescendantCount() - pDel.getChildCount());
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
  private static final StructNodeDelegate deserializeStructDel(
    @Nonnull final NodeDelegate pDel, @Nonnull final ByteArrayDataInput pSource) {
    final long currKey = pDel.getNodeKey();
    final long rightSibl = currKey - pSource.readLong();
    final long leftSibl = currKey - pSource.readLong();
    final long firstChild = currKey - pSource.readLong();
    final long childCount = pSource.readLong();
    final long descendantCount = pSource.readLong() + childCount;
    return new StructNodeDelegate(pDel, firstChild, rightSibl, leftSibl,
      childCount, descendantCount);
  }

  /**
   * Deserialize name node delegate.
   * 
   * @param pNodeDel
   *          {@link NodeDelegate} instance
   * @param pSource
   *          source to read from
   * @return {@link NameNodeDelegate} instance
   */
  private static final NameNodeDelegate deserializeNameDelegate(
    @Nonnull final NodeDelegate pNodeDel, @Nonnull final ByteArrayDataInput pSource) {
    int nameKey = pSource.readInt();
    final int uriKey = pSource.readInt();
    nameKey += uriKey;
    return new NameNodeDelegate(pNodeDel, nameKey, uriKey, pSource.readLong());
  }

  /**
   * Serializing the {@link NameNodeDelegate} instance.
   * 
   * @param pDel
   *          {@link NameNodeDelegate} instance
   * @param pSink
   *          to serialize to
   */
  private static final void serializeNameDelegate(
    @Nonnull final NameNodeDelegate pDel, @Nonnull final ByteArrayDataOutput pSink) {
    pSink.writeInt(pDel.getNameKey() - pDel.getURIKey());
    pSink.writeInt(pDel.getURIKey());
    pSink.writeLong(pDel.getPathNodeKey());
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
    @Nonnull final ValNodeDelegate pDel, @Nonnull final ByteArrayDataOutput pSink) {
    final boolean isCompressed = pDel.isCompressed();
    pSink.writeByte(isCompressed ? (byte)1 : (byte)0);
    final byte[] value =
      isCompressed ? pDel.getCompressed() : pDel.getRawValue();
    pSink.writeInt(value.length);
    pSink.write(value);
  }
}
