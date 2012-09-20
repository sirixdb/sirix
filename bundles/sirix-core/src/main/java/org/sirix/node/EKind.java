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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.sirix.index.path.PathNode;
import org.sirix.index.value.AVLNode;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.delegates.ValNodeDelegate;
import org.sirix.node.interfaces.IKind;
import org.sirix.node.interfaces.INodeBase;
import org.sirix.page.NodePage;
import org.sirix.service.xml.xpath.AtomicValue;
import org.sirix.settings.EFixed;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;

/**
 * Enumeration for different nodes. All nodes are determined by a unique id.
 * 
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public enum EKind implements IKind {

	/** Node kind is element. */
	ELEMENT((byte) 1, ElementNode.class) {
		@Override
		public INodeBase deserialize(final @Nonnull ByteArrayDataInput pSource) {
			// Node delegate.
			final NodeDelegate nodeDel = deserializeNodeDelegate(pSource);

			// Struct delegate.
			final StructNodeDelegate structDel = deserializeStructDel(nodeDel,
					pSource);

			// Name delegate.
			final NameNodeDelegate nameDel = deserializeNameDelegate(nodeDel, pSource);

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
		public void serialize(final @Nonnull ByteArrayDataOutput pSink,
				final @Nonnull INodeBase pToSerialize) {
			final ElementNode node = (ElementNode) pToSerialize;
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
	ATTRIBUTE((byte) 2, AttributeNode.class) {
		@Override
		public INodeBase deserialize(final @Nonnull ByteArrayDataInput pSource) {
			// Node delegate.
			final NodeDelegate nodeDel = deserializeNodeDelegate(pSource);

			// Name delegate.
			final NameNodeDelegate nameDel = deserializeNameDelegate(nodeDel, pSource);

			// Val delegate.
			final boolean isCompressed = pSource.readByte() == (byte) 1 ? true
					: false;
			final byte[] vals = new byte[pSource.readInt()];
			pSource.readFully(vals, 0, vals.length);
			final ValNodeDelegate valDel = new ValNodeDelegate(nodeDel, vals,
					isCompressed);

			// Returning an instance.
			return new AttributeNode(nodeDel, nameDel, valDel);
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput pSink,
				final @Nonnull INodeBase pToSerialize) {
			AttributeNode node = (AttributeNode) pToSerialize;
			serializeDelegate(node.getNodeDelegate(), pSink);
			serializeNameDelegate(node.getNameNodeDelegate(), pSink);
			serializeValDelegate(node.getValNodeDelegate(), pSink);
		}
	},

	/** Node kind is namespace. */
	NAMESPACE((byte) 13, NamespaceNode.class) {
		@Override
		public INodeBase deserialize(final @Nonnull ByteArrayDataInput pSource) {
			// Node delegate.
			final NodeDelegate nodeDel = deserializeNodeDelegate(pSource);

			// Name delegate.
			final NameNodeDelegate nameDel = deserializeNameDelegate(nodeDel, pSource);

			return new NamespaceNode(nodeDel, nameDel);
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput pSink,
				final @Nonnull INodeBase pToSerialize) {
			NamespaceNode node = (NamespaceNode) pToSerialize;
			serializeDelegate(node.getNodeDelegate(), pSink);
			serializeNameDelegate(node.getNameNodeDelegate(), pSink);
		}
	},

	/** Node kind is text. */
	TEXT((byte) 3, TextNode.class) {
		@Override
		public INodeBase deserialize(final @Nonnull ByteArrayDataInput pSource) {
			// Node delegate.
			final NodeDelegate nodeDel = deserializeNodeDelegate(pSource);

			// Val delegate.
			final boolean isCompressed = pSource.readByte() == (byte) 1 ? true
					: false;
			final byte[] vals = new byte[pSource.readInt()];
			pSource.readFully(vals, 0, vals.length);
			final ValNodeDelegate valDel = new ValNodeDelegate(nodeDel, vals,
					isCompressed);

			// Struct delegate.
			final long nodeKey = nodeDel.getNodeKey();
			final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
					EFixed.NULL_NODE_KEY.getStandardProperty(), nodeKey
							- getLong(pSource), nodeKey - getLong(pSource), 0L, 0L);

			// Returning an instance.
			return new TextNode(nodeDel, valDel, structDel);
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput pSink,
				final @Nonnull INodeBase pToSerialize) {
			TextNode node = (TextNode) pToSerialize;
			serializeDelegate(node.getNodeDelegate(), pSink);
			serializeValDelegate(node.getValNodeDelegate(), pSink);
			final StructNodeDelegate del = node.getStructNodeDelegate();
			final long nodeKey = node.getNodeKey();
			putLong(pSink, nodeKey - del.getRightSiblingKey());
			putLong(pSink, nodeKey - del.getLeftSiblingKey());
		}
	},

	/** Node kind is processing instruction. */
	PROCESSING((byte) 7, null) {
		@Override
		public INodeBase deserialize(final @Nonnull ByteArrayDataInput pSource) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput pSink,
				final @Nonnull INodeBase pToSerialize) {
			throw new UnsupportedOperationException();
		}
	},

	/** Node kind is comment. */
	COMMENT((byte) 8, null) {
		@Override
		public INodeBase deserialize(final @Nonnull ByteArrayDataInput pSource) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput pSink,
				final @Nonnull INodeBase pToSerialize) {
			throw new UnsupportedOperationException();
		}
	},

	/** Node kind is document root. */
	// Virtualize document root node?
	DOCUMENT_ROOT((byte) 9, DocumentRootNode.class) {
		@Override
		public INodeBase deserialize(final @Nonnull ByteArrayDataInput pSource) {
			final NodeDelegate nodeDel = new NodeDelegate(
					EFixed.DOCUMENT_NODE_KEY.getStandardProperty(),
					EFixed.NULL_NODE_KEY.getStandardProperty(), pSource.readLong(),
					getLong(pSource));
			final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
					getLong(pSource), EFixed.NULL_NODE_KEY.getStandardProperty(),
					EFixed.NULL_NODE_KEY.getStandardProperty(),
					pSource.readByte() == ((byte) 0) ? 0 : 1, pSource.readLong());
			return new DocumentRootNode(nodeDel, structDel);
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput pSink,
				final @Nonnull INodeBase pToSerialize) {
			DocumentRootNode node = (DocumentRootNode) pToSerialize;
			pSink.writeLong(node.getHash());
			putLong(pSink, node.getRevision());
			putLong(pSink, node.getFirstChildKey());
			pSink.writeByte(node.hasFirstChild() ? (byte) 1 : (byte) 0);
			pSink.writeLong(node.getDescendantCount());
		}
	},

	/** Whitespace text. */
	WHITESPACE((byte) 4, null) {
		@Override
		public INodeBase deserialize(final @Nonnull ByteArrayDataInput pSource) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput pSink,
				final @Nonnull INodeBase pToSerialize) {
			throw new UnsupportedOperationException();
		}
	},

	/** Node kind is deleted node. */
	DELETE((byte) 5, DeletedNode.class) {
		@Override
		public INodeBase deserialize(final @Nonnull ByteArrayDataInput pSource) {
			final NodeDelegate delegate = new NodeDelegate(getLong(pSource), 0, 0, 0);
			return new DeletedNode(delegate);
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput pSink,
				final @Nonnull INodeBase pToSerialize) {
			DeletedNode node = (DeletedNode) pToSerialize;
			putLong(pSink, node.getNodeKey());
		}
	},

	/** NullNode to support the Null Object pattern. */
	NULL((byte) 6, NullNode.class) {
		@Override
		public INodeBase deserialize(final @Nonnull ByteArrayDataInput pSource) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput pSink,
				final @Nonnull INodeBase pToSerialize) {
			throw new UnsupportedOperationException();
		}
	},

	/** Dumb node for testing. */
	DUMB((byte) 20, DumbNode.class) {
		@Override
		public INodeBase deserialize(final @Nonnull ByteArrayDataInput pSource) {
			final long nodeKey = getLong(pSource);
			return new DumbNode(nodeKey);
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput pSink,
				final @Nonnull INodeBase pToSerialize) {
			putLong(pSink, pToSerialize.getNodeKey());
		}
	},

	/** AtomicKind. */
	ATOMIC((byte) 15, AtomicValue.class) {
		@Override
		public INodeBase deserialize(final @Nonnull ByteArrayDataInput pSource) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput pSink,
				final @Nonnull INodeBase pToSerialize) {
			throw new UnsupportedOperationException();
		}
	},

	/** Node kind is path node. */
	PATH((byte) 16, PathNode.class) {
		@Override
		public INodeBase deserialize(final @Nonnull ByteArrayDataInput pSource) {
			// Node delegate.
			final NodeDelegate nodeDel = deserializeNodeDelegate(pSource);

			// Struct delegate.
			final StructNodeDelegate structDel = deserializeStructDel(nodeDel,
					pSource);

			// Name delegate.
			final NameNodeDelegate nameDel = deserializeNameDelegate(nodeDel, pSource);

			return new PathNode(nodeDel, structDel, nameDel, EKind.getKind(pSource
					.readByte()), pSource.readInt(), pSource.readInt());
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput pSink,
				final @Nonnull INodeBase pToSerialize) {
			final PathNode node = (PathNode) pToSerialize;
			serializeDelegate(node.getNodeDelegate(), pSink);
			serializeStrucDelegate(node.getStructNodeDelegate(), pSink);
			serializeNameDelegate(node.getNameNodeDelegate(), pSink);
			pSink.writeByte(node.getPathKind().getId());
			pSink.writeInt(node.getReferences());
			pSink.writeInt(node.getLevel());
		};
	},

	/** Node kind is an AVL node. */
	AVL((byte) 17, AVLNode.class) {
		@Override
		public INodeBase deserialize(final @Nonnull ByteArrayDataInput pSource) {
			final int size = pSource.readInt();
			final byte[] value = new byte[size];
			pSource.readFully(value, 0, size);
			final long valueNodeKey = getLong(pSource);
			final Set<Long> nodeKeys = new HashSet<>(pSource.readInt());
			for (final long nodeKey : nodeKeys) {
				nodeKeys.add(nodeKey);
			}
			final long referencesNodeKey = getLong(pSource);
			// Node delegate.
			final NodeDelegate nodeDel = deserializeNodeDelegate(pSource);
			final long leftChild = getLong(pSource);
			final long rightChild = getLong(pSource);
			final long pathNodeKey = getLong(pSource);
			final boolean isChanged = pSource.readBoolean();
			final AVLNode<TextValue, TextReferences> node = new AVLNode<>(
					new TextValue(value, valueNodeKey, pathNodeKey), new TextReferences(
							nodeKeys, referencesNodeKey), nodeDel);
			node.setLeftChildKey(leftChild);
			node.setRightChildKey(rightChild);
			node.setChanged(isChanged);
			return node;
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput pSink,
				final @Nonnull INodeBase pToSerialize) {
			@SuppressWarnings("unchecked")
			final AVLNode<TextValue, TextReferences> node = (AVLNode<TextValue, TextReferences>) pToSerialize;
			final TextValue key = node.getKey();
			final byte[] textValue = key.getValue();
			pSink.writeInt(textValue.length);
			pSink.write(textValue);
			putLong(pSink, key.getNodeKey());
			final TextReferences value = node.getValue();
			final Set<Long> nodeKeys = value.getNodeKeys();
			pSink.writeInt(nodeKeys.size());
			for (final long nodeKey : nodeKeys) {
				pSink.writeLong(nodeKey);
			}
			putLong(pSink, value.getNodeKey());
			serializeDelegate(node.getNodeDelegate(), pSink);
			putLong(pSink, node.getLeftChildKey());
			putLong(pSink, node.getRightChildKey());
			putLong(pSink, key.getPathNodeKey());
			pSink.writeBoolean(node.isChanged());
		};
	},

	/** Node is a text value. */
	TEXT_VALUE((byte) 18, TextValue.class) {
		@Override
		public INodeBase deserialize(final @Nonnull ByteArrayDataInput pSource) {
			final long nodeKey = getLong(pSource);
			final long pathNodeKey = getLong(pSource);
			final byte[] value = new byte[pSource.readInt()];
			pSource.readFully(value);
			return new TextValue(value, nodeKey, pathNodeKey);
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput pSink,
				final @Nonnull INodeBase pToSerialize) {
			final TextValue node = (TextValue) pToSerialize;
			putLong(pSink, node.getNodeKey());
			putLong(pSink, node.getPathNodeKey());
			final byte[] value = node.getValue();
			pSink.writeInt(value.length);
			pSink.write(value);
		}
	},

	/** Node includes text node references. */
	TEXT_REFERENCES((byte) 19, TextReferences.class) {
		@Override
		public INodeBase deserialize(final @Nonnull ByteArrayDataInput pSource) {
			final long nodeKey = pSource.readLong();
			final int size = pSource.readInt();
			final Set<Long> nodeKeys = new HashSet<>(size);
			for (int i = 0; i < size; i++) {
				nodeKeys.add(pSource.readLong());
			}
			return new TextReferences(nodeKeys, nodeKey);
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput pSink,
				final @Nonnull INodeBase pToSerialize) {
			final TextReferences node = (TextReferences) pToSerialize;
			pSink.writeLong(node.getNodeKey());
			final Set<Long> nodeKeys = node.getNodeKeys();
			pSink.writeInt(nodeKeys.size());
			for (final long key : nodeKeys) {
				pSink.writeLong(key);
			}
		}
	},
	UNKOWN((byte) 21, null) {
		@Override
		public INodeBase deserialize(final @Nonnull ByteArrayDataInput pSource) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput pSink,
				final @Nonnull INodeBase pToSerialize) {
			throw new UnsupportedOperationException();
		}
	};

	/** Identifier. */
	private final byte mId;

	/** Class. */
	private final Class<? extends INodeBase> mClass;

	/** Mapping of keys -> nodes. */
	private static final Map<Byte, EKind> INSTANCEFORID = new HashMap<>();

	/** Mapping of class -> nodes. */
	private static final Map<Class<? extends INodeBase>, EKind> INSTANCEFORCLASS = new HashMap<>();

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
	private EKind(final byte pId, final @Nonnull Class<? extends INodeBase> pClass) {
		mId = pId;
		mClass = pClass;
	}

	@Override
	public byte getId() {
		return mId;
	}

	@Override
	public Class<? extends INodeBase> getNodeClass() {
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
	public static EKind getKind(final @Nonnull Class<? extends INodeBase> pClass) {
		return INSTANCEFORCLASS.get(pClass);
	}

	/**
	 * Deserialize node delegate.
	 * 
	 * @param pUsePCR
	 *          determines if PCR is saved (for attributes, namespaces and
	 *          elements) or not
	 * @param pSource
	 *          source to read from
	 * @return {@link NodeDelegate} instance
	 */
	private static final NodeDelegate deserializeNodeDelegate(
			final @Nonnull ByteArrayDataInput pSource) {
		final long nodeKey = getLong(pSource);
		final long parentKey = nodeKey - getLong(pSource);
		final long hash = pSource.readLong();
		final long revision = getLong(pSource);
		return new NodeDelegate(nodeKey, parentKey, hash, revision);
	}

	/**
	 * Serializing the {@link NodeDelegate} instance.
	 * 
	 * @param pDel
	 *          to be serialize
	 * @param pSink
	 *          to serialize to
	 */
	private static final void serializeDelegate(final @Nonnull NodeDelegate pDel,
			final @Nonnull ByteArrayDataOutput pSink) {
		putLong(pSink, pDel.getNodeKey());
		putLong(pSink, pDel.getNodeKey() - pDel.getParentKey());
		pSink.writeLong(pDel.getHash());
		putLong(pSink, pDel.getRevision());
	}

	/**
	 * Serializing the {@link StructNodeDelegate} instance.
	 * 
	 * @param pDel
	 *          to be serialize
	 * @param pSink
	 *          to serialize to
	 */
	private static final void serializeStrucDelegate(
			final @Nonnull StructNodeDelegate pDel,
			final @Nonnull ByteArrayDataOutput pSink) {
		putLong(pSink, pDel.getNodeKey() - pDel.getRightSiblingKey());
		putLong(pSink, pDel.getNodeKey() - pDel.getLeftSiblingKey());
		putLong(pSink, pDel.getNodeKey() - pDel.getFirstChildKey());
		putLong(pSink, pDel.getChildCount());
		putLong(pSink, pDel.getDescendantCount() - pDel.getChildCount());
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
			final @Nonnull NodeDelegate pDel,
			final @Nonnull ByteArrayDataInput pSource) {
		final long currKey = pDel.getNodeKey();
		final long rightSibl = currKey - getLong(pSource);
		final long leftSibl = currKey - getLong(pSource);
		final long firstChild = currKey - getLong(pSource);
		final long childCount = getLong(pSource);
		final long descendantCount = getLong(pSource) + childCount;
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
			final @Nonnull NodeDelegate pNodeDel,
			final @Nonnull ByteArrayDataInput pSource) {
		int nameKey = pSource.readInt();
		final int uriKey = pSource.readInt();
		nameKey += uriKey;
		return new NameNodeDelegate(pNodeDel, nameKey, uriKey, getLong(pSource));
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
			final @Nonnull NameNodeDelegate pDel,
			final @Nonnull ByteArrayDataOutput pSink) {
		pSink.writeInt(pDel.getNameKey() - pDel.getURIKey());
		pSink.writeInt(pDel.getURIKey());
		putLong(pSink, pDel.getPathNodeKey());
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
			final @Nonnull ValNodeDelegate pDel,
			final @Nonnull ByteArrayDataOutput pSink) {
		final boolean isCompressed = pDel.isCompressed();
		pSink.writeByte(isCompressed ? (byte) 1 : (byte) 0);
		final byte[] value = isCompressed ? pDel.getCompressed() : pDel
				.getRawValue();
		pSink.writeInt(value.length);
		pSink.write(value);
	}

	/**
	 * Store a compressed long value.
	 * 
	 * @param pOutput
	 *          {@link ByteArrayDataOutput} reference
	 * @param value
	 *          long value
	 */
	private static final void putLong(final @Nonnull ByteArrayDataOutput pOutput,
			long value) {
		while ((value & ~0x7F) != 0) {
			pOutput.write(((byte) ((value & 0x7f) | 0x80)));
			value >>>= 7;
		}
		pOutput.write((byte) value);
	}

	/**
	 * Get a compressed long value.
	 * 
	 * @param pInput
	 *          {@link ByteArrayDataInput} reference
	 * @return long value
	 */
	private static final long getLong(final @Nonnull ByteArrayDataInput pInput) {
		byte singleByte = pInput.readByte();
		long value = singleByte & 0x7F;
		for (int shift = 7; (singleByte & 0x80) != 0; shift += 7) {
			singleByte = pInput.readByte();
			value |= (singleByte & 0x7FL) << shift;
		}
		return value;
	}

	/**
	 * Simple DumbNode just for testing the {@link NodePage}s.
	 * 
	 * @author Sebastian Graf, University of Konstanz
	 * @author Johannes Lichtenberger
	 * 
	 */
	public static class DumbNode implements INodeBase {

		/** Node key. */
		private final long mNodeKey;

		/**
		 * Simple constructor.
		 * 
		 * @param pNodeKey
		 *          to be set
		 * @param pHash
		 *          to be set
		 */
		public DumbNode(final @Nonnull long pNodeKey) {
			mNodeKey = pNodeKey;
		}

		@Override
		public long getNodeKey() {
			return mNodeKey;
		}

		@Override
		public EKind getKind() {
			return EKind.NULL;
		}

		@Override
		public long getRevision() {
			return 0;
		}
	}
}
