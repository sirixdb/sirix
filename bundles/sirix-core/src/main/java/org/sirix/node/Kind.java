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

import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.api.PageReadTrx;
import org.sirix.index.path.PathNode;
import org.sirix.index.value.AVLNode;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.delegates.ValNodeDelegate;
import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.RecordPersistenter;
import org.sirix.page.UnorderedKeyValuePage;
import org.sirix.service.xml.xpath.AtomicValue;
import org.sirix.settings.Fixed;

import com.google.common.base.Optional;
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
public enum Kind implements RecordPersistenter {

	/** Node kind is element. */
	ELEMENT((byte) 1, ElementNode.class) {
		@Override
		public Record deserialize(final @Nonnull ByteArrayDataInput source,
				final @Nonnull PageReadTrx pageReadTrx) {
			// Node delegate.
			final NodeDelegate nodeDel = deserializeNodeDelegate(source, pageReadTrx);

			// Struct delegate.
			final StructNodeDelegate structDel = deserializeStructDel(nodeDel, source);

			// Name delegate.
			final NameNodeDelegate nameDel = deserializeNameDelegate(nodeDel, source);

			// Attributes.
			final int attrCount = source.readInt();
			final List<Long> attrKeys = new ArrayList<>(attrCount);
			final BiMap<Integer, Long> attrs = HashBiMap.<Integer, Long> create();
			for (int i = 0; i < attrCount; i++) {
				final long nodeKey = source.readLong();
				attrKeys.add(nodeKey);
				attrs.put(source.readInt(), nodeKey);
			}

			// Namespaces.
			final int nsCount = source.readInt();
			final List<Long> namespKeys = new ArrayList<>(nsCount);
			for (int i = 0; i < nsCount; i++) {
				namespKeys.add(source.readLong());
			}

			return new ElementNode(structDel, nameDel, attrKeys, attrs, namespKeys);
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput sink,
				final @Nonnull Record toSerialize,
				final @Nonnull PageReadTrx pageReadTrx) {
			final ElementNode node = (ElementNode) toSerialize;
			serializeDelegate(node.getNodeDelegate(), sink, pageReadTrx);
			serializeStrucDelegate(node.getStructNodeDelegate(), sink);
			serializeNameDelegate(node.getNameNodeDelegate(), sink);
			sink.writeInt(node.getAttributeCount());
			for (int i = 0, attCount = node.getAttributeCount(); i < attCount; i++) {
				final long key = node.getAttributeKey(i);
				sink.writeLong(key);
				sink.writeInt(node.getAttributeNameKey(key).get());
			}
			sink.writeInt(node.getNamespaceCount());
			for (int i = 0, nspCount = node.getNamespaceCount(); i < nspCount; i++) {
				sink.writeLong(node.getNamespaceKey(i));
			}
		}
	},

	/** Node kind is attribute. */
	ATTRIBUTE((byte) 2, AttributeNode.class) {
		@Override
		public Record deserialize(final @Nonnull ByteArrayDataInput source,
				final @Nonnull PageReadTrx pageReadTrx) {
			// Node delegate.
			final NodeDelegate nodeDel = deserializeNodeDelegate(source, pageReadTrx);

			// Name delegate.
			final NameNodeDelegate nameDel = deserializeNameDelegate(nodeDel, source);

			// Val delegate.
			final boolean isCompressed = source.readByte() == (byte) 1 ? true : false;
			final byte[] vals = new byte[source.readInt()];
			source.readFully(vals, 0, vals.length);
			final ValNodeDelegate valDel = new ValNodeDelegate(nodeDel, vals,
					isCompressed);

			// Returning an instance.
			return new AttributeNode(nodeDel, nameDel, valDel);
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput sink,
				final @Nonnull Record toSerialize,
				final @Nonnull PageReadTrx pageReadTrx) {
			final AttributeNode node = (AttributeNode) toSerialize;
			serializeDelegate(node.getNodeDelegate(), sink, pageReadTrx);
			serializeNameDelegate(node.getNameNodeDelegate(), sink);
			serializeValDelegate(node.getValNodeDelegate(), sink);
		}
	},

	/** Node kind is namespace. */
	NAMESPACE((byte) 13, NamespaceNode.class) {
		@Override
		public Record deserialize(final @Nonnull ByteArrayDataInput source,
				final @Nonnull PageReadTrx pageReadTrx) {
			// Node delegate.
			final NodeDelegate nodeDel = deserializeNodeDelegate(source, pageReadTrx);

			// Name delegate.
			final NameNodeDelegate nameDel = deserializeNameDelegate(nodeDel, source);

			return new NamespaceNode(nodeDel, nameDel);
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput sink,
				final @Nonnull Record toSerialize,
				final @Nonnull PageReadTrx pageReadTrx) {
			final NamespaceNode node = (NamespaceNode) toSerialize;
			serializeDelegate(node.getNodeDelegate(), sink, pageReadTrx);
			serializeNameDelegate(node.getNameNodeDelegate(), sink);
		}
	},

	/** Node kind is text. */
	TEXT((byte) 3, TextNode.class) {
		@Override
		public Record deserialize(final @Nonnull ByteArrayDataInput source,
				final @Nonnull PageReadTrx pageReadTrx) {
			// Node delegate.
			final NodeDelegate nodeDel = deserializeNodeDelegate(source, pageReadTrx);

			// Val delegate.
			final boolean isCompressed = source.readByte() == (byte) 1 ? true : false;
			final byte[] vals = new byte[source.readInt()];
			source.readFully(vals, 0, vals.length);
			final ValNodeDelegate valDel = new ValNodeDelegate(nodeDel, vals,
					isCompressed);

			// Struct delegate.
			final long nodeKey = nodeDel.getNodeKey();
			final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
					Fixed.NULL_NODE_KEY.getStandardProperty(), nodeKey - getLong(source),
					nodeKey - getLong(source), 0L, 0L);

			// Returning an instance.
			return new TextNode(valDel, structDel);
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput sink,
				final @Nonnull Record toSerialize,
				final @Nonnull PageReadTrx pageReadTrx) {
			final TextNode node = (TextNode) toSerialize;
			serializeDelegate(node.getNodeDelegate(), sink, pageReadTrx);
			serializeValDelegate(node.getValNodeDelegate(), sink);
			final StructNodeDelegate del = node.getStructNodeDelegate();
			final long nodeKey = node.getNodeKey();
			putLong(sink, nodeKey - del.getRightSiblingKey());
			putLong(sink, nodeKey - del.getLeftSiblingKey());
		}
	},

	/** Node kind is processing instruction. */
	PROCESSING_INSTRUCTION((byte) 7, PINode.class) {
		@Override
		public Record deserialize(final @Nonnull ByteArrayDataInput source,
				final @Nonnull PageReadTrx pageReadTrx) {
			// Node delegate.
			final NodeDelegate nodeDel = deserializeNodeDelegate(source, pageReadTrx);

			// Struct delegate.
			final StructNodeDelegate structDel = deserializeStructDel(nodeDel, source);

			// Name delegate.
			final NameNodeDelegate nameDel = deserializeNameDelegate(nodeDel, source);

			// Val delegate.
			final boolean isCompressed = source.readByte() == (byte) 1 ? true : false;
			final byte[] vals = new byte[source.readInt()];
			source.readFully(vals, 0, vals.length);
			final ValNodeDelegate valDel = new ValNodeDelegate(nodeDel, vals,
					isCompressed);

			// Returning an instance.
			return new PINode(structDel, nameDel, valDel);
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput pSink,
				final @Nonnull Record pToSerialize,
				final @Nonnull PageReadTrx pageReadTrx) {
			final PINode node = (PINode) pToSerialize;
			serializeDelegate(node.getNodeDelegate(), pSink, pageReadTrx);
			serializeStrucDelegate(node.getStructNodeDelegate(), pSink);
			serializeNameDelegate(node.getNameNodeDelegate(), pSink);
			serializeValDelegate(node.getValNodeDelegate(), pSink);
		}
	},

	/** Node kind is comment. */
	COMMENT((byte) 8, CommentNode.class) {
		@Override
		public Record deserialize(final @Nonnull ByteArrayDataInput source,
				final @Nonnull PageReadTrx pageReadTrx) {
			// Node delegate.
			final NodeDelegate nodeDel = deserializeNodeDelegate(source, pageReadTrx);

			// Val delegate.
			final boolean isCompressed = source.readByte() == (byte) 1 ? true : false;
			final byte[] vals = new byte[source.readInt()];
			source.readFully(vals, 0, vals.length);
			final ValNodeDelegate valDel = new ValNodeDelegate(nodeDel, vals,
					isCompressed);

			// Struct delegate.
			final long nodeKey = nodeDel.getNodeKey();
			final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
					Fixed.NULL_NODE_KEY.getStandardProperty(), nodeKey - getLong(source),
					nodeKey - getLong(source), 0L, 0L);

			// Returning an instance.
			return new CommentNode(valDel, structDel);
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput sink,
				final @Nonnull Record toSerialize,
				final @Nonnull PageReadTrx pageReadTrx) {
			final CommentNode node = (CommentNode) toSerialize;
			serializeDelegate(node.getNodeDelegate(), sink, pageReadTrx);
			serializeValDelegate(node.getValNodeDelegate(), sink);
			final StructNodeDelegate del = node.getStructNodeDelegate();
			final long nodeKey = node.getNodeKey();
			putLong(sink, nodeKey - del.getRightSiblingKey());
			putLong(sink, nodeKey - del.getLeftSiblingKey());
		}
	},

	/** Node kind is document root. */
	// Virtualize document root node?
	DOCUMENT((byte) 9, DocumentRootNode.class) {
		@Override
		public Record deserialize(final @Nonnull ByteArrayDataInput source,
				final @Nonnull PageReadTrx pageReadTrx) {
			final NodeDelegate nodeDel = new NodeDelegate(
					Fixed.DOCUMENT_NODE_KEY.getStandardProperty(),
					Fixed.NULL_NODE_KEY.getStandardProperty(), source.readLong(),
					getLong(source), Optional.of(SirixDeweyID.newRootID()));
			final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
					getLong(source), Fixed.NULL_NODE_KEY.getStandardProperty(),
					Fixed.NULL_NODE_KEY.getStandardProperty(),
					source.readByte() == ((byte) 0) ? 0 : 1, source.readLong());
			return new DocumentRootNode(nodeDel, structDel);
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput pSink,
				final @Nonnull Record pToSerialize,
				final @Nonnull PageReadTrx pageReadTrx) {
			final DocumentRootNode node = (DocumentRootNode) pToSerialize;
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
		public Record deserialize(final @Nonnull ByteArrayDataInput source,
				final @Nonnull PageReadTrx pageReadTrx) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput sink,
				final @Nonnull Record pToSerialize,
				final @Nonnull PageReadTrx pageReadTrx) {
			throw new UnsupportedOperationException();
		}
	},

	/** Node kind is deleted node. */
	DELETE((byte) 5, DeletedNode.class) {
		@Override
		public Record deserialize(final @Nonnull ByteArrayDataInput source,
				final @Nonnull PageReadTrx pageReadTrx) {
			final NodeDelegate delegate = new NodeDelegate(getLong(source), 0, 0, 0,
					Optional.<SirixDeweyID> absent());
			return new DeletedNode(delegate);
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput sink,
				final @Nonnull Record pToSerialize,
				final @Nonnull PageReadTrx pageReadTrx) {
			DeletedNode node = (DeletedNode) pToSerialize;
			putLong(sink, node.getNodeKey());
		}
	},

	/** NullNode to support the Null Object pattern. */
	NULL((byte) 6, NullNode.class) {
		@Override
		public Record deserialize(final @Nonnull ByteArrayDataInput source,
				final @Nonnull PageReadTrx pageReadTrx) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput ink,
				final @Nonnull Record toSerialize,
				final @Nonnull PageReadTrx pageReadTrx) {
			throw new UnsupportedOperationException();
		}
	},

	/** Dumb node for testing. */
	DUMB((byte) 20, DumbNode.class) {
		@Override
		public Record deserialize(final @Nonnull ByteArrayDataInput source,
				final @Nonnull PageReadTrx pageReadTrx) {
			final long nodeKey = getLong(source);
			return new DumbNode(nodeKey);
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput sink,
				final @Nonnull Record toSerialize,
				final @Nonnull PageReadTrx pageReadTrx) {
			putLong(sink, toSerialize.getNodeKey());
		}
	},

	/** AtomicKind. */
	ATOMIC((byte) 15, AtomicValue.class) {
		@Override
		public Record deserialize(final @Nonnull ByteArrayDataInput source,
				final @Nonnull PageReadTrx pageReadTrx) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput sink,
				final @Nonnull Record pToSerialize,
				final @Nonnull PageReadTrx pageReadTrx) {
			throw new UnsupportedOperationException();
		}
	},

	/** Node kind is path node. */
	PATH((byte) 16, PathNode.class) {
		@Override
		public Record deserialize(final @Nonnull ByteArrayDataInput source,
				final @Nonnull PageReadTrx pageReadTrx) {
			// Node delegate.
			final NodeDelegate nodeDel = deserializeNodeDelegateWithoutIDs(source,
					pageReadTrx);

			// Struct delegate.
			final StructNodeDelegate structDel = deserializeStructDel(nodeDel, source);

			// Name delegate.
			final NameNodeDelegate nameDel = deserializeNameDelegate(nodeDel, source);

			return new PathNode(nodeDel, structDel, nameDel, Kind.getKind(source
					.readByte()), source.readInt(), source.readInt());
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput sink,
				final @Nonnull Record toSerialize,
				final @Nonnull PageReadTrx pageReadTrx) {
			final PathNode node = (PathNode) toSerialize;
			serializeDelegate(node.getNodeDelegate(), sink, pageReadTrx);
			serializeStrucDelegate(node.getStructNodeDelegate(), sink);
			serializeNameDelegate(node.getNameNodeDelegate(), sink);
			sink.writeByte(node.getPathKind().getId());
			sink.writeInt(node.getReferences());
			sink.writeInt(node.getLevel());
		};
	},

	/** Node kind is an AVL node. */
	AVL((byte) 17, AVLNode.class) {
		@Override
		public Record deserialize(final @Nonnull ByteArrayDataInput source,
				final @Nonnull PageReadTrx pageReadTrx) {
			final int size = source.readInt();
			final byte[] value = new byte[size];
			source.readFully(value, 0, size);
			final long valueNodeKey = getLong(source);
			final Set<Long> nodeKeys = new HashSet<>(source.readInt());
			for (final long nodeKey : nodeKeys) {
				nodeKeys.add(nodeKey);
			}
			final long referencesNodeKey = getLong(source);
			// Node delegate.
			final NodeDelegate nodeDel = deserializeNodeDelegateWithoutIDs(source,
					pageReadTrx);
			final long leftChild = getLong(source);
			final long rightChild = getLong(source);
			final long pathNodeKey = getLong(source);
			final boolean isChanged = source.readBoolean();
			final AVLNode<TextValue, TextReferences> node = new AVLNode<>(
					new TextValue(value, valueNodeKey, pathNodeKey), new TextReferences(
							nodeKeys, referencesNodeKey), nodeDel);
			node.setLeftChildKey(leftChild);
			node.setRightChildKey(rightChild);
			node.setChanged(isChanged);
			return node;
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput sink,
				final @Nonnull Record toSerialize,
				final @Nonnull PageReadTrx pageReadTrx) {
			@SuppressWarnings("unchecked")
			final AVLNode<TextValue, TextReferences> node = (AVLNode<TextValue, TextReferences>) toSerialize;
			final TextValue key = node.getKey();
			final byte[] textValue = key.getValue();
			sink.writeInt(textValue.length);
			sink.write(textValue);
			putLong(sink, key.getNodeKey());
			final TextReferences value = node.getValue();
			final Set<Long> nodeKeys = value.getNodeKeys();
			sink.writeInt(nodeKeys.size());
			for (final long nodeKey : nodeKeys) {
				sink.writeLong(nodeKey);
			}
			putLong(sink, value.getNodeKey());
			serializeDelegate(node.getNodeDelegate(), sink, pageReadTrx);
			putLong(sink, node.getLeftChildKey());
			putLong(sink, node.getRightChildKey());
			putLong(sink, key.getPathNodeKey());
			sink.writeBoolean(node.isChanged());
		};
	},

	/** Node is a text value. */
	TEXT_VALUE((byte) 18, TextValue.class) {
		@Override
		public Record deserialize(final @Nonnull ByteArrayDataInput source,
				final @Nonnull PageReadTrx pageReadTrx) {
			final long nodeKey = getLong(source);
			final long pathNodeKey = getLong(source);
			final byte[] value = new byte[source.readInt()];
			source.readFully(value);
			return new TextValue(value, nodeKey, pathNodeKey);
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput sink,
				final @Nonnull Record toSerialize,
				final @Nonnull PageReadTrx pageReadTrx) {
			final TextValue node = (TextValue) toSerialize;
			putLong(sink, node.getNodeKey());
			putLong(sink, node.getPathNodeKey());
			final byte[] value = node.getValue();
			sink.writeInt(value.length);
			sink.write(value);
		}
	},

	/** Node includes text node references. */
	TEXT_REFERENCES((byte) 19, TextReferences.class) {
		@Override
		public Record deserialize(final @Nonnull ByteArrayDataInput source,
				final @Nonnull PageReadTrx pageReadTrx) {
			final long nodeKey = source.readLong();
			final int size = source.readInt();
			final Set<Long> nodeKeys = new HashSet<>(size);
			for (int i = 0; i < size; i++) {
				nodeKeys.add(source.readLong());
			}
			return new TextReferences(nodeKeys, nodeKey);
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput sink,
				final @Nonnull Record toSerialize,
				final @Nonnull PageReadTrx pageReadTrx) {
			final TextReferences node = (TextReferences) toSerialize;
			sink.writeLong(node.getNodeKey());
			final Set<Long> nodeKeys = node.getNodeKeys();
			sink.writeInt(nodeKeys.size());
			for (final long key : nodeKeys) {
				sink.writeLong(key);
			}
		}
	},

	/** Node type not known. */
	UNKNOWN((byte) 21, null) {
		@Override
		public Record deserialize(final @Nonnull ByteArrayDataInput source,
				final @Nonnull PageReadTrx pageReadTrx) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput sink,
				final @Nonnull Record toSerialize,
				final @Nonnull PageReadTrx pageReadTrx) {
			throw new UnsupportedOperationException();
		}
	};

	/** Identifier. */
	private final byte mId;

	/** Class. */
	private final Class<? extends Record> mClass;

	/** Mapping of keys -> nodes. */
	private static final Map<Byte, Kind> INSTANCEFORID = new HashMap<>();

	/** Mapping of class -> nodes. */
	private static final Map<Class<? extends Record>, Kind> INSTANCEFORCLASS = new HashMap<>();

	static {
		for (final Kind node : values()) {
			INSTANCEFORID.put(node.mId, node);
			INSTANCEFORCLASS.put(node.mClass, node);
		}
	}

	/**
	 * Constructor.
	 * 
	 * @param pId
	 *          unique identifier
	 * @param clazz
	 *          class
	 */
	private Kind(final byte pId, final @Nonnull Class<? extends Record> clazz) {
		mId = pId;
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
	public Class<? extends Record> getNodeClass() {
		return mClass;
	}

	/**
	 * Public method to get the related node based on the identifier.
	 * 
	 * @param id
	 *          the identifier for the node
	 * @return the related node
	 */
	public static Kind getKind(final byte id) {
		return INSTANCEFORID.get(id);
	}

	/**
	 * Public method to get the related node based on the class.
	 * 
	 * @param clazz
	 *          the class for the node
	 * @return the related node
	 */
	public static Kind getKind(final @Nonnull Class<? extends Record> clazz) {
		return INSTANCEFORCLASS.get(clazz);
	}

	/**
	 * Deserialize node delegate without stored dewey IDs.
	 * 
	 * @param source
	 *          source to read from
	 * @param resourceConfig
	 *          resource configuration
	 * @return {@link NodeDelegate} instance
	 */
	private static final NodeDelegate deserializeNodeDelegateWithoutIDs(
			final @Nonnull ByteArrayDataInput source,
			final @Nonnull PageReadTrx pageReadTrx) {
		final long nodeKey = getLong(source);
		final long parentKey = nodeKey - getLong(source);
		final long hash = source.readLong();
		final long revision = getLong(source);
		return new NodeDelegate(nodeKey, parentKey, hash, revision,
				Optional.<SirixDeweyID> absent());
	}

	/**
	 * Deserialize node delegate.
	 * 
	 * @param source
	 *          source to read from
	 * @param resourceConfig
	 *          resource configuration
	 * @return {@link NodeDelegate} instance
	 */
	private static final NodeDelegate deserializeNodeDelegate(
			final @Nonnull ByteArrayDataInput source,
			final @Nonnull PageReadTrx pageReadTrx) {
		final long nodeKey = getLong(source);
		final long parentKey = nodeKey - getLong(source);
		final long hash = source.readLong();
		final long revision = getLong(source);
		Optional<SirixDeweyID> id = Optional.<SirixDeweyID> absent();
		if (pageReadTrx.getSession().getResourceConfig().mDeweyIDsStored) {
			final byte deweyIDLength = source.readByte();
			final byte[] deweyID = new byte[deweyIDLength];
			source.readFully(deweyID, 0, deweyIDLength);
			id = Optional.of(new SirixDeweyID(deweyID));
		}
		return new NodeDelegate(nodeKey, parentKey, hash, revision, id);
	}

	/**
	 * Serializing the {@link NodeDelegate} instance.
	 * 
	 * @param nodeDel
	 *          to be serialize
	 * @param sink
	 *          to serialize to
	 * @param {@link ResourceConfiguration} instance
	 */
	private static final void serializeDelegate(
			final @Nonnull NodeDelegate nodeDel,
			final @Nonnull ByteArrayDataOutput sink,
			final @Nonnull PageReadTrx pageReadTrx) {
		putLong(sink, nodeDel.getNodeKey());
		putLong(sink, nodeDel.getNodeKey() - nodeDel.getParentKey());
		sink.writeLong(nodeDel.getHash());
		putLong(sink, nodeDel.getRevision());
		if (pageReadTrx.getSession().getResourceConfig().mDeweyIDsStored) {
			final Optional<SirixDeweyID> id = nodeDel.getDeweyID();
			if (id.isPresent()) {
				final byte[] deweyID = nodeDel.getDeweyID().get().toBytes();
				sink.writeByte(deweyID.length);
				sink.write(deweyID);
			}
		}
	}

	/**
	 * Serializing the {@link StructNodeDelegate} instance.
	 * 
	 * @param nodeDel
	 *          to be serialize
	 * @param sink
	 *          to serialize to
	 */
	private static final void serializeStrucDelegate(
			final @Nonnull StructNodeDelegate nodeDel,
			final @Nonnull ByteArrayDataOutput sink) {
		putLong(sink, nodeDel.getNodeKey() - nodeDel.getRightSiblingKey());
		putLong(sink, nodeDel.getNodeKey() - nodeDel.getLeftSiblingKey());
		putLong(sink, nodeDel.getNodeKey() - nodeDel.getFirstChildKey());
		putLong(sink, nodeDel.getChildCount());
		putLong(sink, nodeDel.getDescendantCount() - nodeDel.getChildCount());
	}

	/**
	 * Deserialize struct delegate.
	 * 
	 * @param nodeDel
	 *          node delegate
	 * @param source
	 *          input source
	 * @return {@link StructNodeDelegate} instance
	 */
	private static final StructNodeDelegate deserializeStructDel(
			final @Nonnull NodeDelegate nodeDel,
			final @Nonnull ByteArrayDataInput source) {
		final long currKey = nodeDel.getNodeKey();
		final long rightSibl = currKey - getLong(source);
		final long leftSibl = currKey - getLong(source);
		final long firstChild = currKey - getLong(source);
		final long childCount = getLong(source);
		final long descendantCount = getLong(source) + childCount;
		return new StructNodeDelegate(nodeDel, firstChild, rightSibl, leftSibl,
				childCount, descendantCount);
	}

	/**
	 * Deserialize name node delegate.
	 * 
	 * @param nodeDel
	 *          {@link NodeDelegate} instance
	 * @param source
	 *          source to read from
	 * @return {@link NameNodeDelegate} instance
	 */
	private static final NameNodeDelegate deserializeNameDelegate(
			final @Nonnull NodeDelegate nodeDel,
			final @Nonnull ByteArrayDataInput source) {
		int nameKey = source.readInt();
		final int uriKey = source.readInt();
		nameKey += uriKey;
		return new NameNodeDelegate(nodeDel, nameKey, uriKey, getLong(source));
	}

	/**
	 * Serializing the {@link NameNodeDelegate} instance.
	 * 
	 * @param nameDel
	 *          {@link NameNodeDelegate} instance
	 * @param sink
	 *          to serialize to
	 */
	private static final void serializeNameDelegate(
			final @Nonnull NameNodeDelegate nameDel,
			final @Nonnull ByteArrayDataOutput sink) {
		sink.writeInt(nameDel.getNameKey() - nameDel.getURIKey());
		sink.writeInt(nameDel.getURIKey());
		putLong(sink, nameDel.getPathNodeKey());
	}

	/**
	 * Serializing the {@link ValNodeDelegate} instance.
	 * 
	 * @param valueDel
	 *          to be serialized
	 * @param sink
	 *          to serialize to
	 */
	private static final void serializeValDelegate(
			final @Nonnull ValNodeDelegate valueDel,
			final @Nonnull ByteArrayDataOutput sink) {
		final boolean isCompressed = valueDel.isCompressed();
		sink.writeByte(isCompressed ? (byte) 1 : (byte) 0);
		final byte[] value = isCompressed ? valueDel.getCompressed() : valueDel
				.getRawValue();
		sink.writeInt(value.length);
		sink.write(value);
	}

	/**
	 * Store a "compressed" variable-length long value.
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
	 * Get a "compressed" variable-length long value.
	 * 
	 * @param input
	 *          {@link ByteArrayDataInput} reference
	 * @return long value
	 */
	private static final long getLong(final @Nonnull ByteArrayDataInput input) {
		byte singleByte = input.readByte();
		long value = singleByte & 0x7F;
		for (int shift = 7; (singleByte & 0x80) != 0; shift += 7) {
			singleByte = input.readByte();
			value |= (singleByte & 0x7FL) << shift;
		}
		return value;
	}

	/**
	 * Simple DumbNode just for testing the {@link UnorderedKeyValuePage}s.
	 * 
	 * @author Sebastian Graf, University of Konstanz
	 * @author Johannes Lichtenberger
	 * 
	 */
	public static class DumbNode implements Record {

		/** Node key. */
		private final long mNodeKey;

		/**
		 * Simple constructor.
		 * 
		 * @param nodeKey
		 *          to be set
		 * @param pHash
		 *          to be set
		 */
		public DumbNode(final @Nonnull long nodeKey) {
			mNodeKey = nodeKey;
		}

		@Override
		public long getNodeKey() {
			return mNodeKey;
		}

		@Override
		public Kind getKind() {
			return Kind.NULL;
		}

		@Override
		public long getRevision() {
			return 0;
		}
	}
}
