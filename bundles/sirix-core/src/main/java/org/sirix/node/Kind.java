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

import static org.sirix.node.Utils.getVarLong;
import static org.sirix.node.Utils.putVarLong;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.api.PageReadTrx;
import org.sirix.index.avltree.AVLNode;
import org.sirix.index.avltree.NodeReferences;
import org.sirix.index.avltree.Value;
import org.sirix.index.avltree.ValueKind;
import org.sirix.index.path.PathNode;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.delegates.ValNodeDelegate;
import org.sirix.node.interfaces.Node;
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
				final @Nonnegative long recordID, final @Nonnull PageReadTrx pageReadTrx) {
			// Node delegate.
			final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID,
					pageReadTrx);

			// Struct delegate.
			final StructNodeDelegate structDel = deserializeStructDel(nodeDel, source);

			// Name delegate.
			final NameNodeDelegate nameDel = deserializeNameDelegate(nodeDel, source);

			// Attributes.
			final int attrCount = source.readInt();
			final List<Long> attrKeys = new ArrayList<>(attrCount);
			final BiMap<Long, Long> attrs = HashBiMap.<Long, Long> create();
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

			return new ElementNode(structDel, nameDel, attrKeys, attrs, namespKeys);
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput sink,
				final @Nonnull Record record, final @Nullable Record nextRecord,
				final @Nonnull PageReadTrx pageReadTrx) {
			final ElementNode node = (ElementNode) record;
			final Node nextNode = (Node) nextRecord;
			serializeDelegate(node.getNodeDelegate(), nextNode, sink, pageReadTrx);
			serializeStrucDelegate(node.getStructNodeDelegate(), sink);
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

	/** Node kind is attribute. */
	ATTRIBUTE((byte) 2, AttributeNode.class) {
		@Override
		public Record deserialize(final @Nonnull ByteArrayDataInput source,
				final @Nonnegative long recordID, final @Nonnull PageReadTrx pageReadTrx) {
			// Node delegate.
			final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID,
					pageReadTrx);

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
				final @Nonnull Record record, final @Nullable Record nextRecord,
				final @Nonnull PageReadTrx pageReadTrx) {
			final AttributeNode node = (AttributeNode) record;
			final Node nextNode = (Node) nextRecord;
			serializeDelegate(node.getNodeDelegate(), nextNode, sink, pageReadTrx);
			serializeNameDelegate(node.getNameNodeDelegate(), sink);
			serializeValDelegate(node.getValNodeDelegate(), sink);
		}
	},

	/** Node kind is namespace. */
	NAMESPACE((byte) 13, NamespaceNode.class) {
		@Override
		public Record deserialize(final @Nonnull ByteArrayDataInput source,
				final @Nonnegative long recordID, final @Nonnull PageReadTrx pageReadTrx) {
			// Node delegate.
			final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID,
					pageReadTrx);

			// Name delegate.
			final NameNodeDelegate nameDel = deserializeNameDelegate(nodeDel, source);

			return new NamespaceNode(nodeDel, nameDel);
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput sink,
				final @Nonnull Record record, final @Nullable Record nextRecord,
				final @Nonnull PageReadTrx pageReadTrx) {
			final NamespaceNode node = (NamespaceNode) record;
			final Node nextNode = (Node) nextRecord;
			serializeDelegate(node.getNodeDelegate(), nextNode, sink, pageReadTrx);
			serializeNameDelegate(node.getNameNodeDelegate(), sink);
		}
	},

	/** Node kind is text. */
	TEXT((byte) 3, TextNode.class) {
		@Override
		public Record deserialize(final @Nonnull ByteArrayDataInput source,
				final @Nonnegative long recordID, final @Nonnull PageReadTrx pageReadTrx) {
			// Node delegate.
			final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID,
					pageReadTrx);

			// Val delegate.
			final boolean isCompressed = source.readByte() == (byte) 1 ? true : false;
			final byte[] vals = new byte[source.readInt()];
			source.readFully(vals, 0, vals.length);
			final ValNodeDelegate valDel = new ValNodeDelegate(nodeDel, vals,
					isCompressed);

			// Struct delegate.
			final long nodeKey = nodeDel.getNodeKey();
			final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
					Fixed.NULL_NODE_KEY.getStandardProperty(), nodeKey
							- getVarLong(source), nodeKey - getVarLong(source), 0L, 0L);

			// Returning an instance.
			return new TextNode(valDel, structDel);
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput sink,
				final @Nonnull Record record, final @Nullable Record nextRecord,
				final @Nonnull PageReadTrx pageReadTrx) {
			final TextNode node = (TextNode) record;
			final Node nextNode = (Node) nextRecord;
			serializeDelegate(node.getNodeDelegate(), nextNode, sink, pageReadTrx);
			serializeValDelegate(node.getValNodeDelegate(), sink);
			final StructNodeDelegate del = node.getStructNodeDelegate();
			final long nodeKey = node.getNodeKey();
			putVarLong(sink, nodeKey - del.getRightSiblingKey());
			putVarLong(sink, nodeKey - del.getLeftSiblingKey());
		}
	},

	/** Node kind is processing instruction. */
	PROCESSING_INSTRUCTION((byte) 7, PINode.class) {
		@Override
		public Record deserialize(final @Nonnull ByteArrayDataInput source,
				final @Nonnegative long recordID, final @Nonnull PageReadTrx pageReadTrx) {
			// Node delegate.
			final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID,
					pageReadTrx);

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
		public void serialize(final @Nonnull ByteArrayDataOutput sink,
				final @Nonnull Record record, final @Nullable Record nextRecord,
				final @Nonnull PageReadTrx pageReadTrx) {
			final PINode node = (PINode) record;
			final Node nextNode = (Node) nextRecord;
			serializeDelegate(node.getNodeDelegate(), nextNode, sink, pageReadTrx);
			serializeStrucDelegate(node.getStructNodeDelegate(), sink);
			serializeNameDelegate(node.getNameNodeDelegate(), sink);
			serializeValDelegate(node.getValNodeDelegate(), sink);
		}
	},

	/** Node kind is comment. */
	COMMENT((byte) 8, CommentNode.class) {
		@Override
		public Record deserialize(final @Nonnull ByteArrayDataInput source,
				final @Nonnegative long recordID, final @Nonnull PageReadTrx pageReadTrx) {
			// Node delegate.
			final NodeDelegate nodeDel = deserializeNodeDelegate(source, recordID,
					pageReadTrx);

			// Val delegate.
			final boolean isCompressed = source.readByte() == (byte) 1 ? true : false;
			final byte[] vals = new byte[source.readInt()];
			source.readFully(vals, 0, vals.length);
			final ValNodeDelegate valDel = new ValNodeDelegate(nodeDel, vals,
					isCompressed);

			// Struct delegate.
			final long nodeKey = nodeDel.getNodeKey();
			final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
					Fixed.NULL_NODE_KEY.getStandardProperty(), nodeKey
							- getVarLong(source), nodeKey - getVarLong(source), 0L, 0L);

			// Returning an instance.
			return new CommentNode(valDel, structDel);
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput sink,
				final @Nonnull Record record, final @Nullable Record nextRecord,
				final @Nonnull PageReadTrx pageReadTrx) {
			final CommentNode node = (CommentNode) record;
			final Node nextNode = (Node) nextRecord;
			serializeDelegate(node.getNodeDelegate(), nextNode, sink, pageReadTrx);
			serializeValDelegate(node.getValNodeDelegate(), sink);
			final StructNodeDelegate del = node.getStructNodeDelegate();
			final long nodeKey = node.getNodeKey();
			putVarLong(sink, nodeKey - del.getRightSiblingKey());
			putVarLong(sink, nodeKey - del.getLeftSiblingKey());
		}
	},

	/** Node kind is document root. */
	// Virtualize document root node?
	DOCUMENT((byte) 9, DocumentRootNode.class) {
		@Override
		public Record deserialize(final @Nonnull ByteArrayDataInput source,
				final @Nonnegative long recordID, final @Nonnull PageReadTrx pageReadTrx) {
			final NodeDelegate nodeDel = new NodeDelegate(
					Fixed.DOCUMENT_NODE_KEY.getStandardProperty(),
					Fixed.NULL_NODE_KEY.getStandardProperty(), source.readLong(),
					getVarLong(source), Optional.of(SirixDeweyID.newRootID()));
			final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
					getVarLong(source), Fixed.NULL_NODE_KEY.getStandardProperty(),
					Fixed.NULL_NODE_KEY.getStandardProperty(),
					source.readByte() == ((byte) 0) ? 0 : 1, source.readLong());
			return new DocumentRootNode(nodeDel, structDel);
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput pSink,
				final @Nonnull Record precord, final @Nullable Record nextRecord,
				final @Nonnull PageReadTrx pageReadTrx) {
			final DocumentRootNode node = (DocumentRootNode) precord;
			pSink.writeLong(node.getHash());
			putVarLong(pSink, node.getRevision());
			putVarLong(pSink, node.getFirstChildKey());
			pSink.writeByte(node.hasFirstChild() ? (byte) 1 : (byte) 0);
			pSink.writeLong(node.getDescendantCount());
		}
	},

	/** Whitespace text. */
	WHITESPACE((byte) 4, null) {
		@Override
		public Record deserialize(final @Nonnull ByteArrayDataInput source,
				final @Nonnegative long recordID, final @Nonnull PageReadTrx pageReadTrx) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput sink,
				final @Nonnull Record record, final @Nullable Record nextRecord,
				final @Nonnull PageReadTrx pageReadTrx) {
			throw new UnsupportedOperationException();
		}
	},

	/** Node kind is deleted node. */
	DELETE((byte) 5, DeletedNode.class) {
		@Override
		public Record deserialize(final @Nonnull ByteArrayDataInput source,
				final @Nonnegative long recordID, final @Nonnull PageReadTrx pageReadTrx) {
			final NodeDelegate delegate = new NodeDelegate(recordID, 0, 0,
					0, Optional.<SirixDeweyID> absent());
			return new DeletedNode(delegate);
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput sink,
				final @Nonnull Record record, final @Nullable Record nextRecord,
				final @Nonnull PageReadTrx pageReadTrx) {
//			DeletedNode node = (DeletedNode) record;
//			putVarLong(sink, node.getNodeKey());
		}
	},

	/** NullNode to support the Null Object pattern. */
	NULL((byte) 6, NullNode.class) {
		@Override
		public Record deserialize(final @Nonnull ByteArrayDataInput source,
				final @Nonnegative long recordID, final @Nonnull PageReadTrx pageReadTrx) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput ink,
				final @Nonnull Record record, final @Nullable Record nextRecord,
				final @Nonnull PageReadTrx pageReadTrx) {
			throw new UnsupportedOperationException();
		}
	},

	/** Dumb node for testing. */
	DUMB((byte) 20, DumbNode.class) {
		@Override
		public Record deserialize(final @Nonnull ByteArrayDataInput source,
				final @Nonnegative long recordID, final @Nonnull PageReadTrx pageReadTrx) {
//			final long nodeKey = getVarLong(source);
			return new DumbNode(recordID);
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput sink,
				final @Nonnull Record record, final @Nullable Record nextRecord,
				final @Nonnull PageReadTrx pageReadTrx) {
//			putVarLong(sink, record.getNodeKey());
		}
	},

	/** AtomicKind. */
	ATOMIC((byte) 15, AtomicValue.class) {
		@Override
		public Record deserialize(final @Nonnull ByteArrayDataInput source,
				final @Nonnegative long recordID, final @Nonnull PageReadTrx pageReadTrx) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput sink,
				final @Nonnull Record record, final @Nullable Record nextRecord,
				final @Nonnull PageReadTrx pageReadTrx) {
			throw new UnsupportedOperationException();
		}
	},

	/** Node kind is path node. */
	PATH((byte) 16, PathNode.class) {
		@Override
		public Record deserialize(final @Nonnull ByteArrayDataInput source,
				final @Nonnegative long recordID, final @Nonnull PageReadTrx pageReadTrx) {
			// Node delegate.
			final NodeDelegate nodeDel = deserializeNodeDelegateWithoutIDs(source,
					recordID, pageReadTrx);

			// Struct delegate.
			final StructNodeDelegate structDel = deserializeStructDel(nodeDel, source);

			// Name delegate.
			final NameNodeDelegate nameDel = deserializeNameDelegate(nodeDel, source);

			return new PathNode(nodeDel, structDel, nameDel, Kind.getKind(source
					.readByte()), source.readInt(), source.readInt());
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput sink,
				final @Nonnull Record record, final @Nullable Record nextRecord,
				final @Nonnull PageReadTrx pageReadTrx) {
			final PathNode node = (PathNode) record;
			final Node nextNode = (Node) nextRecord;
			serializeDelegate(node.getNodeDelegate(), nextNode, sink, pageReadTrx);
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
				final @Nonnegative long recordID, final @Nonnull PageReadTrx pageReadTrx) {
			final int size = source.readInt();
			final byte[] value = new byte[size];
			source.readFully(value, 0, size);
			final boolean kind = source.readBoolean();
			final ValueKind valueKind = kind ? ValueKind.TEXT : ValueKind.ATTRIBUTE;
			final int keySize = source.readInt();
			final Set<Long> nodeKeys = new HashSet<>(keySize);
			for (int i = 0; i < keySize; i++) {
				nodeKeys.add(source.readLong());
			}
			// Node delegate.
			final NodeDelegate nodeDel = deserializeNodeDelegateWithoutIDs(source,
					recordID, pageReadTrx);
			final long leftChild = getVarLong(source);
			final long rightChild = getVarLong(source);
			final long pathNodeKey = getVarLong(source);
			final boolean isChanged = source.readBoolean();
			final AVLNode<Value, NodeReferences> node = new AVLNode<>(new Value(
					value, pathNodeKey, valueKind), new NodeReferences(nodeKeys), nodeDel);
			node.setLeftChildKey(leftChild);
			node.setRightChildKey(rightChild);
			node.setChanged(isChanged);
			return node;
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput sink,
				final @Nonnull Record record, final @Nullable Record nextRecord,
				final @Nonnull PageReadTrx pageReadTrx) {
			@SuppressWarnings("unchecked")
			final AVLNode<Value, NodeReferences> node = (AVLNode<Value, NodeReferences>) record;
			final Value key = node.getKey();
			final byte[] textValue = key.getValue();
			sink.writeInt(textValue.length);
			sink.write(textValue);
			sink.writeBoolean(key.getKind() == ValueKind.TEXT ? true : false);
			final NodeReferences value = node.getValue();
			final Set<Long> nodeKeys = value.getNodeKeys();
			sink.writeInt(nodeKeys.size());
			for (final long nodeKey : nodeKeys) {
				sink.writeLong(nodeKey);
			}
			final Node nextNode = (Node) nextRecord;
			serializeDelegate(node.getNodeDelegate(), nextNode, sink, pageReadTrx);
			putVarLong(sink, node.getLeftChildKey());
			putVarLong(sink, node.getRightChildKey());
			putVarLong(sink, key.getPathNodeKey());
			sink.writeBoolean(node.isChanged());
		};
	},

	/** Node includes a deweyID <=> nodeKey mapping. */
	DEWEYIDMAPPING((byte) 23, DeweyIDMappingNode.class) {
		@Override
		public Record deserialize(final @Nonnull ByteArrayDataInput source,
				final @Nonnegative long recordID, final @Nonnull PageReadTrx pageReadTrx) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput sink,
				final @Nonnull Record record, final @Nullable Record nextRecord,
				final @Nonnull PageReadTrx pageReadTrx) {
			throw new UnsupportedOperationException();
		}
	},

	/** Node type not known. */
	UNKNOWN((byte) 22, null) {
		@Override
		public Record deserialize(final @Nonnull ByteArrayDataInput source,
				final @Nonnegative long recordID, final @Nonnull PageReadTrx pageReadTrx) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void serialize(final @Nonnull ByteArrayDataOutput sink,
				final @Nonnull Record record, final @Nullable Record nextRecord,
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
	 * @param id
	 *          unique identifier
	 * @param clazz
	 *          class
	 */
	private Kind(final byte id, final @Nonnull Class<? extends Record> clazz) {
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
			final @Nonnegative long recordID, final @Nonnull PageReadTrx pageReadTrx) {
		final long nodeKey = recordID;
		final long parentKey = nodeKey - getVarLong(source);
		final long hash = source.readLong();
		final long revision = getVarLong(source);
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
			final @Nonnegative long recordID, final @Nonnull PageReadTrx pageReadTrx) {
		final long nodeKey = recordID;
		final long parentKey = nodeKey - getVarLong(source);
		final long hash = source.readLong();
		final long revision = getVarLong(source);
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
	 *          node delegate
	 * @param nextNode
	 *          next node in the page or {@code null}
	 * @param sink
	 *          to serialize to
	 * @param pageReadTrx
	 *          {@link PageReadTrx} instance
	 * @param {@link ResourceConfiguration} instance
	 */
	private static final void serializeDelegate(
			final @Nonnull NodeDelegate nodeDel, final @Nonnull Node nextNode,
			final @Nonnull ByteArrayDataOutput sink,
			final @Nonnull PageReadTrx pageReadTrx) {
		putVarLong(sink, nodeDel.getNodeKey() - nodeDel.getParentKey());
		sink.writeLong(nodeDel.getHash());
		putVarLong(sink, nodeDel.getRevision());
		if (pageReadTrx.getSession().getResourceConfig().mDeweyIDsStored) {
			final Optional<SirixDeweyID> id = nodeDel.getDeweyID();
			if (id.isPresent()) {
//				if (prevNode != null && prevNode.getDeweyID().isPresent()) {
//					final byte[] prevDeweyID = prevNode.getDeweyID().get().toBytes();
//					final byte[] deweyID = nodeDel.getDeweyID().get().toBytes();
//					if (prevDeweyID.length <= deweyID.length) {
//						int i = 0;
//						for (; i < prevDeweyID.length; i++) {
//							if (prevDeweyID[i] != deweyID[i]) {
//								break;
//							}
//						}
//						writeDeweyID(sink, deweyID, i);
//					} else {
//						int i = 0;
//						for (; i < deweyID.length; i++) {
//							if (prevDeweyID[i] != deweyID[i]) {
//								break;
//							}
//						}
//						writeDeweyID(sink, prevDeweyID, i);
//					}
//				} else {
					final byte[] deweyID = nodeDel.getDeweyID().get().toBytes();
					sink.writeByte(deweyID.length);
					sink.write(deweyID);
//				}
			}
		}
	}

	/**
	 * Write the deweyID.
	 * 
	 * @param sink
	 *          to write to
	 * @param deweyID
	 *          deweyID in bytes
	 * @param i
	 *          the index from which to start the copy of the array
	 */
	private static void writeDeweyID(final @Nonnull ByteArrayDataOutput sink,
			final @Nonnull byte[] deweyID, final @Nonnegative int i) {
		sink.writeByte(deweyID.length - i - 1);
		sink.write(Arrays.copyOfRange(deweyID, i, deweyID.length));
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
		putVarLong(sink, nodeDel.getNodeKey() - nodeDel.getRightSiblingKey());
		putVarLong(sink, nodeDel.getNodeKey() - nodeDel.getLeftSiblingKey());
		putVarLong(sink, nodeDel.getNodeKey() - nodeDel.getFirstChildKey());
		putVarLong(sink, nodeDel.getChildCount());
		putVarLong(sink, nodeDel.getDescendantCount() - nodeDel.getChildCount());
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
		final long rightSibl = currKey - getVarLong(source);
		final long leftSibl = currKey - getVarLong(source);
		final long firstChild = currKey - getVarLong(source);
		final long childCount = getVarLong(source);
		final long descendantCount = getVarLong(source) + childCount;
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
		final int uriKey = source.readInt();
		int prefixKey = source.readInt();
		// prefixKey += uriKey;
		int localNameKey = source.readInt();
		// localNameKey += prefixKey;
		return new NameNodeDelegate(nodeDel, uriKey, prefixKey, localNameKey,
				getVarLong(source));
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
		sink.writeInt(nameDel.getURIKey());
		sink.writeInt(nameDel.getPrefixKey()); // - nameDel.getURIKey());
		sink.writeInt(nameDel.getLocalNameKey());// - nameDel.getPrefixKey());
		putVarLong(sink, nameDel.getPathNodeKey());
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
