package org.sirix.node.interfaces;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;

import javax.annotation.Nonnull;

/**
 * Node kind interface.
 * 
 * @author Johannes Lichtenberger
 *
 */
public interface NodeKind {
	/**
	 * Deserializing a node using a {@link ByteArrayDataInput}.
	 * 
	 * @param pSource
	 *          input source
	 * @return a {@link Node} instance
	 */
	NodeBase deserialize(final @Nonnull ByteArrayDataInput pSource);

	/**
	 * Serializing a node from a {@link ByteArrayDataOutput}.
	 * 
	 * @param pSink
	 *          where the data should be serialized to
	 * @param pToSerialize
	 *          the node to serialize
	 */
	void serialize(final @Nonnull ByteArrayDataOutput pSink,
			final @Nonnull NodeBase pToSerialize);

	/**
	 * Get the nodeKind.
	 * 
	 * @return the unique kind
	 */
	byte getId();

	/**
	 * Get class of node.
	 * 
	 * @return class of node
	 */
	Class<? extends NodeBase> getNodeClass();
}
