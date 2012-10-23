package org.sirix.node.interfaces;

import javax.annotation.Nonnull;

import org.sirix.api.PageReadTrx;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;

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
	 * @param source
	 *          input source
	 * @param pageReadTrx
	 *          {@link PageReadTrx} instance
	 * @return a {@link Node} instance
	 */
	NodeBase deserialize(final @Nonnull ByteArrayDataInput source,
			final @Nonnull PageReadTrx pageReadTrx);

	/**
	 * Serializing a node from a {@link ByteArrayDataOutput}.
	 * 
	 * @param sink
	 *          where the data should be serialized to
	 * @param toSerialize
	 *          the node to serialize
	 * @param pageReadTrx
	 *          {@link PageReadTrx} instance
	 */
	void serialize(final @Nonnull ByteArrayDataOutput sink,
			final @Nonnull NodeBase toSerialize,
			final @Nonnull PageReadTrx pageReadTrx);

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
