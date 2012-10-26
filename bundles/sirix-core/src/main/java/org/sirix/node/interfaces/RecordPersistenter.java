package org.sirix.node.interfaces;

import javax.annotation.Nonnull;

import org.sirix.api.PageReadTrx;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;

/**
 * Persistenting a record.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public interface RecordPersistenter {
	/**
	 * Deserializing a node using a {@link ByteArrayDataInput}.
	 * 
	 * @param source
	 *          input source
	 * @param pageReadTrx
	 *          {@link PageReadTrx} instance
	 * @return a {@link Node} instance
	 */
	Record deserialize(final @Nonnull ByteArrayDataInput source,
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
			final @Nonnull Record toSerialize,
			final @Nonnull PageReadTrx pageReadTrx);
}
