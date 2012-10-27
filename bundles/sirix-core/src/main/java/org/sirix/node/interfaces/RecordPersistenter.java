package org.sirix.node.interfaces;

import javax.annotation.Nonnull;

import org.sirix.api.PageReadTrx;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;

/**
 * Persistenting a record (first byte of a record must be its type).
 * 
 * @author Johannes Lichtenberger
 * 
 */
public interface RecordPersistenter {
	/**
	 * Deserializing a record using a {@link ByteArrayDataInput} instance.
	 * 
	 * @param source
	 *          input source
	 * @param pageReadTrx
	 *          {@link PageReadTrx} instance
	 * @return a {@link Node} instance
	 * @throws NullPointerException
	 *           if one of the parameters is {@code null}
	 */
	@Nonnull
	Record deserialize(@Nonnull ByteArrayDataInput source,
			@Nonnull PageReadTrx pageReadTrx);

	/**
	 * Serializing a record from a {@link ByteArrayDataOutput} instance.
	 * 
	 * @param sink
	 *          where the data should be serialized to
	 * @param toSerialize
	 *          the node to serialize
	 * @param pageReadTrx
	 *          {@link PageReadTrx} instance
	 * @throws NullPointerException
	 *           if one of the parameters is {@code null}
	 */
	void serialize(@Nonnull ByteArrayDataOutput sink,
			@Nonnull Record toSerialize, @Nonnull PageReadTrx pageReadTrx);
}
