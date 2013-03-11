package org.sirix.node.interfaces;

import javax.annotation.Nonnegative;
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
	 * Deserialize a record using a {@link ByteArrayDataInput} instance.
	 * 
	 * @param source
	 *          input source
	 * @param recordID
	 * 					the unique recordID
	 * @param pageReadTrx
	 *          {@link PageReadTrx} instance
	 * @return a {@link Node} instance
	 * @throws NullPointerException
	 *           if one of the parameters is {@code null}
	 */
	@Nonnull
	Record deserialize(@Nonnull ByteArrayDataInput source, @Nonnegative long recordID, 
			@Nonnull PageReadTrx pageReadTrx);

	/**
	 * Serialize a record from a {@link ByteArrayDataOutput} instance.
	 * 
	 * @param sink
	 *          where the data should be serialized to
	 * @param record
	 *          the record to serialize
	 * @param previousRecord
	 *          the previous record to serialize, is {@code null} for the first
	 *          record of a RecordPage
	 * @param pageReadTrx
	 *          {@link PageReadTrx} instance
	 * @throws NullPointerException
	 *           if one of the parameters is {@code null}
	 */
	void serialize(@Nonnull ByteArrayDataOutput sink, @Nonnull Record record,
			@Nonnull Record previousRecord, @Nonnull PageReadTrx pageReadTrx);
}
