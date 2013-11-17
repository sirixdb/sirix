package org.sirix.node.interfaces;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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
	 * @throws IOException
	 * 					 if an I/O error occurs during deserialization
	 */
	@Nonnull
	Record deserialize(DataInput source, @Nonnegative long recordID, 
			PageReadTrx pageReadTrx) throws IOException;

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
	 * @throws IOException
	 * 					 if an I/O error occurs during serialization
	 */
	void serialize(DataOutput sink, Record record,
			Record previousRecord, PageReadTrx pageReadTrx) throws IOException;
}
