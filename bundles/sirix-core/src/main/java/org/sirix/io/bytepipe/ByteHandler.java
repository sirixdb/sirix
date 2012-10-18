package org.sirix.io.bytepipe;

import javax.annotation.Nonnull;

import org.sirix.exception.SirixIOException;

/**
 * Interface for the decorator, representing any byte representation to be
 * serialized or to serialize.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public interface ByteHandler {

	/**
	 * Method to serialize any byte-chunk.
	 * 
	 * @param toSerialize
	 *          byte to be serialized
	 * @return result of the serialization
	 * @throws SirixIOException
	 *           if a byte handling exception occurs
	 */
	byte[] serialize(@Nonnull byte[] toSerialize) throws SirixIOException;

	/**
	 * Method to deserialize any byte-chunk.
	 * 
	 * @param toDeserialize
	 *          to deserialize
	 * @return result of the deserialization
	 * @throws SirixIOException
	 *           if a byte handling exception occurs
	 */
	byte[] deserialize(@Nonnull byte[] toDeserialize) throws SirixIOException;

	/**
	 * Method to retrieve a new instance.
	 * 
	 * @return new instance
	 */
	ByteHandler getInstance();
}
