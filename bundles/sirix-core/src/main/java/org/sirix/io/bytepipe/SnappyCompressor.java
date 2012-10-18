package org.sirix.io.bytepipe;

import java.io.IOException;

import javax.annotation.Nonnull;

import org.sirix.exception.SirixIOException;
import org.xerial.snappy.Snappy;

/**
 * Snappy compression/decompression.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public class SnappyCompressor implements ByteHandler {

	@Override
	public byte[] serialize(final @Nonnull byte[] toSerialize)
			throws SirixIOException {
		byte[] compressed;
		try {
			compressed = Snappy.compress(toSerialize);
		} catch (final IOException e) {
			throw new SirixIOException(e);
		}
		return compressed;
	}

	@Override
	public byte[] deserialize(final @Nonnull byte[] toDeserialize)
			throws SirixIOException {
		byte[] uncompressed;
		try {
			uncompressed = Snappy.uncompress(toDeserialize);
		} catch (final IOException e) {
			throw new SirixIOException(e);
		}
		return uncompressed;
	}

	@Override
	public ByteHandler getInstance() {
		return new SnappyCompressor();
	}
}
