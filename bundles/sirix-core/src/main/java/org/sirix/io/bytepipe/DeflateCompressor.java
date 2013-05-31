/**
 * 
 */
package org.sirix.io.bytepipe;

import java.io.ByteArrayOutputStream;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.sirix.exception.SirixIOException;

/**
 * Decorator to zip any data.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public class DeflateCompressor implements ByteHandler {

	/** {@link Deflater} instance. */
	private final Deflater mCompressor;

	/** {@link Inflater} instance. */
	private final Inflater mDecompressor;

	private final byte[] mTmp;

	/** {@link ByteArrayOutputStream} instance. */
	private final ByteArrayOutputStream mOut;

	/**
	 * Constructor.
	 */
	public DeflateCompressor() {
		mCompressor = new Deflater();
		mDecompressor = new Inflater();
		mTmp = new byte[32767];
		mOut = new ByteArrayOutputStream();
	}

	@Override
	public byte[] serialize(final byte[] toSerialize)
			throws SirixIOException {
		mCompressor.reset();
		mOut.reset();
		mCompressor.setInput(toSerialize);
		mCompressor.finish();
		int count;
		while (!mCompressor.finished()) {
			count = mCompressor.deflate(mTmp);
			mOut.write(mTmp, 0, count);
		}
		final byte[] result = mOut.toByteArray();
		return result;
	}

	@Override
	public byte[] deserialize(final byte[] toDeserialize)
			throws SirixIOException {
		mDecompressor.reset();
		mOut.reset();
		mDecompressor.setInput(toDeserialize);
		int count;
		while (!mDecompressor.finished()) {
			try {
				count = mDecompressor.inflate(mTmp);
			} catch (final DataFormatException e) {
				throw new SirixIOException(e);
			}
			mOut.write(mTmp, 0, count);
		}
		final byte[] result = mOut.toByteArray();
		return result;
	}

	@Override
	public ByteHandler getInstance() {
		return new DeflateCompressor();
	}
}
