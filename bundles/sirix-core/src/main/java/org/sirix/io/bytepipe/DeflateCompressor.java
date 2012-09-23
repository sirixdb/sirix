/**
 * 
 */
package org.sirix.io.bytepipe;

import java.io.ByteArrayOutputStream;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import javax.annotation.Nonnull;

import org.sirix.exception.SirixIOException;

/**
 * Decorator to zip any data.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public class DeflateCompressor implements IByteHandler {

  private final Deflater mCompressor;

  private final Inflater mDecompressor;

  private final byte[] mTmp;

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
  public byte[] serialize(@Nonnull final byte[] pToSerialize)
    throws SirixIOException {
    mCompressor.reset();
    mOut.reset();
    mCompressor.setInput(pToSerialize);
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
  public byte[] deserialize(@Nonnull final byte[] pToDeserialize)
    throws SirixIOException {
    mDecompressor.reset();
    mOut.reset();
    mDecompressor.setInput(pToDeserialize);
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
  public IByteHandler getInstance() {
    return new DeflateCompressor();
  }
}
