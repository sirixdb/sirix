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
public class SnappyCompressor implements IByteHandler {

  @Override
  public byte[] serialize(final @Nonnull byte[] pToSerialize)
    throws SirixIOException {
    byte[] compressed;
    try {
      compressed = Snappy.compress(pToSerialize);
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
    return compressed;
  }

  @Override
  public byte[] deserialize(final @Nonnull byte[] pToDeserialize)
    throws SirixIOException {
    byte[] uncompressed;
    try {
      uncompressed = Snappy.uncompress(pToDeserialize);
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
    return uncompressed;
  }

  @Override
  public IByteHandler getInstance() {
    return new SnappyCompressor();
  }
}
