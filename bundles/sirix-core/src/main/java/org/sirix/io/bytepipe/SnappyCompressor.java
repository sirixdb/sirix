package org.sirix.io.bytepipe;

import java.io.IOException;

import javax.annotation.Nonnull;

import org.sirix.exception.TTIOException;
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
    throws TTIOException {
    byte[] compressed;
    try {
      compressed = Snappy.compress(pToSerialize);
    } catch (final IOException e) {
      throw new TTIOException(e);
    }
    return compressed;
  }

  @Override
  public byte[] deserialize(final @Nonnull byte[] pToDeserialize)
    throws TTIOException {
    byte[] uncompressed;
    try {
      uncompressed = Snappy.uncompress(pToDeserialize);
    } catch (final IOException e) {
      throw new TTIOException(e);
    }
    return uncompressed;
  }

  @Override
  public IByteHandler getInstance() {
    return new SnappyCompressor();
  }
}
