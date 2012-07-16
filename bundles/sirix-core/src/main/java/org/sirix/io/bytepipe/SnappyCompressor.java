package org.sirix.io.bytepipe;

import java.io.IOException;

import javax.annotation.Nonnull;

import org.sirix.exception.TTIOException;
import org.xerial.snappy.Snappy;

public class SnappyCompressor implements IByteHandler {

  @Override
  public byte[] serialize(@Nonnull final byte[] pToSerialize)
    throws TTIOException {
    byte[] compressed;
    try {
      compressed = Snappy.compress(pToSerialize);
    } catch(final IOException e) {
      throw new TTIOException(e);
    }
    return compressed;
  }

  @Override
  public byte[] deserialize(@Nonnull final byte[] pToDeserialize)
    throws TTIOException {
    byte[] uncompressed;
    try {
      uncompressed = Snappy.uncompress(pToDeserialize);
    } catch(final IOException e) {
      throw new TTIOException(e);
    }
    return uncompressed;
  }

}
