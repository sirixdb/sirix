package org.sirix.io.bytepipe;

import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;

/**
 * Snappy compression/decompression.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class SnappyCompressor implements ByteHandler {

  @Override
  public OutputStream serialize(final OutputStream toSerialize) {
    return new SnappyOutputStream(toSerialize);
  }

  @Override
  public InputStream deserialize(final InputStream toDeserialize) {
    try {
      return new SnappyInputStream(toDeserialize);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public ByteHandler getInstance() {
    return new SnappyCompressor();
  }
}
