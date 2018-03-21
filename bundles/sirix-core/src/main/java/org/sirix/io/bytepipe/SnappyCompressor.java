package org.sirix.io.bytepipe;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

/**
 * Snappy compression/decompression.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class SnappyCompressor implements ByteHandler {

  @Override
  public OutputStream serialize(final OutputStream toSerialize) throws IOException {
    return new SnappyOutputStream(toSerialize);
  }

  @Override
  public InputStream deserialize(final InputStream toDeserialize) throws IOException {
    return new SnappyInputStream(toDeserialize);
  }

  @Override
  public ByteHandler getInstance() {
    return new SnappyCompressor();
  }
}
