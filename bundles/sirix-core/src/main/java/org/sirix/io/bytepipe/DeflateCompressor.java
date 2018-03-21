/**
 * 
 */
package org.sirix.io.bytepipe;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * Decorator to zip any data.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public final class DeflateCompressor implements ByteHandler {

  @Override
  public OutputStream serialize(final OutputStream toSerialize) throws IOException {
    return new DeflaterOutputStream(toSerialize);
  }

  @Override
  public InputStream deserialize(final InputStream toDeserialize) throws IOException {
    return new InflaterInputStream(toDeserialize);
  }

  @Override
  public ByteHandler getInstance() {
    return new DeflateCompressor();
  }
}
