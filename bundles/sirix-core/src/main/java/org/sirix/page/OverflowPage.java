package org.sirix.page;

import net.openhft.chronicle.bytes.Bytes;
import org.jetbrains.annotations.NotNull;
import org.sirix.api.PageTrx;
import org.sirix.page.interfaces.Page;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * OverflowPage used to store records which are longer than a predefined threshold.
 *
 * @author Johannes Lichtenberger
 */
public final class OverflowPage implements Page {

  /**
   * Data to be stored.
   */
  private final byte[] data;

  public OverflowPage() {
    data = new byte[0];
  }

  /**
   * Constructor.
   *
   * @param data data to be stored
   */
  public OverflowPage(final byte[] data) {
    assert data != null;
    this.data = data;
  }

  public OverflowPage(final Bytes<ByteBuffer> in) {
    data = new byte[in.readInt()];
    in.read(data);
  }

  @Override
  public List<PageReference> getReferences() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void commit(@NotNull PageTrx pageWriteTrx) {
  }

  @Override
  public PageReference getOrCreateReference(int offset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean setOrCreateReference(int offset, PageReference pageReference) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void serialize(final Bytes<ByteBuffer> out, final SerializationType type) {
    out.writeInt(data.length);
    out.write(data);
  }

  public byte[] getData() {
    return data;
  }
}
