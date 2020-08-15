package org.sirix.page;

import org.sirix.api.PageTrx;
import org.sirix.page.interfaces.Page;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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

  public OverflowPage(final DataInput in) throws IOException {
    data = new byte[in.readInt()];
    in.readFully(data);
  }

  @Override
  public List<PageReference> getReferences() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void commit(PageTrx pageWriteTrx) {
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
  public void serialize(final DataOutput out, final SerializationType type) throws IOException {
    out.writeInt(data.length);
    out.write(data);
  }

  public byte[] getData() {
    return data;
  }
}
