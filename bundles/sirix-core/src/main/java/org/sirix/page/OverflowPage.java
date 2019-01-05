package org.sirix.page;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import org.sirix.api.PageWriteTrx;
import org.sirix.node.interfaces.Record;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.page.interfaces.Page;

/**
 * OverflowPage used to store records which are longer than a predefined threshold.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class OverflowPage implements Page {

  /** Data to be stored. */
  private final byte[] mData;

  public OverflowPage() {
    mData = new byte[0];
  }

  /**
   * Constructor.
   *
   * @param data data to be stored
   */
  public OverflowPage(final byte[] data) {
    assert data != null;
    mData = data;
  }

  public OverflowPage(final DataInput in) throws IOException {
    mData = new byte[in.readInt()];
    in.readFully(mData);
  }

  @Override
  public List<PageReference> getReferences() {
    throw new UnsupportedOperationException();
  }

  @Override
  public <K extends Comparable<? super K>, V extends Record, S extends KeyValuePage<K, V>> void commit(
      PageWriteTrx<K, V, S> pageWriteTrx) {}

  @Override
  public PageReference getReference(int offset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setReference(int offset, PageReference pageReference) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void serialize(final DataOutput out, final SerializationType type) throws IOException {
    out.writeInt(mData.length);
    out.write(mData);
  }

  public byte[] getData() {
    return mData;
  }
}
