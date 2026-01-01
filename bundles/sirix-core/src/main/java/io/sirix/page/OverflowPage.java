package io.sirix.page;


import io.sirix.api.StorageEngineWriter;
import io.sirix.page.interfaces.Page;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.lang.foreign.MemorySegment;
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

  /**
   * Constructor.
   *
   * @param data data to be stored as byte array
   */
  public OverflowPage(final byte[] data) {
    assert data != null;
    this.data = data;
  }


  @Override
  public List<PageReference> getReferences() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void commit(@NonNull StorageEngineWriter pageWriteTrx) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PageReference getOrCreateReference(int offset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean setOrCreateReference(int offset, PageReference pageReference) {
    throw new UnsupportedOperationException();
  }

  /**
   * Get the data as a MemorySegment (for compatibility with existing code).
   * Returns a heap segment backed by the byte array.
   */
  public MemorySegment getData() {
    return MemorySegment.ofArray(data);
  }

  /**
   * Get the raw byte array data.
   */
  public byte[] getDataBytes() {
    return data;
  }
}
