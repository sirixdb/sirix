package io.sirix.page;


import io.sirix.api.PageTrx;
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
  private final MemorySegment data;

  /**
   * Constructor.
   *
   * @param data data to be stored
   */
  public OverflowPage(final MemorySegment data) {
    assert data != null;
    this.data = data;
  }


  @Override
  public List<PageReference> getReferences() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void commit(@NonNull PageTrx pageWriteTrx) {
  }

  @Override
  public PageReference getOrCreateReference(int offset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean setOrCreateReference(int offset, PageReference pageReference) {
    throw new UnsupportedOperationException();
  }

  public MemorySegment getData() {
    return data;
  }
}
