package org.sirix.gui.view;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Item axis which implements a simple iterator and iterable.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public class VisualItemAxis implements Iterator<VisualItem>, Iterable<VisualItem> {

  /** Diff list. */
  private final List<? extends VisualItem> mItems;

  /** Index in diff list. */
  private int mIndex;
  
  /** First index. */
  private int mFirstIndex;

  /**
   * Constructor.
   * 
   * @param pItems
   *          visual items
   */
  public VisualItemAxis(final List<? extends VisualItem> pItems) {
    this(pItems, -1);
  }

  /**
   * Constructor.
   * 
   * @param pItems
   *          visual items
   * @param pIndex
   *          start index
   */
  public VisualItemAxis(final List<? extends VisualItem> pItems, final int pIndex) {
    checkArgument(pIndex >= -1 && pIndex < pItems.size(), "pIndex must be >= 0 and < item size!");
    mItems = checkNotNull(pItems);
    mIndex = pIndex;
    mFirstIndex = mIndex;
  }

  @Override
  public Iterator<VisualItem> iterator() {
    return this;
  }

  @Override
  public boolean hasNext() {
    if (mIndex + 1 < mItems.size()) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public VisualItem next() {
    VisualItem item = null;
    try {
      item = mItems.get(++mIndex);
    } catch (final IndexOutOfBoundsException e) {
      throw new NoSuchElementException();
    }
    return item;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
  
  /**
   * Reset the axis.
   */
  public void reset() {
    mIndex = mFirstIndex;
  }
}
