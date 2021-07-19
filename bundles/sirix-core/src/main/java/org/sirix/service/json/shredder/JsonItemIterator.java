package org.sirix.service.json.shredder;

import com.google.common.collect.AbstractIterator;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Iter;
import org.sirix.axis.IncludeSelf;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;

public final class JsonItemIterator extends AbstractIterator<Item> {

  private final IncludeSelf includeSelf;
  private Deque<Iter> iter;
  private boolean first;
  private Item item;
  private Iter currIter;

  public JsonItemIterator(Item item, IncludeSelf includeSelf) {
    this.item = Objects.requireNonNull(item);
    this.includeSelf = Objects.requireNonNull(includeSelf);
    this.iter = new ArrayDeque<>();
    this.iter.push(item.iterate());
  }

  @Override
  protected Item computeNext() {
    if (first && includeSelf == IncludeSelf.YES) {
      first = false;
      currIter = item.iterate();
      iter.push(currIter);
      return item;
    }

    Item currItem = null;

    while (currItem == null && !iter.isEmpty()) {
      final Iter iterator = iter.peek();
      currItem = iterator.next();
    }

    if (currItem != null) {
      iter.push(currItem.iterate());
    }
    return currItem;
  }
}
