package io.sirix.service.json.shredder;

import com.google.common.collect.AbstractIterator;
import io.sirix.axis.IncludeSelf;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;

public final class JsonItemIterator extends AbstractIterator<Item> {

  private final IncludeSelf includeSelf;
  private final Deque<Iter> iter;
  private boolean first;
  private final Item item;

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
      Iter currIter = item.iterate();
      iter.push(currIter);
      return item;
    }

    Item currItem = null;

    while (currItem == null && !iter.isEmpty()) {
      try (final Iter iterator = iter.peek()) {
        currItem = iterator.next();
      }
    }

    if (currItem != null) {
      iter.push(currItem.iterate());
    }

    return currItem;
  }
}
