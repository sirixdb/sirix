package org.sirix.xquery.compiler.translator;

import org.brackit.xquery.sequence.BaseIter;
import org.brackit.xquery.sequence.LazySequence;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Iter;
import org.sirix.xquery.stream.json.SirixJsonStream;

class SirixJsonLazySequence extends LazySequence {

  private final SirixJsonStream stream;

  SirixJsonLazySequence(final SirixJsonStream stream) {
    this.stream = stream;
  }

  org.sirix.api.Axis getAxis() {
    return stream.getAxis();
  }

  @Override
  public Iter iterate() {
    return new BaseIter() {
      @Override
      public Item next() {
        return stream.next();
      }

      @Override
      public void close() {
      }
    };
  }
}
