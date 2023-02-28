package org.sirix.xquery.compiler.translator;

import org.brackit.xquery.jdm.Item;
import org.brackit.xquery.jdm.Iter;
import org.brackit.xquery.sequence.BaseIter;
import org.brackit.xquery.sequence.LazySequence;
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
