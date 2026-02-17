package io.sirix.query.compiler.translator;

import io.sirix.api.Axis;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.sequence.BaseIter;
import io.brackit.query.sequence.LazySequence;
import io.sirix.query.stream.json.SirixJsonStream;

class SirixJsonLazySequence extends LazySequence {

  private final SirixJsonStream stream;

  SirixJsonLazySequence(final SirixJsonStream stream) {
    this.stream = stream;
  }

  Axis getAxis() {
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
      public void close() {}
    };
  }
}
