package org.sirix.xquery.function.jn.index.scan;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.jdm.*;
import org.brackit.xquery.sequence.BaseIter;
import org.brackit.xquery.sequence.LazySequence;
import org.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.sirix.xquery.json.JsonDBItem;
import org.sirix.xquery.stream.json.SirixJsonItemKeyStream;

import java.util.Iterator;

public abstract class AbstractScanIndex extends AbstractFunction {
  public AbstractScanIndex(QNm name, Signature signature, boolean isBuiltIn) {
    super(name, signature, isBuiltIn);
  }

  public Sequence getSequence(final JsonDBItem doc, final Iterator<NodeReferences> index) {
    return new LazySequence() {
      @Override
      public Iter iterate() {
        return new BaseIter() {
          Stream<?> s;

          @Override
          public Item next() {
            if (s == null) {
              s = new SirixJsonItemKeyStream(index, doc.getCollection(), doc.getTrx());
            }
            return (Item) s.next();
          }

          @Override
          public void close() {
            if (s != null) {
              s.close();
            }
          }
        };
      }
    };
  }
}
