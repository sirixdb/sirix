package io.sirix.query.bench.validtime;

import io.brackit.query.BrackitQueryContext;
import io.brackit.query.Query;
import io.brackit.query.QueryContext;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.IntNumeric;
import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.CompileChain;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.jdm.type.SequenceType;
import io.brackit.query.module.Functions;
import io.brackit.query.module.StaticContext;
import io.brackit.query.sequence.AbstractSequence;
import io.brackit.query.sequence.BaseIter;
import io.brackit.query.sequence.FunctionConversionSequence;

/** No Sirix dependency: counting a lazy built-in result versus a trivial UDF wrapper. */
public final class UdfMaterializationRepro {
  private static int constructed;

  private static final class Keys extends AbstractFunction {
    Keys() {
      super(new QNm("urn:repro", "probe", "keys"), new Signature(SequenceType.ITEM_SEQUENCE), true);
    }

    @Override
    public Sequence execute(final StaticContext context, final QueryContext queryContext, final Sequence[] args) {
      return new AbstractSequence() {
        @Override
        public IntNumeric size() {
          return new Int32(64);
        }

        @Override
        public boolean booleanValue() {
          throw new UnsupportedOperationException("Not needed for count");
        }

        @Override
        public Item get(final IntNumeric position) {
          final int index = position.intValue();
          return index < 1 || index > 64 ? null : item(index);
        }

        private Item item(final int value) {
          constructed++;
          return new Int32(value);
        }

        @Override
        public Iter iterate() {
          return new BaseIter() {
            private int position;

            @Override
            public void close() {
            }

            @Override
            public Item next() {
              return position == 64 ? null : item(++position);
            }
          };
        }
      };
    }
  }

  public static void main(final String[] args) {
    Functions.predefine(new Keys());
    final String namespace = "declare namespace probe = 'urn:repro'; ";
    final String[] queries = {
        "count(probe:keys())",
        "declare function local:slice() { probe:keys() }; count(local:slice())"};
    for (final String text : queries) {
      constructed = 0;
      final Sequence count = new Query(new CompileChain(), namespace + text).evaluate(new BrackitQueryContext());
      System.out.println("count=" + count + " constructed=" + constructed + " query=" + text);
    }
    constructed = 0;
    final Sequence lazy = new Keys().execute(null, new BrackitQueryContext(), new Sequence[0]);
    final Sequence converted = FunctionConversionSequence.asTypedSequence(SequenceType.ITEM_SEQUENCE, lazy, false);
    final IntNumeric count = converted.size();
    System.out.println("count=" + count + " constructed=" + constructed + " after item()* return conversion");
  }
}
