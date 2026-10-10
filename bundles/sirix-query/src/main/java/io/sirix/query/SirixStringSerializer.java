package io.sirix.query;

import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.node.Node;
import io.brackit.query.sequence.AbstractSequence;
import io.brackit.query.sequence.BaseIter;
import io.brackit.query.util.serialize.StringSerializer;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Objects;

/**
 * Brackit-compatible output with batched columnar and computed primitive records. Recognizing
 * record items also works through Brackit's execution-scoped sequence wrapper, without unwrapping
 * or losing its scope.
 */
public final class SirixStringSerializer extends StringSerializer {
  private final PrintWriter out;
  private @Nullable ColumnarJsonWriter columnarWriter;

  public SirixStringSerializer(final PrintWriter out) {
    super(Objects.requireNonNull(out));
    this.out = out;
  }

  public SirixStringSerializer(final PrintStream out) {
    this(new PrintWriter(Objects.requireNonNull(out), false, out.charset()));
  }

  @Override
  public void serialize(final @Nullable Sequence sequence) {
    if (sequence == null
        || (sequence instanceof Item
            && (!(sequence instanceof ColumnarRecordSequence.RowRecord record) || !record.isBatchWritable()))
        || isFormat()) {
      super.serialize(sequence);
      return;
    }
    boolean delegated = false;
    try (final Iter iterator = sequence.iterate()) {
      final Item head = iterator.next();
      if (!ColumnarJsonWriter.isBatchRecord(head)) {
        delegated = true;
        super.serialize(remaining(head, iterator));
        return;
      }
      if (columnarWriter == null) {
        columnarWriter = new ColumnarJsonWriter(out);
      }
      try (final ColumnarJsonWriter buffer = columnarWriter) {
        boolean first = true;
        for (Item item = head; item != null; item = iterator.next()) {
          if (!first && !(item instanceof Node<?>)) {
            buffer.append(' ');
          }
          if (!buffer.record(item)) {
            buffer.drain();
            delegated = true;
            super.serialize(remaining(item, iterator));
            return;
          }
          first = false;
        }
      }
    } catch (final IOException exception) {
      throw new UncheckedIOException(exception);
    } finally {
      if (!delegated) {
        out.flush();
      }
    }
  }

  private static Sequence remaining(final @Nullable Item head, final Iter iterator) {
    return new AbstractSequence() {
      @Override
      public Iter iterate() {
        return new BaseIter() {
          private boolean pending = true;

          @Override
          public @Nullable Item next() {
            if (pending) {
              pending = false;
              return head;
            }
            return iterator.next();
          }

          @Override
          public void close() {}
        };
      }
    };
  }

}
