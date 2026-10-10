package io.sirix.query;

import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.node.Node;
import io.brackit.query.sequence.AbstractSequence;
import io.brackit.query.sequence.BaseIter;
import io.brackit.query.util.serialize.StringSerializer;

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
  private ColumnarJsonWriter columnarWriter;

  public SirixStringSerializer(final PrintWriter out) {
    super(Objects.requireNonNull(out));
    this.out = out;
  }

  public SirixStringSerializer(final PrintStream out) {
    this(new PrintWriter(Objects.requireNonNull(out)));
  }

  @Override
  public void serialize(final Sequence sequence) {
    if (sequence == null
        || (sequence instanceof Item
            && (!(sequence instanceof ColumnarRecordSequence.Record record) || !record.isBatchWritable()))
        || isFormat()) {
      super.serialize(sequence);
      return;
    }
    boolean delegated = false;
    try (final Iter iterator = sequence.iterate()) {
      final Item head = iterator.next();
      if (!ColumnarJsonWriter.isBatchRecord(head)) {
        // Replay the peeked item with the same iterator: never evaluate a lazy sequence twice.
        delegated = true;
        super.serialize(new AbstractSequence() {
          public Iter iterate() {
            return new BaseIter() {
              private boolean pending = true;

              public Item next() {
                if (pending) {
                  pending = false;
                  return head;
                }
                return iterator.next();
              }

              public void close() {}
            };
          }
        });
        return;
      }
      if (columnarWriter == null) {
        columnarWriter = new ColumnarJsonWriter(out);
      }
      final ColumnarJsonWriter buffer = columnarWriter;
      boolean first = true;
      for (Item item = head; item != null; item = iterator.next()) {
        if (!first && !(item instanceof Node<?>)) {
          buffer.append(' ');
        }
        if (!buffer.record(item)) {
          buffer.drain();
          super.serialize(item);
        }
        first = item instanceof Node<?>;
      }
      buffer.drain();
    } catch (final IOException exception) {
      throw new UncheckedIOException(exception);
    } finally {
      if (!delegated) {
        try {
          if (columnarWriter != null) {
            columnarWriter.drain();
          }
        } catch (final IOException exception) {
          throw new UncheckedIOException(exception);
        } finally {
          out.flush();
        }
      }
    }
  }

}
