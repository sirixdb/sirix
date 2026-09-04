package io.sirix.index.projection;

import io.sirix.access.DatabaseType;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.node.ValueDictionaryEntryNode;

import java.util.Iterator;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Commits one resource-wide rank-ordered value dictionary from an ASCENDING distinct-value stream.
 *
 * <p>
 * This is the dictionary half of the load-time pre-pass: the corpus half — reading the input once
 * and producing that ascending stream under a memory budget — is {@link ExternalDistinctValues},
 * and the two together are what let a loader build these dictionaries for itself instead of being
 * handed a value file. The stream form is the load-bearing one: a fat column's distinct set is
 * gigabytes at scale ({@link ExternalDistinctValues}), so the {@link List} overload is for tests
 * and small columns only.
 * </p>
 *
 * <p>
 * Values must ascend STRICTLY under {@link ValueDictionaryEntryNode#compareUtf16Range} — the
 * dictionary's own ordering invariant. The check is per adjacent pair and runs in both overloads,
 * so a producer that emits a duplicate or an inversion fails here rather than writing a dictionary
 * whose rank order silently disagrees with the comparator every reader uses.
 * </p>
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class PrePassDictionaryBuilder {
  private PrePassDictionaryBuilder() {
    throw new AssertionError("no instances");
  }

  /**
   * @param values distinct values, ASCENDING in the engine's collation; the caller owns the order
   * @return the dictionary's header key
   */
  public static long build(final JsonNodeTrx wtx, final int column, final List<byte[]> values) {
    return build(wtx, column, requireNonNull(values, "values must not be null").iterator());
  }

  /**
   * Streaming form: the values are consumed once, in order, and never held together.
   *
   * @param values distinct values, ASCENDING in the engine's collation
   * @return the dictionary's header key
   */
  public static long build(final JsonNodeTrx wtx, final int column, final Iterator<byte[]> values) {
    requireNonNull(wtx, "wtx must not be null");
    requireNonNull(values, "values must not be null");
    final DatabaseType databaseType = GlobalValueDictionary.databaseTypeOf(wtx.getStorageEngineWriter());
    final RankPassDictionaryAppender appender =
        new RankPassDictionaryAppender(column, databaseType, wtx.getStorageEngineWriter(), generation -> {
          wtx.commit();
          return wtx.getStorageEngineWriter();
        });
    byte[] previous = null;
    long index = 0;
    while (values.hasNext()) {
      final byte[] value = requireNonNull(values.next(), "a distinct value must not be null");
      if (previous != null
          && ValueDictionaryEntryNode.compareUtf16Range(previous, 0, previous.length, value, 0, value.length) >= 0) {
        throw new IllegalArgumentException("values must ascend strictly; they do not at " + index);
      }
      appender.accept(value, 0, value.length);
      previous = value;
      index++;
    }
    appender.finish();
    final var writer = wtx.getStorageEngineWriter();
    GlobalValueDictionary.buildBlockIndex(appender.headerKey(), writer.getNamePage(writer.getActualRevisionRootPage()),
        databaseType, writer, writer.getLog());
    wtx.commit();
    return appender.headerKey();
  }
}
