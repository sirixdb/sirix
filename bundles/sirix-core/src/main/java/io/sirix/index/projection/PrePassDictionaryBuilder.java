package io.sirix.index.projection;

import io.sirix.access.DatabaseType;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.node.ValueDictionaryEntryNode;

import java.util.List;

/**
 * MEASUREMENT HARNESS: builds a rank-ordered dictionary from an ascending value list.
 *
 * <p>
 * This is the fresh-build route's pre-pass, minus the corpus scan — the caller supplies the distinct
 * values. It exists in the harness rather than in product code because the extraction half is
 * corpus-shaped, and because the promotion gate that would elect this route has not been re-derived.
 * </p>
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
    for (int i = 1; i < values.size(); i++) {
      final byte[] previous = values.get(i - 1);
      final byte[] current = values.get(i);
      if (ValueDictionaryEntryNode.compareUtf16Range(previous, 0, previous.length, current, 0, current.length) >= 0) {
        throw new IllegalArgumentException("values must ascend strictly; they do not at " + i);
      }
    }
    final DatabaseType databaseType = GlobalValueDictionary.databaseTypeOf(wtx.getStorageEngineWriter());
    final RankPassDictionaryAppender appender =
        new RankPassDictionaryAppender(column, databaseType, wtx.getStorageEngineWriter(), generation -> {
          wtx.commit();
          return wtx.getStorageEngineWriter();
        });
    for (final byte[] value : values) {
      appender.accept(value, 0, value.length);
    }
    appender.finish();
    final var writer = wtx.getStorageEngineWriter();
    GlobalValueDictionary.buildBlockIndex(appender.headerKey(),
        writer.getNamePage(writer.getActualRevisionRootPage()), databaseType, writer, writer.getLog());
    wtx.commit();
    return appender.headerKey();
  }
}
