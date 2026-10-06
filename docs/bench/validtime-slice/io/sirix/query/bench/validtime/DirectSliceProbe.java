package io.sirix.query.bench.validtime;

import io.brackit.query.Query;
import io.brackit.query.atomic.Numeric;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.bench.bitemporal.BitemporalSchema;
import io.sirix.query.json.BasicJsonDBStore;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Isolates a direct half-open slice count; the expected count is the sum of SH1 Q7's oracle groups.
 */
public final class DirectSliceProbe {
  private static final String QUERY = """
      count(jn:open-bitemporal('bt','contracts',xs:dateTime('2024-12-26T00:00:00Z'),
                                       xs:dateTime('2024-06-15T00:00:00Z')))
      """;

  public static void main(final String[] args) throws Exception {
    if (args.length != 3) {
      throw new IllegalArgumentException("Usage: DirectSliceProbe <db-root> <q7-oracle.tsv> <repetitions>");
    }
    final int repetitions = Integer.parseInt(args[2]);
    if (repetitions < 1) {
      throw new IllegalArgumentException("repetitions must be positive");
    }
    long expected = 0;
    for (final String row : Files.readAllLines(Path.of(args[1]))) {
      expected += Long.parseLong(row.split("\t", -1)[1]);
    }
    try (var store = BasicJsonDBStore.newBuilder().location(Path.of(args[0])).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var document = store.lookup("bt").getDocument("contracts", BitemporalSchema.systemTime(24));
      document.getTrx().close();
      for (int repetition = 0; repetition < repetitions; repetition++) {
        final long start = System.nanoTime();
        final Query query = new Query(chain, QUERY);
        final long compiled = System.nanoTime();
        final long count = ((Numeric) query.evaluate(context)).longValue();
        final long finished = System.nanoTime();
        if (count != expected) {
          throw new IllegalStateException("Slice count " + count + " differs from Q7 oracle sum " + expected);
        }
        System.out.printf("SLICE rep=%d compile_ms=%.3f execute_ms=%.3f total_ms=%.3f count=%d%n", repetition,
            (compiled - start) / 1e6, (finished - compiled) / 1e6, (finished - start) / 1e6, count);
      }
    }
  }
}
