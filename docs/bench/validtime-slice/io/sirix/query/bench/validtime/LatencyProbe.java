package io.sirix.query.bench.validtime;

import io.brackit.query.Query;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.sequence.ItemSequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.bench.bitemporal.BitemporalSchema;
import io.sirix.query.bench.bitemporal.BitemporalQueries;
import io.sirix.query.bench.bitemporal.BitemporalCanonicalizer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;

public final class LatencyProbe {
  public static void main(final String[] args) throws Exception {
    if (args.length < 5) {
      throw new IllegalArgumentException("Usage: LatencyProbe <db-root> <oracle-dir> <output-dir> <reps> <query-numbers...>");
    }
    final Path db = Path.of(args[0]);
    final Path oracle = Path.of(args[1]);
    final Path output = Path.of(args[2]);
    Files.createDirectories(output);
    final int reps = Integer.parseInt(args[3]);
    if (reps < 1) {
      throw new IllegalArgumentException("reps must be positive");
    }
    try (var store = BasicJsonDBStore.newBuilder().location(db).build();
         var context = SirixQueryContext.createWithJsonStore(store);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = store.lookup("bt");
      for (final String resource : new String[]{"contracts", "products", "suppliers"}) {
        final var doc = collection.getDocument(resource, BitemporalSchema.systemTime(24));
        doc.getTrx().close();
      }
      for (int a = 4; a < args.length; a++) {
        final int number = Integer.parseInt(args[a]);
        if (number < 1 || number > 12) {
          throw new IllegalArgumentException("query number must be in [1,12]");
        }
        final var spec = BitemporalQueries.all().get(number - 1);
        for (int rep = 0; rep < reps; rep++) {
          final long t0 = System.nanoTime();
          final Query query = new Query(chain, spec.text());
          final long t1 = System.nanoTime();
          final Sequence sequence = query.execute(context);
          final ArrayList<Item> items = new ArrayList<>();
          if (sequence != null) {
            try (final Iter iter = sequence.iterate()) {
              Item item;
              while ((item = iter.next()) != null) {
                items.add(item);
              }
            }
          }
          final long t2 = System.nanoTime();
          final StringWriter serialized = new StringWriter();
          try (final PrintWriter writer = new PrintWriter(serialized)) {
            new StringSerializer(writer).serialize(new ItemSequence(items.toArray(Item[]::new)));
          }
          final long t3 = System.nanoTime();
          final Path tsv = output.resolve("q" + number + ".tsv");
          BitemporalCanonicalizer.write(spec, serialized.toString(), tsv);
          final long t4 = System.nanoTime();
          if (Files.mismatch(tsv, oracle.resolve("q" + number + ".tsv")) != -1) {
            throw new IllegalStateException("Oracle mismatch Q" + number);
          }
          System.out.printf("RUN q=%d rep=%d compile_ms=%.3f execute_ms=%.3f serialize_ms=%.3f canonicalize_ms=%.3f total_ms=%.3f rows=%d%n",
              number, rep, (t1-t0)/1e6, (t2-t1)/1e6, (t3-t2)/1e6, (t4-t3)/1e6, (t3-t0)/1e6, items.size());
        }
      }
    }
  }
}
