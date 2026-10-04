package io.sirix.query.performance;

import io.brackit.query.Query;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.sequence.ItemSequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.bench.bitemporal.BitemporalCanonicalizer;
import io.sirix.query.bench.bitemporal.BitemporalQueries;
import io.sirix.query.bench.bitemporal.BitemporalSchema;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBItem;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class LatencyProbe {
  public static void main(final String[] args) throws Exception {
    final Path db = Path.of(args[0]);
    final Path oracle = Path.of(args[1]);
    final Path out = Path.of(args[2]);
    final int reps = Integer.parseInt(args[3]);
    Files.createDirectories(out);
    final long start = System.nanoTime();
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(db).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup(BitemporalSchema.DATABASE);
      for (final String name : List.of(BitemporalSchema.CONTRACTS, BitemporalSchema.PRODUCTS, BitemporalSchema.SUPPLIERS)) {
        final JsonDBItem item = collection.getDocument(name, BitemporalSchema.systemTime(24));
        item.getTrx().close();
      }
      System.out.printf("SETUP ms=%.3f%n", (System.nanoTime() - start) / 1e6);
      for (int arg = 4; arg < args.length; arg++) {
        final int q = Integer.parseInt(args[arg]);
        final BitemporalQueries.Query text = BitemporalQueries.all().get(q - 1);
        for (int rep = 0; rep < reps; rep++) {
          final long t0 = System.nanoTime();
          final Query query = new Query(chain, text.text());
          final long t1 = System.nanoTime();
          final Sequence value = query.execute(ctx);
          final List<Item> items = new ArrayList<>();
          if (value != null) {
            try (final Iter iter = value.iterate()) {
              Item item;
              while ((item = iter.next()) != null) items.add(item);
            }
          }
          final long t2 = System.nanoTime();
          final StringWriter serialized = new StringWriter();
          try (final PrintWriter writer = new PrintWriter(serialized)) {
            new StringSerializer(writer).serialize(new ItemSequence(items.toArray(Item[]::new)));
          }
          final long t3 = System.nanoTime();
          final Path tsv = out.resolve("q" + q + ".tsv");
          BitemporalCanonicalizer.write(text, serialized.toString(), tsv);
          if (Files.mismatch(tsv, oracle.resolve("q" + q + ".tsv")) != -1) throw new IllegalStateException("oracle mismatch Q" + q);
          final long t4 = System.nanoTime();
          System.out.printf("RUN q=%d rep=%d compile_ms=%.3f execute_ms=%.3f serialize_ms=%.3f canonical_ms=%.3f total_ms=%.3f rows=%d%n",
              q, rep, (t1-t0)/1e6, (t2-t1)/1e6, (t3-t2)/1e6, (t4-t3)/1e6, (t3-t0)/1e6, items.size());
          System.out.flush();
        }
      }
    }
  }
}
