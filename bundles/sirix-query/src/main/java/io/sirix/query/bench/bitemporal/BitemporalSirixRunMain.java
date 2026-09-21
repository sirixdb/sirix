package io.sirix.query.bench.bitemporal;

import io.brackit.query.Query;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.index.IndexDef;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBItem;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;

/** Executes, fully materializes, validates and canonicalizes all twelve SH1 Sirix queries. */
public final class BitemporalSirixRunMain {

  private static final Path REQUIRED_ROOT = Path.of("/var/tmp/sirix-bitemporal");

  private BitemporalSirixRunMain() {
    throw new AssertionError("no instances");
  }

  public static void main(final String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: BitemporalSirixRunMain <database-root> <output-directory>");
      System.exit(2);
      return;
    }
    final Path databaseRoot = requireCampaignPath(Path.of(args[0]), "database root");
    final Path output = requireCampaignPath(Path.of(args[1]), "output directory");
    if (!Files.isDirectory(databaseRoot.resolve(BitemporalSchema.DATABASE))) {
      throw new IllegalArgumentException("missing Sirix database bt under " + databaseRoot);
    }
    Files.createDirectories(output);

    final StringBuilder manifest = new StringBuilder(4_096).append("{\"engine\":\"sirix\",\"queries\":[");
    try (BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(databaseRoot).build();
        SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
        SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup(BitemporalSchema.DATABASE);
      if (collection == null) {
        throw new IllegalStateException("collection bt is not registered");
      }
      assertValidTimeIndex(collection, BitemporalSchema.CONTRACTS);
      assertValidTimeIndex(collection, BitemporalSchema.PRODUCTS);
      assertValidTimeIndex(collection, BitemporalSchema.SUPPLIERS);

      boolean first = true;
      for (final BitemporalQueries.Query query : BitemporalQueries.all()) {
        final String text = query.text();
        final long started = System.nanoTime();
        final String serialized = execute(chain, context, text);
        final double seconds = (System.nanoTime() - started) / 1e9;
        final Path raw = output.resolve("q" + query.index() + ".raw.json");
        Files.writeString(raw, serialized, StandardCharsets.UTF_8);
        final BitemporalCanonicalizer.Result result =
            BitemporalCanonicalizer.write(query, serialized, output.resolve("q" + query.index() + ".tsv"));
        final String querySha = sha256(text);
        System.out.printf("SIRIX_QUERY q=%d rows=%d seconds=%.6f sha256=%s route=%s strict_residual=%s%n",
            query.index(), result.rows(), seconds, result.sha256(), query.route(), query.strictEndResidual());
        if (!first) {
          manifest.append(',');
        }
        first = false;
        manifest.append("{\"q\":")
                .append(query.index())
                .append(",\"rows\":")
                .append(result.rows())
                .append(",\"sha256\":\"")
                .append(result.sha256())
                .append("\",\"query_sha256\":\"")
                .append(querySha)
                .append("\",\"route\":\"")
                .append(query.route())
                .append("\",\"strict_end_residual\":")
                .append(query.strictEndResidual())
                .append('}');
      }
    }
    manifest.append("]}\n");
    Files.writeString(output.resolve("manifest.json"), manifest, StandardCharsets.UTF_8);
  }

  private static Path requireCampaignPath(final Path path, final String label) {
    final Path normalized = path.toAbsolutePath().normalize();
    if (!normalized.startsWith(REQUIRED_ROOT)) {
      throw new IllegalArgumentException(label + " must be under " + REQUIRED_ROOT + ": " + normalized);
    }
    return normalized;
  }

  private static void assertValidTimeIndex(final JsonDBCollection collection, final String resource) {
    final JsonDBItem document = collection.getDocument(resource, BitemporalSchema.systemTime(24));
    if (document == null) {
      throw new IllegalStateException("cannot open " + resource + " at E24");
    }
    try {
      boolean found = false;
      for (final IndexDef definition : document.getTrx()
                                               .getResourceSession()
                                               .getRtxIndexController(document.getTrx().getRevisionNumber())
                                               .getIndexes()
                                               .getIndexDefs()) {
        if (definition.isValidTimeIndex()) {
          found = true;
          break;
        }
      }
      if (!found) {
        throw new IllegalStateException("missing persisted VALIDTIME route for " + resource);
      }
    } finally {
      document.getTrx().close();
    }
  }

  private static String execute(final SirixCompileChain chain, final SirixQueryContext context, final String queryText)
      throws IOException {
    final Sequence result = new Query(chain, queryText).execute(context);
    final StringWriter output = new StringWriter(1 << 16);
    try (PrintWriter writer = new PrintWriter(output)) {
      new StringSerializer(writer).serialize(result);
    }
    return output.toString();
  }

  private static String sha256(final String value) throws Exception {
    return HexFormat.of()
                    .formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
  }
}
