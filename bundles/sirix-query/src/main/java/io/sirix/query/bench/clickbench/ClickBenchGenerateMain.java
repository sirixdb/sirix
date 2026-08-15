package io.sirix.query.bench.clickbench;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPOutputStream;

/**
 * Writes the synthetic ClickBench {@code hits} dataset to a file, so the reference engine can be
 * pointed at byte-identical data to the one SirixDB ingested — which is what makes the differential
 * check ({@code bench/clickbench/compare-results.py}) meaningful.
 *
 * <pre>
 *   ClickBenchGenerateMain &lt;out.json[.gz]&gt; &lt;rows&gt; [seed]
 * </pre>
 */
public final class ClickBenchGenerateMain {

  private static final int BUFFER_CHARS = 1 << 20;

  private ClickBenchGenerateMain() {
    throw new AssertionError("no instances");
  }

  public static void main(final String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: ClickBenchGenerateMain <out.json[.gz]> <rows> [seed]");
      System.exit(2);
      return;
    }
    final Path out = Path.of(args[0]);
    final long rows = Long.parseLong(args[1]);
    if (rows <= 0) {
      throw new IllegalArgumentException("row count must be positive: " + rows);
    }
    final long seed = args.length > 2 ? Long.parseLong(args[2]) : 42L;
    final Path parent = out.toAbsolutePath().getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }

    final long start = System.nanoTime();
    final char[] buffer = new char[BUFFER_CHARS];
    long chars = 0;
    try (OutputStream fileOut = Files.newOutputStream(out);
         OutputStream sink = out.getFileName().toString().endsWith(".gz")
             ? new GZIPOutputStream(fileOut, 1 << 16)
             : fileOut;
         Writer writer = new BufferedWriter(new OutputStreamWriter(sink, StandardCharsets.UTF_8), BUFFER_CHARS);
         Reader source = new ClickBenchHitsGenerator(0L, rows, seed)) {
      int read;
      while ((read = source.read(buffer, 0, buffer.length)) != -1) {
        writer.write(buffer, 0, read);
        chars += read;
      }
    }
    final double seconds = (System.nanoTime() - start) / 1e9;
    System.out.printf("# generated %,d rows (%,d chars, %,d bytes on disk) in %.3f s (%,.0f rows/s)%n",
                      rows, chars, Files.size(out), seconds, rows / Math.max(seconds, 1e-9));
  }
}
