package io.sirix.benchmark;

import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;

public final class HotInMemoryReadBenchmarkSmoke {
  private HotInMemoryReadBenchmarkSmoke() {}

  public static void main(final String[] args) throws Exception {
    final Path corpus = Files.createTempFile("hot-one-title-", ".json");
    final String previousCorpus = System.getProperty("sirix.bench.corpus");
    try {
      Files.writeString(corpus, "[{\"title\":\"Movie\"}]");
      System.setProperty("sirix.bench.corpus", corpus.toString());
      final Options options = new OptionsBuilder().include(
          "^io.sirix.benchmark.HotInMemoryReadBenchmark\\.(serializeKey|pointGet|pointGetHotKeys|descendToLeaf|lowerBoundSeek|findEntryInLeaf)$")
                                                  .forks(0)
                                                  .warmupIterations(0)
                                                  .measurementIterations(1)
                                                  .measurementTime(TimeValue.milliseconds(10))
                                                  .shouldFailOnError(true)
                                                  .build();
      final Collection<RunResult> results = new Runner(options).run();
      if (results.size() != 6) {
        throw new AssertionError(
            "Expected all six stored-key stages to run on a one-title corpus, got " + results.size());
      }
    } finally {
      if (previousCorpus == null) {
        System.clearProperty("sirix.bench.corpus");
      } else {
        System.setProperty("sirix.bench.corpus", previousCorpus);
      }
      Files.deleteIfExists(corpus);
    }
  }
}
