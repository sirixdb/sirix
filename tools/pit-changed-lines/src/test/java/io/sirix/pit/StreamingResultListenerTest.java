package io.sirix.pit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pitest.classinfo.ClassName;
import org.pitest.mutationtest.ClassMutationResults;
import org.pitest.mutationtest.DetectionStatus;
import org.pitest.mutationtest.MutationResult;
import org.pitest.mutationtest.MutationStatusTestPair;
import org.pitest.mutationtest.engine.Location;
import org.pitest.mutationtest.engine.MutationDetails;
import org.pitest.mutationtest.engine.MutationIdentifier;

/**
 * CONV-202's partial results: a module killed by its deadline must report every status recorded
 * before the kill, which requires a stream flushed as each analysed class finishes rather than a
 * report serialised at the end of the run.
 */
final class StreamingResultListenerTest {

  private static final String MUTATOR =
      "org.pitest.mutationtest.engine.gregor.mutators.ReturnValsMutator";

  @TempDir
  private Path directory;

  private static MutationResult result(final int line, final DetectionStatus status,
      final int tests, final String killingTest, final String description) {
    final MutationDetails details = new MutationDetails(
        new MutationIdentifier(
            Location.location(ClassName.fromString("io.sirix.api.NodeTrx"), "commit", "()V"),
            List.of(3, 4), MUTATOR),
        "NodeTrx.java", description, line, 0);
    return new MutationResult(details,
        killingTest == null
            ? new MutationStatusTestPair(tests, status, List.of(), List.of(), List.of())
            : new MutationStatusTestPair(tests, status, List.of(killingTest), List.of(),
                List.of()));
  }

  @Test
  void writesOnePitShapedElementPerMutant() {
    final StringWriter target = new StringWriter();
    final StreamingResultListener listener = new StreamingResultListener(target);

    listener.runStart();
    listener.handleMutationResult(new ClassMutationResults(
        List.of(result(24, DetectionStatus.KILLED, 2, "io.sirix.ATest#works", "replaced it"))));

    assertEquals("<mutation detected='true' status='KILLED' numberOfTestsRun='2'>"
        + "<sourceFile>NodeTrx.java</sourceFile>"
        + "<mutatedClass>io.sirix.api.NodeTrx</mutatedClass>"
        + "<mutatedMethod>commit</mutatedMethod><methodDescription>()V</methodDescription>"
        + "<lineNumber>24</lineNumber><mutator>" + MUTATOR + "</mutator>"
        + "<indexes><index>3</index><index>4</index></indexes>"
        + "<killingTest>io.sirix.ATest#works</killingTest>"
        + "<description>replaced it</description></mutation>\n", target.toString());
  }

  @Test
  void reportsAnUndetectedMutantAsUndetectedWithoutAKillingTest() {
    final StringWriter target = new StringWriter();
    final StreamingResultListener listener = new StreamingResultListener(target);

    listener.handleMutationResult(new ClassMutationResults(
        List.of(result(36, DetectionStatus.SURVIVED, 332, null, "replaced it"))));

    final String record = target.toString();
    assertTrue(record.startsWith("<mutation detected='false' status='SURVIVED' "
        + "numberOfTestsRun='332'>"), record);
    assertTrue(record.contains("<killingTest></killingTest>"), record);
  }

  @Test
  void keepsOneRecordPerLineThroughEscaping() {
    final StringWriter target = new StringWriter();
    final StreamingResultListener listener = new StreamingResultListener(target);

    listener.handleMutationResult(new ClassMutationResults(List.of(
        result(24, DetectionStatus.NO_COVERAGE, 0, null, "replaced <a> & 'b' \"c\"\nand more"))));

    final List<String> records = target.toString().lines().toList();
    assertEquals(1, records.size(), "an embedded newline must not split the record");
    assertTrue(records.get(0).contains(
        "<description>replaced &lt;a&gt; &amp; &apos;b&apos; &quot;c&quot; and more</description>"),
        records.get(0));
  }

  @Test
  void flushesEveryClassSoAKilledRunKeepsWhatFinished() throws IOException {
    final Path path = directory.resolve(StreamingResultListener.FILE);
    try (Writer writer = Files.newBufferedWriter(path)) {
      final StreamingResultListener listener = new StreamingResultListener(writer);

      listener.runStart();
      listener.handleMutationResult(new ClassMutationResults(
          List.of(result(24, DetectionStatus.KILLED, 1, "io.sirix.ATest#works", "first"))));
      assertEquals(1, Files.readAllLines(path).size(),
          "the first class is on disk before the run ends");

      listener.handleMutationResult(new ClassMutationResults(
          List.of(result(36, DetectionStatus.SURVIVED, 9, null, "second"),
              result(37, DetectionStatus.TIMED_OUT, 3, null, "third"))));
      assertEquals(3, Files.readAllLines(path).size(),
          "every analysed class appends its own results");
    }
  }

  @Test
  void registersItselfAsAnOutputFormatNamedStream() {
    final StreamingResultListenerFactory factory = new StreamingResultListenerFactory();

    assertEquals("STREAM", factory.name());
    assertTrue(factory.description().contains("killed run"), factory.description());
  }
}
