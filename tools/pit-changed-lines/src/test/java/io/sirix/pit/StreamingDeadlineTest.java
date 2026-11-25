package io.sirix.pit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.tools.ToolProvider;
import javax.xml.parsers.DocumentBuilderFactory;
import org.hamcrest.Matcher;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.w3c.dom.Element;

/**
 * CONV-202/205: real PIT must persist completed work behind an unfinished first unit. The oracle is
 * the fixture's arithmetic contract, not a callback invoked by the test.
 */
final class StreamingDeadlineTest {
  @TempDir
  private Path directory;

  @Test
  void preservesFinishedUnitsWhenTheLeadingUnitMissesItsDeadline() throws Exception {
    final Process process = startPit();
    try {
      await(directory.resolve("slow"), process);
      await(directory.resolve("fast"), process);
      // The mutant assertion failed and its real PIT worker exited; the leading unit stays held.
      final long worker = Long.parseLong(Files.readString(directory.resolve("fast")));
      ProcessHandle.of(worker).ifPresent(handle -> handle.onExit().orTimeout(20, TimeUnit.SECONDS).join());
      final Path stream = directory.resolve("report").resolve(StreamingResultListener.FILE);
      final long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
      while ((!Files.exists(stream) || Files.size(stream) == 0) && System.nanoTime() < deadline) {
        assertTrue(process.isAlive(), log());
        Thread.sleep(20);
      }
      assertTrue(Files.exists(stream) && Files.size(stream) > 0,
          "completed fast mutant must be durable while the first unit is still blocked\n" + log());
      final List<String> records = Files.readAllLines(stream);
      assertEquals(1, records.size(), "one MATH mutant in Fast, without duplicate reporting");
      final Element mutation =
          DocumentBuilderFactory.newInstance()
                                .newDocumentBuilder()
                                .parse(new ByteArrayInputStream(records.getFirst().getBytes(StandardCharsets.UTF_8)))
                                .getDocumentElement();
      assertEquals("deadline.Fast", mutation.getElementsByTagName("mutatedClass").item(0).getTextContent());
      assertEquals("KILLED", mutation.getAttribute("status"));
      assertTrue(process.isAlive(), "the earlier unit has not finished");
    } finally {
      stop(process);
    }
  }

  @Test
  void completedRunReportsAllFourArithmeticMutantsExactlyOnce() throws Exception {
    Files.writeString(directory.resolve("release"), "released");
    final Process process = startPit();
    try {
      assertTrue(process.waitFor(40, TimeUnit.SECONDS), "PIT must finish with the fixture released");
      assertEquals(0, process.exitValue(), log());
      final List<String> records =
          Files.readAllLines(directory.resolve("report").resolve(StreamingResultListener.FILE));
      assertEquals(4, records.size(), "three Slow additions and one Fast addition under MATH");
      final Set<String> methods = new HashSet<>();
      for (final String record : records) {
        final Element mutation =
            DocumentBuilderFactory.newInstance()
                                  .newDocumentBuilder()
                                  .parse(new ByteArrayInputStream(record.getBytes(StandardCharsets.UTF_8)))
                                  .getDocumentElement();
        assertEquals("KILLED", mutation.getAttribute("status"));
        methods.add(mutation.getElementsByTagName("mutatedClass").item(0).getTextContent() + "."
            + mutation.getElementsByTagName("mutatedMethod").item(0).getTextContent());
      }
      assertEquals(Set.of("deadline.Fast.value", "deadline.Slow.a", "deadline.Slow.b", "deadline.Slow.c"), methods);
      final Element xml = DocumentBuilderFactory.newInstance()
                                                .newDocumentBuilder()
                                                .parse(directory.resolve("report/mutations.xml").toFile())
                                                .getDocumentElement();
      assertEquals(4, xml.getElementsByTagName("mutation").getLength(), "the ordinary XML listener still completes");
    } finally {
      stop(process);
    }
  }

  private Process startPit() throws Exception {
    final Path classes = Files.createDirectories(directory.resolve("classes"));
    final Path source = Files.createDirectories(directory.resolve("source/deadline"));
    Files.writeString(source.resolve("Slow.java"), """
        package deadline;
        public class Slow {
          public int a(int x) { return x + 1; }
          public int b(int x) { return x + 2; }
          public int c(int x) { return x + 3; }
        }
        """);
    Files.writeString(source.resolve("Fast.java"), """
        package deadline;
        public class Fast { public int value(int x) { return x + 1; } }
        """);
    Files.writeString(source.resolve("SlowTest.java"), """
        package deadline;
        import org.junit.Test;
        import java.nio.file.Files;
        import java.nio.file.Path;
        import static org.junit.Assert.assertEquals;
        public class SlowTest {
          @Test public void arithmetic() throws Exception {
            int a = new Slow().a(7), b = new Slow().b(7), c = new Slow().c(7);
            if (a != 8 || b != 9 || c != 10) {
              Files.writeString(Path.of("%s/slow"), "held");
              while (!Files.exists(Path.of("%s/release"))) Thread.sleep(10);
            }
            assertEquals(8, a); assertEquals(9, b); assertEquals(10, c);
          }
        }
        """.formatted(directory, directory));
    Files.writeString(source.resolve("FastTest.java"), """
        package deadline;
        import org.junit.Test;
        import java.nio.file.Files;
        import java.nio.file.Path;
        import static org.junit.Assert.assertEquals;
        public class FastTest {
          @Test public void arithmetic() throws Exception {
            int actual = new Fast().value(7);
            if (actual != 8) Files.writeString(Path.of("%s/fast"), "" + ProcessHandle.current().pid());
            assertEquals(8, actual);
          }
        }
        """.formatted(directory));
    final String classpath = System.getProperty("pit.regression.classpath");
    final List<String> compile = new ArrayList<>(List.of("-cp", classpath, "-d", classes.toString()));
    for (final String name : List.of("Slow", "Fast", "SlowTest", "FastTest")) {
      compile.add(source.resolve(name + ".java").toString());
    }
    assertEquals(0, ToolProvider.getSystemJavaCompiler().run(null, null, null, compile.toArray(String[]::new)));
    final List<String> command =
        new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx256m"));
    command.add("-javaagent:" + System.getProperty("pit.regression.agent"));
    command.addAll(List.of("-cp", classpath, "org.pitest.mutationtest.commandline.MutationCoverageReport",
        "--reportDir", directory.resolve("report").toString(), "--sourceDirs",
        Objects.requireNonNull(source.getParent(), "fixture source directory must have a parent").toString(),
        "--targetClasses", "deadline.Fast,deadline.Slow", "--targetTests", "deadline.*Test", "--classPath",
        classes + "," + Path.of(Assert.class.getProtectionDomain().getCodeSource().getLocation().toURI()) + ","
            + Path.of(Matcher.class.getProtectionDomain().getCodeSource().getLocation().toURI()),
        "--mutators", "MATH", "--threads", "2", "--timeoutConst", "120000", "--outputFormats", "XML,STREAM",
        "--timestampedReports", "false", "--includeLaunchClasspath", "false"));
    return new ProcessBuilder(command).redirectErrorStream(true)
                                      .redirectOutput(directory.resolve("pit.log").toFile())
                                      .start();
  }

  private void await(final Path receipt, final Process process) throws Exception {
    final long deadline = System.nanoTime() + Duration.ofSeconds(40).toNanos();
    while (!Files.exists(receipt) && process.isAlive() && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertTrue(Files.exists(receipt), "real PIT did not reach fixture checkpoint " + receipt + "\n" + log());
  }

  private String log() throws Exception {
    return Files.readString(directory.resolve("pit.log"));
  }

  private static void stop(final Process process) throws Exception {
    final List<ProcessHandle> children = process.descendants().toList();
    for (final ProcessHandle child : children) {
      child.destroyForcibly();
    }
    process.destroyForcibly();
    assertTrue(process.waitFor(10, TimeUnit.SECONDS));
    for (final ProcessHandle child : children) {
      child.onExit().orTimeout(10, TimeUnit.SECONDS).join();
      assertFalse(child.isAlive(), "PIT worker must stop before the regression returns");
    }
  }
}
