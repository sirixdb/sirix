package io.sirix.query.function.xml.diff;

import io.brackit.query.node.parser.DocumentParser;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.query.Main;
import io.sirix.query.node.BasicXmlDBStore;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class DiffShellLifecycleTest {
  @TempDir
  Path directory;

  @ParameterizedTest(name = "changed={0}")
  @ValueSource(booleans = {false, true})
  void oneShotShellExitsAfterRepeatedDiffs(final boolean changed) throws Exception {
    try (final var store = BasicXmlDBStore.newBuilder().location(directory).build()) {
      final var collection =
          store.create("diff", new DocumentParser("<root xml:lang='en'><item>1</item></root>"));
      final var session = collection.getDocument("resource1").getTrx().getResourceSession();
      try (final XmlNodeTrx writer = session.beginNodeTrx()) {
        if (changed) {
          assertTrue(writer.moveToFirstChild());
          assertTrue(writer.moveToFirstChild());
          assertTrue(writer.moveToFirstChild());
          writer.setValue("2");
        }
        writer.commit();
      }
      assertEquals(2, session.getMostRecentRevisionNumber());
    }

    final String query = "let $first := xn:diff('diff','resource1',1,2) "
        + "let $second := xn:diff('diff','resource1',1,2) "
        + "return if (deep-equal($first, $second)) then ($first, 'DIFF_COMPLETED') else error()";
    final Path output = directory.resolve("shell-output.txt");
    final String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    final Process process = new ProcessBuilder(java, "-Xmx512m", "--add-modules", "jdk.incubator.vector",
        "--enable-native-access=ALL-UNNAMED", "-Duser.home=" + directory, "-DdbLocation=" + directory,
        "-Djava.io.tmpdir=" + directory, "-cp", System.getProperty("java.class.path"), Main.class.getName(), "-q",
        query).redirectErrorStream(true).redirectOutput(output.toFile()).start();
    try {
      // This bounds a leaked-process wait, rather than measuring query performance.
      final boolean exited = process.waitFor(30, TimeUnit.SECONDS);
      final String transcript = Files.readString(output);
      assertTrue(transcript.contains("DIFF_COMPLETED"), transcript);
      if (changed) {
        assertTrue(transcript.contains("xn:doc('diff','resource1', 1)"), transcript);
        assertTrue(transcript.contains("replace value"), transcript);
      }
      assertTrue(exited, "Shell completed xn:diff but did not exit:\n" + transcript);
      assertEquals(0, process.exitValue(), transcript);
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
      }
      process.waitFor();
    }
  }
}
