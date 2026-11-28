/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.scan;

import io.sirix.query.scan.QryFixture.Outcome;
import io.sirix.query.scan.QrySeam.Counters;
import io.sirix.query.scan.QrySeam.RouteOutcome;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QryPlanned(step = QrySteps.ROUTE, value = "QRY-601")
public final class RegionPredicatedAggregateStartupTest {

  @TempDir
  Path directory;

  @ParameterizedTest(name = "route={0}, arrayContainsColumnar={1}")
  @CsvSource({"unset,false", "unset,true", "false,false", "false,true", "true,false", "true,true"})
  @DisplayName("QRY-601/D2: pre-evidence default is off; startup switches are independent")
  void startupSwitchesAreIndependent(final String route, final boolean arrayContains) throws Exception {
    final List<String> command =
        new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "--enable-preview",
            "--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED", "-Xmx512m",
            "-Duser.home=" + directory, "-Djava.io.tmpdir=" + directory, "-Dbrackit.predicate.arrayContains=true",
            "-Dsirix.scan.arrayContainsColumnar=" + arrayContains));
    if (!route.equals("unset")) {
      command.add("-D" + QrySeam.SWITCH_PROPERTY + "=" + route);
    }
    command.addAll(List.of("-cp", System.getProperty("sirix.qry.testClasspath"),
        RegionPredicatedAggregateStartupTest.class.getName(), directory.toString(), route));
    final Path output = directory.resolve("startup.log");
    final ProcessBuilder builder =
        new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(output.toFile());
    builder.environment().remove("JAVA_TOOL_OPTIONS");
    builder.environment().remove("JDK_JAVA_OPTIONS");
    builder.environment().remove("_JAVA_OPTIONS");
    final Process process = builder.start();
    try {
      assertTrue(process.waitFor(60, TimeUnit.SECONDS), "startup probe did not terminate");
      assertEquals(0, process.exitValue(), () -> readOutput(output));
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        process.waitFor();
      }
    }
  }

  private static String readOutput(final Path output) {
    try {
      return Files.readString(output);
    } catch (final Exception e) {
      throw new AssertionError(e);
    }
  }

  public static void main(final String[] args) throws Exception {
    final QryFixture fixture = new QryFixture(Path.of(args[0]), "qry-startup-db");
    try {
      fixture.store("lemire.jn", QryCorpus.array(QryCorpus.lemireRecords(1_500, 0x601L)));
      for (final String query : List.of(QryCorpus.adminSum(fixture.src("lemire.jn")),
          QryCorpus.nestedMembershipCount(fixture.src("lemire.jn")))) {
        final Outcome expected = fixture.interpreter(query);
        final Outcome[] actual = new Outcome[1];
        final Counters counters =
            QrySeam.around(() -> actual[0] = fixture.routeWithStartupSwitch("lemire.jn", -1, query, 1));
        assertEquals(expected, actual[0], counters.toString());
        if (args[1].equals("true")) {
          assertEquals(RouteOutcome.SERVED, counters.outcome(), counters.toString());
        } else {
          assertEquals(0, counters.served(), counters.toString());
          assertEquals(0, counters.declined(), counters.toString());
          assertEquals(0, counters.seamRecords(), counters.toString());
          assertEquals(0, counters.refused(), counters.toString());
          assertEquals(0, counters.columnReads(), counters.toString());
          assertTrue(counters.recordPageReads() > 0, "the disabled route must use the record path: " + counters);
        }
      }
    } finally {
      fixture.drop();
    }
  }
}
