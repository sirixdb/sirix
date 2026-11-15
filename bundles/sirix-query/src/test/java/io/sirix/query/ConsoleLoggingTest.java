package io.sirix.query;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.util.LogbackMDCAdapter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Enumeration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ConsoleLoggingTest {
  @TempDir
  Path directory;

  @Test
  void sharedConfigurationsKeepDiagnosticsOffCommandOutput() throws Exception {
    // CLI stdout is a result contract: even enabled diagnostics belong exclusively on stderr.
    // Execute each packaged configuration with Logback rather than inspecting its XML text.
    int configurations = 0;
    for (final String name : new String[] {"logback.xml", "logback-test.xml"}) {
      final Enumeration<URL> resources = getClass().getClassLoader().getResources(name);
      while (resources.hasMoreElements()) {
        final URL resource = resources.nextElement();
        final ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        final ByteArrayOutputStream stderr = new ByteArrayOutputStream();
        final PrintStream previousOut = System.out;
        final PrintStream previousErr = System.err;
        final LoggerContext context = new LoggerContext();
        try (final PrintStream out = new PrintStream(stdout, true, StandardCharsets.UTF_8);
            final PrintStream err = new PrintStream(stderr, true, StandardCharsets.UTF_8)) {
          System.setOut(out);
          System.setErr(err);
          context.setMDCAdapter(new LogbackMDCAdapter());
          context.putProperty("user.home", directory.toString());
          final JoranConfigurator configurator = new JoranConfigurator();
          configurator.setContext(context);
          configurator.doConfigure(resource);
          context.getLogger("io.sirix.access.Databases").setLevel(Level.INFO);
          context.getLogger("io.sirix.access.Databases").warn("allocator-warning-witness");
          context.getLogger("io.sirix.access.Databases").info("effective-config-witness");
        } finally {
          context.stop();
          System.setOut(previousOut);
          System.setErr(previousErr);
        }
        assertEquals("", stdout.toString(StandardCharsets.UTF_8), resource.toString());
        final String diagnostics = stderr.toString(StandardCharsets.UTF_8);
        assertTrue(diagnostics.contains("allocator-warning-witness"), resource.toString());
        assertTrue(diagnostics.contains("effective-config-witness"), resource.toString());
        configurations++;
      }
    }
    assertTrue(configurations >= 4, "exercise the core/query main and test configurations");
  }
}
