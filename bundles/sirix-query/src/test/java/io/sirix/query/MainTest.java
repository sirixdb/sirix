package io.sirix.query;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class MainTest {
  @TempDir
  Path directory;

  @Test
  void outputAdapterPreservesExplicitStreamEncodings() {
    final String text = "é 商店";
    for (final Charset charset : List.of(StandardCharsets.UTF_8, StandardCharsets.UTF_16BE,
        Charset.forName("windows-1252"))) {
      final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      final PrintStream stream = new PrintStream(bytes, false, charset);
      final var writer = Main.outputWriter(stream);
      writer.print(text);
      writer.flush();
      assertArrayEquals(text.getBytes(charset), bytes.toByteArray());
    }
  }

  @Test
  void redirectedOutputIsGoldenRegardlessOfColourEnvironment() throws Exception {
    final Path logging = directory.resolve("logback.xml");
    Files.writeString(logging, "<configuration><root level='OFF'/></configuration>");
    for (int mode = 0; mode < 3; mode++) {
      final List<String> arguments =
          new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
              "--enable-preview", "--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED",
              "-Xmx512m", "-Duser.home=" + directory, "-Dlogback.configurationFile=" + logging, "-cp",
              System.getProperty("java.class.path"), Main.class.getName(), "-q", "{\"name\":\"商店😀\",\"n\":12}"));
      if (mode == 1)
        arguments.add("--no-color");
      final Path output = directory.resolve("out" + mode);
      final ProcessBuilder builder =
          new ProcessBuilder(arguments).redirectOutput(output.toFile())
                                       .redirectError(directory.resolve("err" + mode).toFile());
      builder.environment().put("COLORTERM", "truecolor");
      builder.environment().put("TERM", "xterm-256color");
      if (mode == 2)
        builder.environment().put("NO_COLOR", "");
      final Process process = builder.start();
      try {
        assertTrue(process.waitFor(60, TimeUnit.SECONDS));
        assertEquals(0, process.exitValue(), Files.readString(directory.resolve("err" + mode)));
        final String nl = System.lineSeparator();
        assertEquals(nl + "Query result" + nl + "{\"name\":\"商店😀\",\"n\":12}" + nl + nl, Files.readString(output));
      } finally {
        if (process.isAlive())
          process.destroyForcibly();
        process.waitFor();
      }
    }
  }
}
