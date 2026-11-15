package io.sirix.query;

import org.junit.jupiter.api.Test;
import io.sirix.query.shell.ShellHighlighter;
import io.sirix.query.shell.ShellPalette;
import org.jline.reader.LineReader;
import org.jline.reader.EndOfFileException;
import org.jline.reader.UserInterruptException;
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.ArgumentMatchers.anyString;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class MainTest {
  @TempDir
  Path directory;

  @Test
  void completeQueriesDoNotShowAMisleadingContinuationPrompt() {
    final LineReader reader = mock(LineReader.class);
    when(reader.readLine(anyString())).thenReturn("1+1", "");
    assertEquals("1+1",
        Main.readStringFromScannerWithEndMark(reader, "sirix> ", new ShellHighlighter(ShellPalette.NONE)));
    verify(reader).readLine("sirix> ");
    verify(reader).readLine("");
  }

  @Test
  void multilineBlankLineExecutionAndEndMarksArePreserved() {
    final LineReader reader = mock(LineReader.class);
    when(reader.readLine(anyString())).thenReturn("let $x := 1", "return $x", "");
    assertEquals("let $x := 1" + System.lineSeparator() + "return $x",
        Main.readStringFromScannerWithEndMark(reader, "sirix> ", new ShellHighlighter(ShellPalette.NONE)));
    verify(reader, times(2)).readLine("");
    final LineReader eof = mock(LineReader.class);
    when(eof.readLine(anyString())).thenReturn("1").thenThrow(new EndOfFileException());
    assertEquals("1", Main.readStringFromScannerWithEndMark(eof, "sirix> ", new ShellHighlighter(ShellPalette.NONE)));
    final LineReader empty = mock(LineReader.class);
    when(empty.readLine(anyString())).thenThrow(new EndOfFileException());
    assertNull(Main.readStringFromScannerWithEndMark(empty, "sirix> ", new ShellHighlighter(ShellPalette.NONE)));
    final LineReader interrupted = mock(LineReader.class);
    when(interrupted.readLine(anyString())).thenThrow(new UserInterruptException(""));
    assertNull(Main.readStringFromScannerWithEndMark(interrupted, "sirix> ", new ShellHighlighter(ShellPalette.NONE)));
  }

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
