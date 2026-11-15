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
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.AttributedString;
import org.mockito.MockedStatic;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.RETURNS_SELF;
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
  void accumulatedLinesDetermineTheNextLinesHighlighting() {
    final ShellHighlighter highlighter = new ShellHighlighter(ShellPalette.ANSI_16);
    final LineReader reader = mock(LineReader.class);
    when(reader.readLine(anyString())).thenReturn("(: open comment").thenAnswer(invocation -> {
      final var highlighted = highlighter.highlight(reader, "still a comment :)");
      assertEquals(AttributedString.fromAnsi("\033[2mX").styleAt(0), highlighted.styleAt(0));
      return "still a comment :)";
    }).thenReturn("");
    assertEquals("(: open comment" + System.lineSeparator() + "still a comment :)",
        Main.readStringFromScannerWithEndMark(reader, "sirix> ", highlighter));
    // A new query must clear the previous query's lexical context as well.
    final LineReader next = mock(LineReader.class);
    when(next.readLine(anyString())).thenAnswer(invocation -> {
      assertEquals(AttributedString.fromAnsi("\033[33mX").styleAt(0), highlighter.highlight(next, "1").styleAt(0));
      return "1";
    }).thenReturn("");
    assertEquals("1", Main.readStringFromScannerWithEndMark(next, "sirix> ", highlighter));
  }

  @Test
  void cliQueryFileAndStdinDispatchProduceTheSamePayloadInProcess() throws Exception {
    final String source = "{'name':'商店😀','n':12}";
    final Path queryFile = directory.resolve("query with spaces.xq");
    Files.writeString(queryFile, source);
    for (final String[] args : new String[][] {{"-q", source}, {"-q", source, "--no-color"},
        {"-qf", queryFile.toString()}, {"-qf", "-"}, {}}) {
      final String[] output = invoke(source, args);
      final String nl = System.lineSeparator();
      assertEquals(nl + "Query result" + nl + "{\"name\":\"商店😀\",\"n\":12}" + nl + nl, output[0]);
      assertEquals("", output[1]);
    }
    final String[] pretty = invoke("", "-q", "{'x':{'n':1}}", "-p");
    assertTrue(pretty[0].contains(System.lineSeparator() + "  "), pretty[0]);
    assertEquals("", pretty[1]);
    final String[] empty = invoke("", "-q", " ");
    assertEquals("", empty[0]);
    assertEquals("Error: Query cannot be empty." + System.lineSeparator(), empty[1]);
  }

  @Test
  void interactiveQueriesRenderRecoverFromErrorsAndCloseTheirTerminal() throws Exception {
    final LineReader reader = mock(LineReader.class);
    when(reader.readLine(anyString()))
                                      .thenReturn("jn:store('shell-ci','products','[1]')", "", "1+1", "", "1+", "", "3",
                                          "")
                                      .thenThrow(new EndOfFileException());
    final Terminal terminal = mock(Terminal.class);
    final TerminalBuilder terminals = mock(TerminalBuilder.class, RETURNS_SELF);
    when(terminals.build()).thenReturn(terminal);
    final LineReaderBuilder readers = mock(LineReaderBuilder.class, RETURNS_SELF);
    when(readers.build()).thenReturn(reader);
    final Logger logger = (Logger) LoggerFactory.getLogger(Main.class);
    final ListAppender<ILoggingEvent> diagnostics = new ListAppender<>();
    diagnostics.start();
    logger.addAppender(diagnostics);
    try (final MockedStatic<TerminalBuilder> terminalFactory = mockStatic(TerminalBuilder.class);
        final MockedStatic<LineReaderBuilder> readerFactory = mockStatic(LineReaderBuilder.class)) {
      terminalFactory.when(TerminalBuilder::builder).thenReturn(terminals);
      readerFactory.when(LineReaderBuilder::builder).thenReturn(readers);
      final String[] output = invoke("", "-iq");
      final String nl = System.lineSeparator();
      final String banner = "Enter query string (terminate with Control-D):" + nl;
      assertEquals(banner + nl + "Query result" + nl + nl + nl + banner + nl + "Query result" + nl + "2" + nl + nl
          + banner + banner + nl + "Query result" + nl + "3" + nl + nl + banner, output[0]);
      assertTrue(output[1].startsWith("Error: err:XPST0003:"), output[1]);
      assertTrue(diagnostics.list.isEmpty());
      verify(reader).readLine("sirix shell-ci/products@1> ");
      verify(readers).terminal(terminal);
      verify(terminal).close();
      final LineReader debug = mock(LineReader.class);
      when(debug.readLine(anyString())).thenReturn("1+", "").thenThrow(new EndOfFileException());
      when(readers.build()).thenReturn(debug);
      final String[] debugOutput = invoke("", "-iq", "-d");
      assertEquals(banner + banner, debugOutput[0]);
      assertTrue(debugOutput[1].startsWith("Error: err:XPST0003:"), debugOutput[1]);
      assertEquals(1, diagnostics.list.size());
      assertEquals("Query execution failed", diagnostics.list.getFirst().getFormattedMessage());
      assertEquals("io.brackit.query.QueryException", diagnostics.list.getFirst().getThrowableProxy().getClassName());
    } finally {
      logger.detachAppender(diagnostics);
      diagnostics.stop();
    }
  }

  private static String[] invoke(final String input, final String... arguments) {
    final InputStream previousIn = System.in;
    final PrintStream previousOut = System.out;
    final PrintStream previousErr = System.err;
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final ByteArrayOutputStream error = new ByteArrayOutputStream();
    try (final PrintStream out = new PrintStream(output, true, StandardCharsets.UTF_8);
        final PrintStream err = new PrintStream(error, true, StandardCharsets.UTF_8)) {
      System.setIn(new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)));
      System.setOut(out);
      System.setErr(err);
      Main.main(arguments);
      return new String[] {output.toString(StandardCharsets.UTF_8), error.toString(StandardCharsets.UTF_8)};
    } finally {
      System.setIn(previousIn);
      System.setOut(previousOut);
      System.setErr(previousErr);
    }
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
    final Path query = directory.resolve("Unicode query.xq");
    Files.writeString(query, "{'name':'商店😀','n':12}", StandardCharsets.UTF_8);
    for (int mode = 0; mode < 3; mode++) {
      final List<String> arguments =
          new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
              "--enable-preview", "--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED",
              "-Xmx128m", "-Dstdout.encoding=UTF-8", "-Dstderr.encoding=UTF-8", "-Duser.home=" + directory,
              "-Dlogback.configurationFile=" + logging, "-cp", System.getProperty("java.class.path"),
              Main.class.getName(), "-qf", query.toString()));
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
        assertTrue(process.waitFor(60, TimeUnit.SECONDS), Files.readString(directory.resolve("err" + mode)));
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
