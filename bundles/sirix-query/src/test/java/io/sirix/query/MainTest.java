package io.sirix.query;

import org.junit.jupiter.api.Test;
import io.brackit.query.ErrorCode;
import io.brackit.query.Query;
import io.brackit.query.jdm.Iter;
import io.brackit.query.QueryException;
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
import java.io.Console;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.never;
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
    final LineReader interruptedQuery = mock(LineReader.class);
    when(interruptedQuery.readLine(anyString())).thenReturn("1+1").thenThrow(new UserInterruptException(""));
    assertNull(Main.readStringFromScannerWithEndMark(interruptedQuery, "sirix> ",
        new ShellHighlighter(ShellPalette.NONE)));
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
  void consoleStateControlsSystemTerminalRoutingAndLifetime() throws Exception {
    final Console console = mock(Console.class);
    final Terminal terminal = mock(Terminal.class);
    final StringWriter output = new StringWriter();
    when(terminal.writer()).thenReturn(new PrintWriter(output));
    final TerminalBuilder terminals = mock(TerminalBuilder.class, RETURNS_SELF);
    when(terminals.build()).thenReturn(terminal);
    try (final MockedStatic<TerminalBuilder> factory = mockStatic(TerminalBuilder.class)) {
      factory.when(TerminalBuilder::builder).thenReturn(terminals);
      final String[] args = {"-q", "1", "--no-color"};
      when(console.isTerminal()).thenReturn(false);
      assertEquals(0, Main.run(args, console));
      verify(terminals, never()).build();
      assertEquals("", output.toString());
      when(console.isTerminal()).thenReturn(true);
      assertEquals(0, Main.run(args, console));
      final String nl = System.lineSeparator();
      assertEquals(nl + "Query result" + nl + "1" + nl + nl, output.toString());
      verify(terminals).system(true);
      verify(terminals).build();
      verify(terminal).close();
      final LineReader reader = mock(LineReader.class);
      when(reader.readLine(anyString())).thenThrow(new EndOfFileException());
      final LineReaderBuilder readers = mock(LineReaderBuilder.class, RETURNS_SELF);
      when(readers.build()).thenReturn(reader);
      try (final MockedStatic<LineReaderBuilder> readerFactory = mockStatic(LineReaderBuilder.class)) {
        readerFactory.when(LineReaderBuilder::builder).thenReturn(readers);
        assertEquals(0, Main.run(new String[] {"--no-color"}, console));
        verify(reader).readLine("sirix> ");
        verify(readers).terminal(terminal);
        verify(terminal, times(2)).close();
      }
    }
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

  @Test
  void interactiveShellReturnsToThePromptAfterAnyQueryFailureWithoutApplyingItsUpdates() throws Exception {
    final String document = "jn:doc('shell-failure','data')";
    final LineReader reader = mock(LineReader.class);
    when(reader.readLine(anyString())).thenReturn("jn:store('shell-failure','data','{\"a\":1}')", "",
        // A store failure (not a QueryException) after an update was already queued.
        "for $n in ('data','missing') return insert json {'x':1} into jn:doc('shell-failure',$n)", "",
        // A dynamic query error after an update was already queued.
        "for $i in (1,0) return insert json {'y': 1 idiv $i} into " + document, "",
        "insert json {'c':3} into " + document, "", document, "").thenThrow(new EndOfFileException());
    final TerminalBuilder terminals = mock(TerminalBuilder.class, RETURNS_SELF);
    when(terminals.build()).thenReturn(mock(Terminal.class));
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
      final String[] output = capture("", () -> assertEquals(0, Main.run(new String[] {"-iq", "-d"}, null)));
      final String nl = System.lineSeparator();
      final String banner = "Enter query string (terminate with Control-D):" + nl;
      // Every query got a prompt and the session ended only at end of input.
      assertEquals(6, occurrences(output[0], banner), output[0]);
      // Neither failed query's queued insert reached the database.
      assertTrue(output[0].endsWith(nl + "Query result" + nl + "{\"a\":1,\"c\":3}" + nl + nl + banner), output[0]);
      // With -d the logger also writes stack traces to stderr; the reports are the "Error:" lines.
      final List<String> errors = output[1].lines().filter(line -> line.startsWith("Error:")).toList();
      assertEquals(2, errors.size(), output[1]);
      assertTrue(errors.get(0).startsWith("Error: bit:BIDY0300: Resource could not be opened"), errors.get(0));
      assertTrue(errors.get(1).startsWith("Error: err:FOAR0001"), errors.get(1));
      assertEquals(2, diagnostics.list.size());
      assertEquals("io.sirix.exception.SirixUsageException",
          diagnostics.list.getFirst().getThrowableProxy().getCause().getClassName());
    } finally {
      logger.detachAppender(diagnostics);
      diagnostics.stop();
    }
  }

  @Test
  void nonQueryFailuresAreReportedUnderTheInternalErrorCodeWithAnIdentifyingMessage() {
    final QueryException query = new QueryException(ErrorCode.ERR_DIVISION_BY_ZERO);
    assertSame(query, Main.asQueryException(query));
    for (final String[] failure : new String[][] {{"resource is missing ", "resource is missing"},
        {null, "java.lang.IllegalStateException"}, {" ", "java.lang.IllegalStateException"}}) {
      final IllegalStateException cause = new IllegalStateException(failure[0]);
      final QueryException reported = Main.asQueryException(cause);
      assertEquals(ErrorCode.BIT_DYN_INT_ERROR, reported.getCode());
      assertEquals("bit:BIDY0300: " + failure[1], reported.getMessage());
      assertSame(cause, reported.getCause());
    }
  }

  @Test
  void queryScriptsRunEveryBlankLineSeparatedQueryInOneSessionAndReportFailuresInTheStatus() throws Exception {
    // Initialize logging before stdout is captured, so its one-time status output stays out of it.
    LoggerFactory.getLogger(Main.class);
    final String document = "jn:doc('script-session','data')";
    final Path script = directory.resolve("session script.xq");
    // CRLF line ends, a multi-line query, repeated and whitespace-only separators, no final blank line.
    Files.writeString(script,
        String.join("\r\n", "jn:store('script-session','data','{\"a\":1}')", "", "",
            "for $i in (1,0) return insert json {'y': 1 idiv $i} into " + document, "  ",
            "insert json {'c':3} into " + document, "", "let $d := " + document, "return $d.c"));
    final String[] failed = invoke(-1, "", "-iqf", script.toString(), "--no-color");
    final String nl = System.lineSeparator();
    assertEquals(nl + "Query result" + nl + nl + nl + nl + "Query result" + nl + nl + "Query result" + nl + nl + nl + nl
        + "Query result" + nl + "3" + nl + nl, failed[0]);
    assertEquals(1, failed[1].lines().count(), failed[1]);
    assertTrue(failed[1].startsWith("Error: err:FOAR0001"), failed[1]);
    // The failed query's queued insert was discarded; stdin works with "-" and success exits with 0.
    // A query's lines are joined as written: a string literal keeps its line break.
    final String[] succeeded = invoke("1+1" + nl + nl + document + nl + nl + "'first" + nl + "second'", "-iqf", "-");
    assertEquals(nl + "Query result" + nl + "2" + nl + nl + nl + "Query result" + nl + "{\"a\":1,\"c\":3}" + nl + nl
        + nl + "Query result" + nl + "first\nsecond" + nl + nl, succeeded[0]);
    assertEquals("", succeeded[1]);
    final Path missing = directory.resolve("missing script.xq");
    final String[] unreadable = invoke(-1, "", "-iqf", missing.toString());
    assertEquals("", unreadable[0]);
    assertEquals("I/O Error: " + missing + nl, unreadable[1]);
  }

  @Test
  void emptySubmissionsKeepBothInteractiveModesAliveUntilEof() throws Exception {
    for (final boolean defaultMode : new boolean[] {false, true}) {
      final LineReader reader = mock(LineReader.class);
      when(reader.readLine(anyString())).thenReturn("", "", "1+1", "", "")
                                      .thenThrow(new EndOfFileException());
      final Terminal terminal = mock(Terminal.class);
      when(terminal.writer()).thenAnswer(invocation -> Main.outputWriter(System.out));
      final TerminalBuilder terminals = mock(TerminalBuilder.class, RETURNS_SELF);
      when(terminals.build()).thenReturn(terminal);
      final LineReaderBuilder readers = mock(LineReaderBuilder.class, RETURNS_SELF);
      when(readers.build()).thenReturn(reader);
      final Console console = mock(Console.class);
      when(console.isTerminal()).thenReturn(true);
      try (final MockedStatic<TerminalBuilder> terminalFactory = mockStatic(TerminalBuilder.class);
          final MockedStatic<LineReaderBuilder> readerFactory = mockStatic(LineReaderBuilder.class)) {
        terminalFactory.when(TerminalBuilder::builder).thenReturn(terminals);
        readerFactory.when(LineReaderBuilder::builder).thenReturn(readers);
        final String[] output = capture("", () -> assertEquals(0, Main.run(defaultMode
            ? new String[] {"--no-color"}
            : new String[] {"-iq", "--no-color"}, defaultMode
                ? console
                : null)));
        final String nl = System.lineSeparator();
        final String banner = "Enter query string (terminate with Control-D):" + nl;
        assertEquals(banner + nl + "Query result" + nl + "2" + nl + nl + banner, output[0]);
        assertEquals("", output[1]);
        verify(reader, times(6)).readLine(anyString());
      }
    }
  }

  @Test
  void queryScriptsUseLfInsideLiteralsForFilesAndStdin() throws Exception {
    final String nl = System.lineSeparator();
    for (final String ending : List.of("\n", "\r\n")) {
      final String source = String.join(ending, "'first", "  second'", "", "\"third", "fourth\"");
      final Path script = directory.resolve("line-ends.xq");
      Files.writeString(script, source);
      for (final String input : List.of(script.toString(), "-")) {
        final String[] output = invoke(source, "-iqf", input, "--no-color");
        assertEquals(nl + "Query result" + nl + "first\n  second" + nl + nl + nl + "Query result" + nl
            + "third\nfourth" + nl + nl, output[0]);
        assertEquals("", output[1]);
      }
    }
  }

  @Test
  void queryScriptsPreserveOtherSeparatorsAsQueryContent() throws Exception {
    final String nl = System.lineSeparator();
    final String frame = nl + "Query result" + nl;
    for (final int[] separator : new int[][] {{11, 11}, {12, 12}, {13, 10}, {133, 10}, {8232, 10}, {8233, 8233}}) {
      final String character = Character.toString(separator[0]);
      final String middleLine = separator[0] == 13
          ? character + "\t"
          : character;
      for (final String ending : List.of("\n", "\r\n")) {
        for (final String quote : List.of("'", "\"")) {
          final String source = "string-to-codepoints(" + quote + "a" + character + "b" + quote + ")" + ending
              + " \t" + ending + "string-to-codepoints(" + quote + "a" + ending + middleLine + ending + "b" + quote + ")";
          final Path script = directory.resolve("separator-content.xq");
          Files.writeString(script, source);
          for (final String input : List.of(script.toString(), "-")) {
            final String[] output = capture(source,
                () -> assertEquals(0, Main.run(new String[] {"-iqf", input, "--no-color"}, null),
                    "script character U+" + Integer.toHexString(separator[0])));
            final String multiline = separator[0] == 13
                ? "97 10 10 9 10 98"
                : "97 10 " + separator[1] + " 10 98";
            assertEquals(frame + "97 " + separator[1] + " 98" + nl + nl + frame + multiline + nl + nl, output[0]);
            assertEquals("", output[1]);
          }
        }
      }
    }
  }

  @Test
  void literalLineEndsNormalizeBeforeEvaluationAndRuntimeCarriageReturnsSurviveOutput() {
    try (final SirixQueryContext context = SirixQueryContext.create()) {
      final Query literal = new Query("'first\r\nsecond'");
      try (final Iter values = literal.evaluate(context).iterate()) {
        assertEquals("first\nsecond", values.next().atomize().stringValue());
        assertNull(values.next());
      }
      final StringWriter out = new StringWriter();
      try (final PrintWriter writer = new PrintWriter(out)) {
        literal.serialize(context, new SirixStringSerializer(writer));
      }
      assertEquals("first\nsecond", out.toString());
      final String[] output = invoke("", "-q", "codepoints-to-string((102,13,10,115))", "--no-color");
      final String nl = System.lineSeparator();
      assertEquals(nl + "Query result" + nl + "f\r\ns" + nl + nl, output[0]);
      assertEquals("", output[1]);
    }
  }

  private static int occurrences(final String text, final String part) {
    int count = 0;
    for (int at = text.indexOf(part); at >= 0; at = text.indexOf(part, at + part.length()))
      count++;
    return count;
  }

  private static String[] invoke(final String input, final String... arguments) {
    return invoke(0, input, arguments);
  }

  @Test
  void batchQueryErrorsPreserveDiagnosticsAndFailureStatusAcrossDispatches() throws Exception {
    for (final String[] failure : new String[][] {{"1+", "err:XPST0003"}, {"1 idiv 0", "err:FOAR0001"}}) {
      final Path queryFile = directory.resolve("invalid query.xq");
      Files.writeString(queryFile, failure[0]);
      for (final String[] args : new String[][] {{"-q", failure[0]}, {"-qf", queryFile.toString()}, {"-qf", "-"}, {}}) {
        final String[] output = invoke(-1, failure[0], args);
        assertTrue(output[1].startsWith("Error: " + failure[1]), output[1]);
      }
    }
    final Path missing = directory.resolve("missing.xq");
    final String[] output = invoke(-1, "", "-qf", missing.toString());
    assertEquals("", output[0]);
    assertEquals("I/O Error: " + missing + System.lineSeparator(), output[1]);
  }

  private static String[] invoke(final int expectedStatus, final String input, final String... arguments) {
    return capture(input, () -> {
      if (expectedStatus == 0)
        Main.main(arguments);
      else
        assertEquals(expectedStatus, Main.run(arguments, null));
    });
  }

  private static String[] capture(final String input, final Runnable action) {
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
      action.run();
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
  void batchQueryErrorExitsTheProcessWithADiagnostic() throws Exception {
    final Path error = directory.resolve("error.txt");
    final Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "--enable-preview", "--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED", "-Xmx128m",
        "-Dstderr.encoding=UTF-8", "-Duser.home=" + directory, "-cp", System.getProperty("java.class.path"),
        Main.class.getName(), "-q", "1+").redirectOutput(directory.resolve("out.txt").toFile())
                                         .redirectError(error.toFile())
                                         .start();
    try {
      assertTrue(process.waitFor(60, TimeUnit.SECONDS), Files.readString(error));
      assertTrue(process.exitValue() != 0, "Query errors must fail the process");
      assertTrue(Files.readString(error).contains("Error: err:XPST0003:"), Files.readString(error));
    } finally {
      if (process.isAlive())
        process.destroyForcibly();
      process.waitFor();
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
