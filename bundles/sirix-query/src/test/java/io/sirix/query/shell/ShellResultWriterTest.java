package io.sirix.query.shell;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestReporter;
import io.sirix.query.shell.ShellResultWriter.DiffMode;
import java.io.StringWriter;
import java.util.Random;
import java.util.List;
import org.jline.utils.AttributedString;
import java.io.Writer;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import com.sun.management.ThreadMXBean;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ShellResultWriterTest {
  @Test
  void goldenJsonAndDiffColours() throws Exception {
    assertEquals("{\033[36m\"x\"\033[0m:\033[33m12\033[0m,\033[36m\"s\"\033[0m:\033[32m\"ok\"\033[0m}",
        render("{\"x\":12,\"s\":\"ok\"}", false, ShellPalette.ANSI_16, 1));
    final String result =
        render("{\"insert\":{},\"update\":{},\"replace\":{},\"delete\":{}}", true, ShellPalette.ANSI_16, 2);
    assertEquals(
        "{\033[32m\"insert\"\033[0m:{},\033[33m\"update\"\033[0m:{},\033[33m\"replace\"\033[0m:{},\033[31m\"delete\"\033[0m:{}}",
        result);
  }

  @Test
  void xmlContentQuotesAndMarkupTerminatorsPreserveFollowingTagStyles() throws Exception {
    for (final String content : List.of("text \" ' 12 {insert}", "<!-- \" ' > <fake/> -->",
        "<?target \" ' > <fake/> ?>", "<![CDATA[\" ' > <fake/> {insert}]]>")) {
      final String input = "<root>" + content + "<child a='v' b=\"w\"/></root>";
      for (final DiffMode mode : DiffMode.values()) {
        for (int split = 0; split <= input.length(); split++) {
          final StringWriter out = new StringWriter();
          try (final ShellResultWriter writer = new ShellResultWriter(out, ShellPalette.ANSI_16, mode)) {
            writer.write(input, 0, split);
            writer.flush();
            writer.write(input, split, input.length() - split);
          }
          final String rendered = out.toString();
          assertEquals(input, strip(rendered));
          final var styled = AttributedString.fromAnsi(rendered);
          assertStyle(styled, input.indexOf("<child"), "36");
          assertStyle(styled, input.indexOf("'v'"), "32");
          assertStyle(styled, input.indexOf("\"w\""), "32");
          assertStyle(styled, input.indexOf("</root>"), "36");
          final String contentStyle = content.startsWith("<")
              ? "36"
              : "0";
          for (int position = 0; position < content.length(); position++)
            assertStyle(styled, "<root>".length() + position, contentStyle);
        }
        assertEquals(input, strip(render(input, mode != DiffMode.NONE, ShellPalette.NONE, 1)));
      }
    }
  }

  @Test
  void mixedXmlJsonAndPatchContextsRecoverAcrossEveryWriterOverloadAndFinish() throws Exception {
    final String input = "{\"x\":1} <root><nested/>\" {\"insert\":1} <child/></root> \"json\" "
        + "<?pi \" > ?> <!-- ' > --> insert node <a/>; replace node <b/>";
    for (final int overload : new int[] {0, 1, 2}) {
      final StringWriter out = new StringWriter();
      try (final ShellResultWriter writer = new ShellResultWriter(out, ShellPalette.ANSI_16, DiffMode.JSON)) {
        final char[] chars = input.toCharArray();
        for (int i = 0; i < chars.length; i++) {
          if (overload == 0)
            writer.write(chars[i]);
          else if (overload == 1)
            writer.write(chars, i, 1);
          else
            writer.write(input, i, 1);
          writer.flush();
        }
        writer.finish();
        writer.write("{\"replace\":{}}");
        writer.finish();
      }
      final String expected = input + "{\"replace\":{}}";
      assertEquals(expected, strip(out.toString()));
      final var styled = AttributedString.fromAnsi(out.toString());
      assertStyle(styled, input.indexOf("<child/>"), "36");
      assertStyle(styled, input.indexOf("{\"insert"), "0");
      assertStyle(styled, input.indexOf("\"json\""), "32");
      assertStyle(styled, input.indexOf("insert node"), "0");
      assertStyle(styled, input.indexOf("replace node"), "0");
      assertStyle(styled, expected.lastIndexOf("\"replace\""), "33");
    }
    for (final String unfinished : List.of("<a x='", "<a>\"", "<!-- \"", "<?pi '", "<![CDATA[\"")) {
      final StringWriter out = new StringWriter();
      try (final ShellResultWriter writer = new ShellResultWriter(out, ShellPalette.ANSI_16, DiffMode.JSON)) {
        writer.write(unfinished);
        writer.finish();
        writer.write("{\"replace\":1}");
      }
      assertEquals(unfinished + "{\"replace\":1}", strip(out.toString()));
      assertTrue(out.toString().contains("\033[33m\"replace\"\033[0m"));
    }
    assertTrue(render("{\"replace\":{}}", false, ShellPalette.ANSI_16, 1).contains("\033[36m\"replace\"\033[0m"));
  }

  private static void assertStyle(final AttributedString styled, final int offset, final String ansi) {
    assertEquals(AttributedString.fromAnsi("\033[" + ansi + "mX").styleAt(0), styled.styleAt(offset));
  }

  @Test
  void chunkingNeverChangesPayloadAndBuffersStayBounded() throws Exception {
    final String huge = "[{\"long" + "k".repeat(90000) + "\":\"😀\\\"\n商店" + "x".repeat(1000000)
        + "\",\"n\":-12.4e+3,\"b\":true},null]";
    for (final int chunk : new int[] {1, 7, 8191, 65536}) {
      assertEquals(huge, strip(render(huge, false, ShellPalette.ANSI_256, chunk)));
    }
    assertEquals(huge, render(huge, true, ShellPalette.NONE, 333));
  }

  @Test
  void seededAdversarialStringsAndXmlPreserveEveryCharacter() throws Exception {
    final Random random = new Random(94721);
    for (int i = 0; i < 100; i++) {
      final var text = new StringBuilder();
      for (int j = 0; j < 250; j++)
        text.append("{}[]<>\"'\\:,\n\t012ae商店".charAt(random.nextInt("{}[]<>\"'\\:,\n\t012ae商店".length())));
      final String input = text.toString();
      assertEquals(input, strip(render(input, true, ShellPalette.ANSI_16, 13)));
    }
    final String xml = "<root attr=\"商店\"><item>12</item><!-- hi --></root>";
    final String coloured = render(xml, false, ShellPalette.ANSI_16, 1);
    assertEquals(xml, strip(coloured));
    assertTrue(coloured.contains("\033[36m<root"));
    assertTrue(coloured.contains("\033[32m\"商店\""));
  }

  @Test
  void operationPositionsSurviveEverySplitOverloadAndFinish() throws Exception {
    final String input = "let $doc := xn:doc('delete','insert',1) return (\n"
        + "insert node attribute delete-insert { \"v\" } into sdb:select-item($doc,1),\n"
        + "insert node attribute delete:insert { \"v\" } into sdb:select-item($doc,1),\n"
        + "replace value of node $doc/delete with \"insert\", rename node $doc/insert as 'delete',\n"
        + "delete nodes $doc/replace); append nodes <delete insert='replace'>rename</delete>";
    final String expected =
        "let $doc := xn:doc(\033[32m'delete'\033[0m,\033[32m'insert'\033[0m,\033[33m1\033[0m) return (\n"
            + "\033[32minsert\033[0m node attribute delete-insert { \033[32m\"v\"\033[0m } into sdb:select-item($doc,\033[33m1\033[0m),\n"
            + "\033[32minsert\033[0m node attribute delete:insert { \033[32m\"v\"\033[0m } into sdb:select-item($doc,\033[33m1\033[0m),\n"
            + "\033[33mreplace\033[0m value of node $doc/delete with \033[32m\"insert\"\033[0m, \033[33mrename\033[0m node $doc/insert as \033[32m'delete'\033[0m,\n"
            + "\033[31mdelete\033[0m nodes $doc/replace); \033[32mappend\033[0m nodes \033[36m<delete insert=\033[32m'replace'\033[0m\033[36m>\033[0mrename\033[36m</delete>\033[0m";
    final char[] chars = input.toCharArray();
    for (int overload = 0; overload < 3; overload++) {
      for (int split = 0; split <= input.length(); split++) {
        final StringWriter out = new StringWriter();
        try (final ShellResultWriter writer = new ShellResultWriter(out, ShellPalette.ANSI_16, DiffMode.XML)) {
          for (int i = 0; i < input.length(); i++) {
            if (i == split)
              writer.flush();
            if (overload == 0)
              writer.write(input, i, 1);
            else if (overload == 1)
              writer.write(chars, i, 1);
            else
              writer.write(chars[i]);
          }
          writer.finish();
          assertEquals(expected, out.toString());
          writer.write("delete");
          writer.finish();
        }
        assertEquals(expected + "\033[31mdelete\033[0m", out.toString());
      }
    }
    for (final DiffMode mode : List.of(DiffMode.NONE, DiffMode.JSON)) {
      final StringWriter out = new StringWriter();
      try (final ShellResultWriter writer = new ShellResultWriter(out, ShellPalette.ANSI_16, mode)) {
        writer.write("insert delete append replace rename");
      }
      assertEquals("insert delete append replace rename", out.toString());
    }
  }

  @Test
  void markupAndValueConstructorsCannotIntroduceOperations() throws Exception {
    final String input = "<a/>delete; insert node attribute a { (delete), rename }; delete";
    final StringWriter out = new StringWriter();
    try (final ShellResultWriter writer = new ShellResultWriter(out, ShellPalette.ANSI_16, DiffMode.XML)) {
      writer.write(input);
    }
    assertEquals(
        "\033[36m<a/>\033[0mdelete; \033[32minsert\033[0m node attribute a { (delete), rename }; \033[31mdelete\033[0m",
        out.toString());
    final StringWriter start = new StringWriter();
    try (final ShellResultWriter writer = new ShellResultWriter(start, ShellPalette.ANSI_16, DiffMode.XML)) {
      writer.write("delete");
    }
    assertEquals("\033[31mdelete\033[0m", start.toString());
  }

  @Test
  void longWordsPreservePayloadAndFollowingOperationStyle() throws Exception {
    final String word = "deleteNamedOperand".repeat(100);
    final char[] chars = word.toCharArray();
    for (int overload = 0; overload < 3; overload++) {
      for (int chunk = 1; chunk <= 20; chunk++) {
        final StringWriter out = new StringWriter();
        try (final ShellResultWriter writer = new ShellResultWriter(out, ShellPalette.ANSI_16, DiffMode.XML)) {
          for (int offset = 0; offset < word.length(); offset += chunk) {
            final int length = Math.min(chunk, word.length() - offset);
            if (overload == 0)
              writer.write(word, offset, length);
            else if (overload == 1)
              writer.write(chars, offset, length);
            else
              for (int i = offset; i < offset + length; i++)
                writer.write(chars[i]);
            writer.flush();
          }
          writer.write("; delete nodes $doc/a");
          writer.finish();
          assertEquals(word + "; \033[31mdelete\033[0m nodes $doc/a", out.toString());
          writer.write("delete");
        }
        assertEquals(word + "; \033[31mdelete\033[0m nodes $doc/a\033[31mdelete\033[0m", out.toString());
      }
    }
  }

  @Test
  void xmlDiffPatchKeywordsHaveDistinctColours() throws Exception {
    final String patch = "insert node <a/> into $doc; replace value of node $doc/a with 'x'; delete node $doc/b";
    final StringWriter out = new StringWriter();
    try (final ShellResultWriter writer = new ShellResultWriter(out, ShellPalette.ANSI_16, DiffMode.XML)) {
      for (int i = 0; i < patch.length(); i++)
        writer.write(patch.charAt(i));
    }
    final String rendered = out.toString();
    assertEquals(patch, strip(rendered));
    assertTrue(rendered.contains("\033[32minsert\033[0m"));
    assertTrue(rendered.contains("\033[33mreplace\033[0m"));
    assertTrue(rendered.contains("\033[31mdelete\033[0m"));
  }

  @Test
  void xqueryPatchStringsPreserveStylesAcrossAllSplitsAndWriterOverloads() throws Exception {
    for (final String literal : List.of("\"C:\\\"", "'C:\\'", "\"商店😀 delete \"\"insert\"\"\\\"",
        "'商店😀 delete ''insert''\\'", "\"<delete/> {1}\\\"", "''", "\"\"")) {
      for (final String prefix : List.of("replace value of node $doc/a with ", "rename node $doc/a as ",
          "insert nodes ", "insert node attribute a { ", "let $doc := xn:doc(")) {
        final String suffix = prefix.startsWith("let")
            ? ", 'resource\\', 1) return (delete nodes $doc/b)"
            : (prefix.startsWith("insert node attribute")
                ? " }"
                : "") + "; delete nodes $doc/b; insert nodes <a/>; replace node $doc/a with <b/>";
        final String input = prefix + literal + suffix;
        for (int overload = 0; overload < 3; overload++) {
          for (int split = 0; split <= input.length(); split++) {
            final StringWriter out = new StringWriter();
            try (final ShellResultWriter writer = new ShellResultWriter(out, ShellPalette.ANSI_16, DiffMode.XML)) {
              writePart(writer, input, 0, split, overload);
              writer.flush();
              writePart(writer, input, split, input.length() - split, overload);
            }
            final String rendered = out.toString();
            assertEquals(input, strip(rendered));
            final var styled = AttributedString.fromAnsi(rendered);
            for (int offset = prefix.length(); offset < prefix.length() + literal.length(); offset++)
              assertStyle(styled, offset, "32");
            assertStyle(styled, input.indexOf("delete nodes"), "31");
            if (!prefix.startsWith("let")) {
              assertStyle(styled, input.lastIndexOf("insert nodes"), "32");
              assertStyle(styled, input.lastIndexOf("replace node"), "33");
            }
          }
        }
        for (final ShellPalette palette : ShellPalette.values()) {
          final StringWriter out = new StringWriter();
          try (final ShellResultWriter writer = new ShellResultWriter(out, palette, DiffMode.XML)) {
            writer.write(input);
          }
          assertEquals(input, strip(out.toString()));
          assertTrue(out.toString().contains(palette.red() + "delete" + palette.reset()));
        }
      }
    }
  }

  @Test
  void jsonEscapesStayDistinctFromXqueryStringsAndFinishResetsUnterminatedStrings() throws Exception {
    final String json = "{\"value\":\"C:\\\\商店\\\"delete\",\"delete\":1}";
    for (final DiffMode mode : List.of(DiffMode.NONE, DiffMode.JSON)) {
      final StringWriter out = new StringWriter();
      try (final ShellResultWriter writer = new ShellResultWriter(out, ShellPalette.ANSI_16, mode)) {
        for (int i = 0; i < json.length(); i++) {
          writer.write(json.charAt(i));
          writer.flush();
        }
      }
      assertEquals(json, strip(out.toString()));
      final var styled = AttributedString.fromAnsi(out.toString());
      assertStyle(styled, json.indexOf("delete"), "32");
      assertStyle(styled, json.lastIndexOf("\"delete\""), mode == DiffMode.JSON
          ? "31"
          : "36");
      final String apostrophe = "'text' 12";
      final StringWriter plainQuote = new StringWriter();
      try (final ShellResultWriter writer = new ShellResultWriter(plainQuote, ShellPalette.ANSI_16, mode)) {
        writer.write(apostrophe);
      }
      assertEquals("'text' \033[33m12\033[0m", plainQuote.toString());
    }
    for (final String unfinished : List.of("'商店\\", "\"商店\\", "'a''delete", "\"a\"\"delete")) {
      final StringWriter out = new StringWriter();
      try (final ShellResultWriter writer = new ShellResultWriter(out, ShellPalette.ANSI_16, DiffMode.XML)) {
        writer.write(unfinished);
        writer.flush();
        writer.finish();
        writer.write("delete nodes $doc/a");
      }
      assertEquals(unfinished + "delete nodes $doc/a", strip(out.toString()));
      final var styled = AttributedString.fromAnsi(out.toString());
      for (int i = 0; i < unfinished.length(); i++)
        assertStyle(styled, i, "32");
      assertStyle(styled, unfinished.length(), "31");
    }
  }

  private static void writePart(final ShellResultWriter writer, final String input, final int offset, final int length,
      final int overload) throws IOException {
    if (overload == 0)
      writer.write(input, offset, length);
    else if (overload == 1)
      writer.write(input.toCharArray(), offset, length);
    else
      for (int i = offset; i < offset + length; i++)
        writer.write(input.charAt(i));
  }

  @Test
  void serializationUsesNoPerValueScratchAllocations() throws Exception {
    final ThreadMXBean bean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    final String payload = ("{\"n\":123,\"s\":\"hello\"} "
        + "<root><!-- \" > --><?pi ' > ?><![CDATA[\" > ]]><a x='v'>\"12{insert}</a></root> ").repeat(10000);
    final String patch = "let $doc := xn:doc('shop\\','resource\\',1) return ("
        + "replace value of node $doc/a with \"商店\\\", delete nodes $doc/b)";
    for (final DiffMode mode : List.of(DiffMode.JSON, DiffMode.XML)) {
      try (final ShellResultWriter writer = new ShellResultWriter(Writer.nullWriter(), ShellPalette.ANSI_16, mode)) {
        for (int i = 0; i < 50; i++) {
          writer.write(payload);
          writer.write(patch);
          writer.finish();
        }
        final long before = bean.getThreadAllocatedBytes(Thread.currentThread().threadId());
        for (int i = 0; i < 10; i++) {
          writer.write(payload);
          writer.write(patch);
          writer.finish();
        }
        final long allocated = bean.getThreadAllocatedBytes(Thread.currentThread().threadId()) - before;
        assertTrue(allocated <= 512, "Unexpected per-value allocation: " + allocated);
      }
    }
  }

  @Test
  void everyWriterOverloadPreservesOffsetsAndNoColourPolicy() throws Exception {
    final String expected = "{\033[36m\"x\"\033[0m:\033[33m1\033[0m}";
    for (final boolean chars : new boolean[] {false, true}) {
      final StringWriter out = new StringWriter();
      try (final ShellResultWriter writer = new ShellResultWriter(out, ShellPalette.ANSI_16, DiffMode.NONE)) {
        if (chars)
          writer.write("!{\"x\":1}!".toCharArray(), 1, 7);
        else
          for (final char c : "{\"x\":1}".toCharArray())
            writer.write(c);
      }
      assertEquals(expected, out.toString());
    }
    final StringWriter out = new StringWriter();
    try (final ShellResultWriter writer = new ShellResultWriter(out, ShellPalette.NONE, DiffMode.NONE)) {
      writer.write("!abc!".toCharArray(), 1, 3);
      writer.write('x');
      assertEquals("abcx", out.toString());
    }
  }

  @Test
  void xmlAttributeAndTagStylesAndIncompleteTokensResetPrecisely() throws Exception {
    assertEquals("\033[36m<a x=\033[32m'v'\033[0m\033[36m/>\033[0m",
        render("<a x='v'/>", false, ShellPalette.ANSI_16, 1));
    assertEquals("\033[36m<a x=\033[32m'a\\'\033[0m\033[36m/>\033[0m",
        render("<a x='a\\'/>", false, ShellPalette.ANSI_16, 1));
    assertEquals("\033[32m\"abc\033[0m", render("\"abc", false, ShellPalette.ANSI_16, 1));
    assertEquals("{\033[36m\"key\033[0m", render("{\"key", false, ShellPalette.ANSI_16, 1));
    assertEquals("\033[33m12\033[0m", render("12", false, ShellPalette.ANSI_16, 1));
    assertEquals("\033[36m<abc\033[0m", render("<abc", false, ShellPalette.ANSI_16, 1));
    assertEquals("abc", render("abc", false, ShellPalette.ANSI_16, 1));
  }

  @Test
  void scratchBoundariesAndDeeplyNestedDocumentsNeverOverflow() throws Exception {
    for (final String input : new String[] {"abcdefghijklmnopq", "[".repeat(300) + "]".repeat(300)}) {
      assertEquals(input, strip(render(input, true, ShellPalette.ANSI_16, 1)));
    }
    final String nested = "[".repeat(255) + "{\"a\":1,\"b\":2}" + "]".repeat(255);
    assertTrue(render(nested, false, ShellPalette.ANSI_16, 1).contains("\033[36m\"b\"\033[0m"));
    final CountingWriter out = new CountingWriter();
    try (final ShellResultWriter writer = new ShellResultWriter(out, ShellPalette.ANSI_16, DiffMode.NONE)) {
      writer.write("x".repeat(30000));
      writer.flush();
      assertEquals(1, out.flushes);
      assertTrue(out.writes <= 3, "Output must remain batched");
    }
  }

  @Test
  void deepJsonRetainsExactStylesAcrossContainersChunksAndWriterOverloads() throws Exception {
    for (final int depth : new int[] {255, 256, 257, 513, 4097}) {
      for (final boolean mixed : new boolean[] {false, true}) {
        for (final DiffMode mode : List.of(DiffMode.NONE, DiffMode.JSON)) {
          final DepthFixture fixture = nestedJson(depth, mixed, mode);
          final String padded = "!" + fixture.input() + "!";
          final char[] chars = padded.toCharArray();
          for (final ShellPalette palette : List.of(ShellPalette.ANSI_16, ShellPalette.NONE)) {
            for (int overload = 0; overload < 3; overload++) {
              for (final int chunk : new int[] {1, 7, 8191}) {
                final StringWriter out = new StringWriter();
                try (final ShellResultWriter writer = new ShellResultWriter(out, palette, mode)) {
                  for (int offset = 1; offset < padded.length() - 1; offset += chunk) {
                    final int length = Math.min(chunk, padded.length() - 1 - offset);
                    if (overload == 0)
                      writer.write(padded, offset, length);
                    else if (overload == 1)
                      writer.write(chars, offset, length);
                    else
                      for (int i = offset; i < offset + length; i++)
                        writer.write(chars[i]);
                    writer.flush();
                  }
                }
                assertEquals(fixture.input(), strip(out.toString()));
                assertEquals(palette.enabled()
                    ? fixture.coloured()
                    : fixture.input(), out.toString());
              }
            }
          }
        }
      }
    }
  }

  @Test
  void deepJsonEofAndFinishRetainCapacityWithoutLeakingContext() throws Exception {
    final String prefix = "[".repeat(513) + "{\"dangling";
    final String expectedPrefix = "[".repeat(513) + "{\033[36m\"dangling\033[0m";
    final DepthFixture deep = nestedJson(4097, true, DiffMode.NONE);
    final DepthFixture shallow = nestedJson(257, false, DiffMode.NONE);
    final StringWriter out = new StringWriter();
    try (final ShellResultWriter writer = new ShellResultWriter(out, ShellPalette.ANSI_16, DiffMode.NONE)) {
      writer.write(prefix);
      writer.flush();
      writer.finish();
      assertEquals(expectedPrefix, out.toString());
      writer.write(deep.input());
      writer.finish();
      writer.write(shallow.input());
      writer.finish();
      writer.write(deep.input());
    }
    assertEquals(prefix + deep.input() + shallow.input() + deep.input(), strip(out.toString()));
    assertEquals(expectedPrefix + deep.coloured() + shallow.coloured() + deep.coloured(), out.toString());
  }

  @Test
  void deepJsonStackGrowthIsBoundedAndRetainedForAllocationFreeReuse(final TestReporter reporter) throws Exception {
    final ThreadMXBean bean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    final String shallow = "{\"warm\":[1,2]}";
    final String deep = nestedJson(4097, true, DiffMode.JSON).input();
    final long thread = Thread.currentThread().threadId();
    try (final ShellResultWriter writer =
        new ShellResultWriter(Writer.nullWriter(), ShellPalette.ANSI_16, DiffMode.JSON)) {
      for (int i = 0; i < 100; i++) {
        writer.write(shallow);
        writer.finish();
      }
      final long beforeGrowth = bean.getThreadAllocatedBytes(thread);
      writer.write(deep);
      writer.finish();
      final long growth = bean.getThreadAllocatedBytes(thread) - beforeGrowth;
      assertTrue(growth > 0 && growth <= 32768, "Unexpected stack growth allocation: " + growth);
      reporter.publishEntry("stackGrowthBytes", Long.toString(growth));
      for (int i = 0; i < 50; i++) {
        writer.write(deep);
        writer.finish();
      }
      final long beforeReuse = bean.getThreadAllocatedBytes(thread);
      for (int i = 0; i < 10; i++) {
        writer.write(shallow);
        writer.finish();
        writer.write(deep);
        writer.finish();
      }
      final long reused = bean.getThreadAllocatedBytes(thread) - beforeReuse;
      assertTrue(reused <= 512, "Unexpected retained-stack allocation: " + reused);
      reporter.publishEntry("stackReuseBytes", Long.toString(reused));
    }
  }

  private record DepthFixture(String input, String coloured) {
  }

  private static DepthFixture nestedJson(final int depth, final boolean mixed, final DiffMode mode) {
    final StringBuilder input = new StringBuilder();
    final StringBuilder coloured = new StringBuilder();
    for (int i = 1; i < depth; i++) {
      final boolean object = mixed && i % 2 == 0;
      input.append(object
          ? "{\"outer\":"
          : "[");
      coloured.append(object
          ? "{\033[36m\"outer\"\033[0m:"
          : "[");
    }
    input.append("{\"a\":1,\"b\":2,\"insert\":{},\"update\":{},\"replace\":{},\"delete\":{},\"value\":\"[,]{}商店😀\"}");
    coloured.append("{\033[36m\"a\"\033[0m:\033[33m1\033[0m,\033[36m\"b\"\033[0m:\033[33m2\033[0m,")
            .append(mode == DiffMode.JSON
                ? "\033[32m\"insert\"\033[0m:{},\033[33m\"update\"\033[0m:{},\033[33m\"replace\"\033[0m:{},\033[31m\"delete\"\033[0m:{},"
                : "\033[36m\"insert\"\033[0m:{},\033[36m\"update\"\033[0m:{},\033[36m\"replace\"\033[0m:{},\033[36m\"delete\"\033[0m:{},")
            .append("\033[36m\"value\"\033[0m:\033[32m\"[,]{}商店😀\"\033[0m}");
    for (int i = depth - 1; i > 0; i--) {
      final boolean object = mixed && i % 2 == 0;
      input.append(object
          ? ",\"after\":\"tail\"}"
          : ",\"tail\"]");
      coloured.append(object
          ? ",\033[36m\"after\"\033[0m:\033[32m\"tail\"\033[0m}"
          : ",\033[32m\"tail\"\033[0m]");
    }
    return new DepthFixture(input.toString(), coloured.toString());
  }

  @Test
  void fullBufferDrainsPreservePayloadAcrossAllWriteOverloadsAndReuse() throws Exception {
    for (final int length : new int[] {16383, 16384, 16385, 32768}) {
      final String payload = "x".repeat(length);
      for (int overload = 0; overload < 3; overload++) {
        final StringWriter out = new StringWriter();
        try (final ShellResultWriter writer = new ShellResultWriter(out, ShellPalette.ANSI_16, DiffMode.NONE)) {
          if (overload == 0)
            writer.write(payload);
          else if (overload == 1)
            writer.write(payload.toCharArray());
          else
            for (int i = 0; i < length; i++)
              writer.write('x');
          writer.flush();
          assertEquals(payload, out.toString());
          writer.finish();
          writer.write('y');
        }
        assertEquals(payload + "y", out.toString());
      }
    }
  }

  @Test
  void failedFullBufferDrainRetainsPayloadAndEmptyFlushesDoNotWrite() throws Exception {
    final String payload = "x".repeat(16384);
    final StringWriter output = new StringWriter();
    final Writer sink = new Writer() {
      private boolean fail = true;

      @Override
      public void write(final char[] chars, final int offset, final int length) throws IOException {
        assertTrue(length > 0, "Empty flushes must not write payload");
        if (fail) {
          fail = false;
          throw new IOException("sink unavailable");
        }
        output.write(chars, offset, length);
      }

      @Override
      public void flush() {
        // The sink has no buffering of its own.
      }

      @Override
      public void close() {
        // The StringWriter remains readable after the adapter closes.
      }
    };
    try (final ShellResultWriter writer = new ShellResultWriter(sink, ShellPalette.ANSI_16, DiffMode.NONE)) {
      writer.flush();
      writer.finish();
      writer.write(payload);
      final IOException failure = assertThrows(IOException.class, () -> writer.write('y'));
      assertEquals("sink unavailable", failure.getMessage());
      assertEquals("", output.toString());
      writer.write('y');
      writer.flush();
      assertEquals(payload + "y", output.toString());
      writer.flush();
      writer.finish();
    }
    assertEquals(payload + "y", output.toString());
  }

  @Test
  void unmatchedClosingTagsCannotUnderflowTheXmlContentContext() throws Exception {
    final String input = "</orphan><root>\"text\"<child/></root>";
    final String rendered = render(input, false, ShellPalette.ANSI_16, 1);
    assertEquals(input, strip(rendered));
    final var styled = AttributedString.fromAnsi(rendered);
    assertStyle(styled, input.indexOf("\"text\""), "0");
    assertStyle(styled, input.indexOf("<child/>"), "36");
  }

  private static final class CountingWriter extends Writer {
    int writes;
    int flushes;

    @Override
    public void write(final char[] chars, final int offset, final int length) {
      writes++;
    }

    @Override
    public void flush() {
      flushes++;
    }

    @Override
    public void close() {}
  }

  private static String render(final String text, final boolean diff, final ShellPalette palette, final int chunk)
      throws Exception {
    final StringWriter out = new StringWriter();
    try (final ShellResultWriter writer = new ShellResultWriter(out, palette, diff
        ? DiffMode.JSON
        : DiffMode.NONE)) {
      for (int offset = 0; offset < text.length(); offset += chunk)
        writer.write(text, offset, Math.min(chunk, text.length() - offset));
    }
    return out.toString();
  }

  static String strip(final String text) {
    return text.replaceAll("\033\\[[0-9;]*m", "");
  }
}
