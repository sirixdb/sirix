package io.sirix.query.shell;

import org.junit.jupiter.api.Test;
import java.io.StringWriter;
import java.util.Random;
import java.util.List;
import org.jline.utils.AttributedString;
import java.io.Writer;
import java.lang.management.ManagementFactory;
import com.sun.management.ThreadMXBean;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
      for (final boolean diff : new boolean[] {false, true}) {
        for (int split = 0; split <= input.length(); split++) {
          final StringWriter out = new StringWriter();
          try (final ShellResultWriter writer = new ShellResultWriter(out, ShellPalette.ANSI_16, diff)) {
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
        assertEquals(input, strip(render(input, diff, ShellPalette.NONE, 1)));
      }
    }
  }

  @Test
  void mixedXmlJsonAndPatchContextsRecoverAcrossEveryWriterOverloadAndFinish() throws Exception {
    final String input = "{\"x\":1} <root><nested/>\" {\"insert\":1} <child/></root> \"json\" "
        + "<?pi \" > ?> <!-- ' > --> insert node <a/>; replace node <b/>";
    for (final int overload : new int[] {0, 1, 2}) {
      final StringWriter out = new StringWriter();
      try (final ShellResultWriter writer = new ShellResultWriter(out, ShellPalette.ANSI_16, true)) {
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
      assertStyle(styled, input.indexOf("insert node"), "32");
      assertStyle(styled, input.indexOf("replace node"), "33");
      assertStyle(styled, expected.lastIndexOf("\"replace\""), "33");
    }
    for (final String unfinished : List.of("<a x='", "<a>\"", "<!-- \"", "<?pi '", "<![CDATA[\"")) {
      final StringWriter out = new StringWriter();
      try (final ShellResultWriter writer = new ShellResultWriter(out, ShellPalette.ANSI_16, true)) {
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
  void xmlDiffPatchKeywordsHaveDistinctColours() throws Exception {
    final String patch = "insert node <a/> into $doc; replace value of node $doc/a with 'x'; delete node $doc/b";
    final String rendered = render(patch, true, ShellPalette.ANSI_16, 1);
    assertEquals(patch, strip(rendered));
    assertTrue(rendered.contains("\033[32minsert\033[0m"));
    assertTrue(rendered.contains("\033[33mreplace\033[0m"));
    assertTrue(rendered.contains("\033[31mdelete\033[0m"));
  }

  @Test
  void serializationUsesNoPerValueScratchAllocations() throws Exception {
    final ThreadMXBean bean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    final String payload = ("{\"n\":123,\"s\":\"hello\"} "
        + "<root><!-- \" > --><?pi ' > ?><![CDATA[\" > ]]><a x='v'>\"12{insert}</a></root> ").repeat(10000);
    try (final ShellResultWriter writer = new ShellResultWriter(Writer.nullWriter(), ShellPalette.ANSI_16, true)) {
      for (int i = 0; i < 50; i++) {
        writer.write(payload);
        writer.finish();
      }
      final long before = bean.getThreadAllocatedBytes(Thread.currentThread().threadId());
      for (int i = 0; i < 10; i++) {
        writer.write(payload);
        writer.finish();
      }
      final long allocated = bean.getThreadAllocatedBytes(Thread.currentThread().threadId()) - before;
      assertTrue(allocated <= 512, "Unexpected per-value allocation: " + allocated);
    }
  }

  @Test
  void everyWriterOverloadPreservesOffsetsAndNoColourPolicy() throws Exception {
    final String expected = "{\033[36m\"x\"\033[0m:\033[33m1\033[0m}";
    for (final boolean chars : new boolean[] {false, true}) {
      final StringWriter out = new StringWriter();
      try (final ShellResultWriter writer = new ShellResultWriter(out, ShellPalette.ANSI_16, false)) {
        if (chars)
          writer.write("!{\"x\":1}!".toCharArray(), 1, 7);
        else
          for (final char c : "{\"x\":1}".toCharArray())
            writer.write(c);
      }
      assertEquals(expected, out.toString());
    }
    final StringWriter out = new StringWriter();
    try (final ShellResultWriter writer = new ShellResultWriter(out, ShellPalette.NONE, false)) {
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
    try (final ShellResultWriter writer = new ShellResultWriter(out, ShellPalette.ANSI_16, false)) {
      writer.write("x".repeat(30000));
      writer.flush();
      assertEquals(1, out.flushes);
      assertTrue(out.writes <= 3, "Output must remain batched");
    }
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
    try (final ShellResultWriter writer = new ShellResultWriter(out, palette, diff)) {
      for (int offset = 0; offset < text.length(); offset += chunk)
        writer.write(text, offset, Math.min(chunk, text.length() - offset));
    }
    return out.toString();
  }

  static String strip(final String text) {
    return text.replaceAll("\033\\[[0-9;]*m", "");
  }
}
