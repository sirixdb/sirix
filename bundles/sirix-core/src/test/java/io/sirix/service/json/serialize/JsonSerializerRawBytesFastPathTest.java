package io.sirix.service.json.serialize;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixException;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Guards the raw-bytes fast paths that let the serializer emit stored object keys and string values
 * without decoding them into Strings.
 *
 * <p>There are now two of them and they accept DIFFERENT inputs — the byte sink takes any
 * escape-free UTF-8 run ({@code JsonValueScan.mayNeedJsonEscape}), the char sink takes plain-ASCII
 * runs it can widen ({@code JsonValueScan.isPlainAscii}) — while both must still produce exactly
 * the text the slow {@code escape(getValue())} path produces. A divergence would be invisible to a
 * semantic round-trip check (both readings parse to the same value) but would silently change the
 * bytes an HTTP client receives, so the assertions here are on the CHARACTERS, not on parsed
 * equality:
 *
 * <ol>
 *   <li>{@link #charAndByteSinksAgreeOnEverySweepShape()} — every shape of the adversarial sweep
 *       corpus (raw control characters, NUL, DEL, astral surrogate pairs, BOM, bidi marks, unicode
 *       and escape-laden object KEYS) serialized through both sinks, asserted identical. This is
 *       the differential test: whichever pipeline's fast path mis-fires, the two disagree.</li>
 *   <li>{@link #escapeAndNonAsciiShapesSerializeExactly()} — a handful of shapes whose exact
 *       expected output is spelled out, pinning the escaping itself rather than only the agreement
 *       between two implementations that could in principle be wrong together.</li>
 *   <li>{@link #borrowedCursorMatchesOwnedTransaction()} — the same document through a borrowed
 *       cursor, since that path reaches the identical emit code through a different entry.</li>
 * </ol>
 */
public final class JsonSerializerRawBytesFastPathTest {

  private static final Path SHAPES_FILE =
      Paths.get("src", "test", "resources", "json", "correctnessSweepShapes.json");

  /** U+007F, built by code point so the source carries no unprintable byte. */
  private static final String DEL = String.valueOf((char) 0x7F);

  /**
   * Documents whose serialized form is asserted literally. Each exercises one boundary of the
   * plain-ASCII predicate on BOTH a key and a value.
   *
   * <p>Note the DEL rows: {@code StringValue.escape} does NOT escape U+007F (its {@code ch < 128}
   * branch takes precedence over the U+007F–U+009F range test), so DEL is emitted verbatim — legal
   * under RFC 8259, which only mandates escaping {@code "}, {@code \} and U+0000–U+001F. The
   * plain-ASCII predicate still has to REJECT it, or the fast path would emit it verbatim for a
   * different reason and the two would only agree by accident; these rows pin that the byte the
   * reader gets is the same either way.
   */
  private static final String[][] EXACT_SHAPES = {
      // plain ASCII — the fast path itself
      {"{\"plain\":\"value\"}", "{\"plain\":\"value\"}"},
      // empty value: a zero-length byte run is trivially "plain", and must emit ""
      {"{\"empty\":\"\"}", "{\"empty\":\"\"}"},
      // the three ASCII characters the scan must reject even though they are printable
      {"{\"a\":\"q\\\"q\"}", "{\"a\":\"q\\\"q\"}"},
      {"{\"a\":\"b\\\\c\"}", "{\"a\":\"b\\\\c\"}"},
      {"{\"a\":\"p/q\"}", "{\"a\":\"p\\/q\"}"},
      // the same three in the KEY position
      {"{\"q\\\"q\":1}", "{\"q\\\"q\":1}"},
      {"{\"b\\\\c\":1}", "{\"b\\\\c\":1}"},
      {"{\"p/q\":1}", "{\"p\\/q\":1}"},
      // C0 control — escaped, never emitted verbatim
      {"{\"a\":\"x\\u0001y\"}", "{\"a\":\"x\\u0001y\"}"},
      // DEL — rejected by the fast path, then passed through unescaped by the slow path
      {"{\"a\":\"x\\u007Fy\"}", "{\"a\":\"x" + DEL + "y\"}"},
      {"{\"k\\u007Fey\":1}", "{\"k" + DEL + "ey\":1}"},
      // U+0080-U+009F and U+2000-U+20FF: escaped (0xC2 / 0xE2 lead bytes)
      {"{\"a\":\"x\\u0085y\"}", "{\"a\":\"x\\u0085y\"}"},
      {"{\"a\":\"x\\u2028y\"}", "{\"a\":\"x\\u2028y\"}"},
      // non-ASCII that needs NO escape: the char sink must decode it rather than widen it
      {"{\"gr\\u00fc\\u00dfe\":\"\\u00e4\\u00f6\\u00fc\"}", "{\"grüße\":\"äöü\"}"},
      {"{\"\\u65e5\\u672c\\u8a9e\":\"\\u30c6\\u30b9\\u30c8\"}", "{\"日本語\":\"テスト\"}"},
      // astral plane (surrogate pair on the char side, four UTF-8 bytes on the byte side)
      {"{\"k\":\"\\uD83D\\uDE00\"}", "{\"k\":\"😀\"}"},
  };

  private final ObjectMapper exactMapper = JsonMapper.builder()
                                                     .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
                                                     .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                                                     .enable(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS)
                                                     .build();

  @Before
  public void setUp() throws SirixException {
    JsonTestHelper.deleteEverything();
  }

  @After
  public void tearDown() throws SirixException {
    JsonTestHelper.deleteEverything();
  }

  @Test
  public void charAndByteSinksAgreeOnEverySweepShape() throws Exception {
    final List<String> shapes = loadShapes();
    assertTrue("the sweep corpus must be readable", shapes.size() > 50);

    final List<String> mismatches = new ArrayList<>();
    for (final String json : shapes) {
      JsonTestHelper.deleteEverything();
      final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        shred(manager, json);

        final StringWriter charOut = new StringWriter();
        new JsonSerializer.Builder(manager, charOut).build().call();

        final ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        JsonSerializer.newBuilder(manager, byteOut).build().call();

        final String viaChars = charOut.toString();
        final String viaBytes = byteOut.toString(StandardCharsets.UTF_8);
        if (!viaChars.equals(viaBytes)) {
          mismatches.add(describeFirstDifference(clip(json), viaChars, viaBytes));
        }
      }
    }
    assertEquals("char and byte sinks disagree:\n" + String.join("\n", mismatches), List.of(), mismatches);
  }

  @Test
  public void escapeAndNonAsciiShapesSerializeExactly() throws Exception {
    for (final String[] shape : EXACT_SHAPES) {
      final String json = shape[0];
      final String expected = shape[1];

      JsonTestHelper.deleteEverything();
      final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        shred(manager, json);

        final StringWriter charOut = new StringWriter();
        new JsonSerializer.Builder(manager, charOut).build().call();
        assertEquals("char sink, input " + json, expected, charOut.toString());

        final ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        JsonSerializer.newBuilder(manager, byteOut).build().call();
        assertEquals("byte sink, input " + json, expected, byteOut.toString(StandardCharsets.UTF_8));
      }
    }
  }

  @Test
  public void borrowedCursorMatchesOwnedTransaction() throws Exception {
    for (final String[] shape : EXACT_SHAPES) {
      JsonTestHelper.deleteEverything();
      final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        shred(manager, shape[0]);

        final StringWriter owned = new StringWriter();
        new JsonSerializer.Builder(manager, owned).build().call();

        final StringWriter borrowed = new StringWriter();
        try (final JsonNodeReadOnlyTrx rtx = manager.beginNodeReadOnlyTrx()) {
          // Deliberately leave the cursor away from the document root: the borrowed path has to
          // reset it itself, and the fast paths must not depend on where it started.
          rtx.moveToFirstChild();
          rtx.moveToFirstChild();
          new JsonSerializer.Builder(rtx, borrowed).build().call();
        }
        assertEquals("borrowed cursor, input " + shape[0], owned.toString(), borrowed.toString());
      }
    }
  }

  private void shred(final JsonResourceSession manager, final String json) {
    try (final var trx = manager.beginNodeTrx()) {
      new JsonShredder.Builder(trx, JsonShredder.createStringReader(json), InsertPosition.AS_FIRST_CHILD)
          .commitAfterwards()
          .build()
          .call();
    }
  }

  /** The {@code json} field of every shape in the shared adversarial corpus. */
  private List<String> loadShapes() throws Exception {
    final JsonNode root = exactMapper.readTree(Files.readString(SHAPES_FILE, StandardCharsets.UTF_8));
    final List<String> shapes = new ArrayList<>();
    for (final Iterator<JsonNode> it = root.elements(); it.hasNext();) {
      shapes.add(it.next().get("json").asText());
    }
    return shapes;
  }

  private static String describeFirstDifference(final String json, final String left, final String right) {
    final int limit = Math.min(left.length(), right.length());
    int i = 0;
    while (i < limit && left.charAt(i) == right.charAt(i)) {
      i++;
    }
    final int from = Math.max(0, i - 30);
    return "input " + json + "\n  first difference at char " + i + " (lengths " + left.length() + " vs "
        + right.length() + ")\n  chars: …" + clip(left.substring(from, Math.min(left.length(), i + 30)))
        + "…\n  bytes: …" + clip(right.substring(from, Math.min(right.length(), i + 30))) + "…";
  }

  private static String clip(final String value) {
    return value.length() <= 200 ? value : value.substring(0, 200) + "…(" + value.length() + " chars)";
  }

  /** Kept so an accidental empty {@link #EXACT_SHAPES} cannot make the exact test vacuous. */
  @Test
  public void exactShapeTableIsPopulated() {
    assertTrue(Arrays.stream(EXACT_SHAPES).allMatch(s -> s.length == 2));
    assertTrue(EXACT_SHAPES.length >= 15);
  }
}
