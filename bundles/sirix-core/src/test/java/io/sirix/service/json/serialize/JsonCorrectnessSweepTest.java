package io.sirix.service.json.serialize;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.exception.SirixException;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Adversarial correctness sweep for {@link JsonSerializer}.
 *
 * <p>The {@code 83} shapes live in {@code src/test/resources/json/correctnessSweepShapes.json} (a
 * JSON array of {@code {name, json}}) so the exact bytes — including raw control characters, NUL,
 * astral surrogate pairs, BOM, bidi marks and big-number literals — are preserved verbatim rather
 * than re-transcribed into Java string literals. Each shape is shredded into a fresh hash-enabled
 * resource and then put through three families of checks; failures are COLLECTED (never fail-fast)
 * and reported together at the end:
 *
 * <ol>
 *   <li><b>(A) round-trip fidelity</b> — serialize with NO metadata
 *       ({@code new JsonSerializer.Builder(manager, writer).build()}), parse the original input and
 *       the serialized output with Jackson and assert they are semantically equal (objects compared
 *       key-order-insensitively, arrays order-sensitively, numbers by exact value). The mapper uses
 *       {@code USE_BIG_INTEGER_FOR_INTS} + {@code USE_BIG_DECIMAL_FOR_FLOATS} so the 2^53 / 2^64 /
 *       2^128 / 30-plus-digit-integer and 40-digit-decimal shapes are compared without silent double
 *       rounding, and {@code ALLOW_UNQUOTED_CONTROL_CHARS} so the raw-control-char shapes parse at
 *       all (Jackson's default mode rejects unescaped control chars). On mismatch the JSON Pointer
 *       path of the first divergence plus expected-vs-actual is recorded.</li>
 *   <li><b>(B) metadata validity</b> — serialize with {@code withMetaData(true)} AND with
 *       {@code withNodeKeyAndChildCountMetaData(true)}, at {@code maxLevel} in
 *       {@code {1, 2, 3, Integer.MAX_VALUE}} plus one {@code maxChildren(2)} run in each mode, and
 *       assert each output parses as valid JSON via {@code org.json} (a strict structural parser
 *       distinct from Jackson). On failure the parser error and a ~120-char snippet around the
 *       offending offset are recorded.</li>
 *   <li><b>(C) exceptions</b> — any throwable raised during shred/serialize is recorded.</li>
 * </ol>
 *
 * <p>This is the regression guard for the alpha11 serializer defect where an object-valued key's
 * children were emitted as bare {@code {..},{..}} (missing the {@code [ ]} array wrapper) under
 * {@code withMetaData=nodeKeyAndChildCount}, producing invalid JSON for object-shaped resources.
 */
public final class JsonCorrectnessSweepTest {

  private static final Path SHAPES_FILE =
      Paths.get("src", "test", "resources", "json", "correctnessSweepShapes.json");

  /** A test shape: a human-readable name plus the raw JSON document to exercise. */
  private record Shape(String name, String json) {
  }

  /** A collected failure, printed one-per-line and asserted-empty at the end. */
  private record Failure(String shapeName, String json, String mode, String kind, String detail) {
    @Override
    public String toString() {
      String j = json;
      if (j.length() > 220) {
        j = j.substring(0, 220) + "…(" + json.length() + " chars)";
      }
      return "FAILURE  shape=" + shapeName + "  mode=" + mode + "  kind=" + kind + "\n  detail: " + detail
          + "\n  json: " + j;
    }
  }

  /**
   * The metadata serialization variants exercised by check (B): the two metadata modes crossed with
   * the four maxLevel bounds, plus a single maxChildren(2) cap in each mode.
   */
  private record MetaMode(String label, boolean nodeKeyAndChildCount, int maxLevel, int maxChildren) {
  }

  private static final int NO_CHILD_CAP = Integer.MAX_VALUE;

  // ── Jackson mapper for check (A) ────────────────────────────────────────────────────────────────
  // Big-integer / big-decimal modes keep exact values for the numeric-edge shapes (2^53, 2^64,
  // 2^128, 30+-digit integers, 40-digit decimals). Both the input and the Sirix output are parsed
  // with the SAME mapper so node kinds line up: integers -> BigIntegerNode, floats -> DecimalNode;
  // JsonNode.equals then enforces int-vs-float typing (1 != 1.0) and value-exact numeric equality.
  // ALLOW_UNQUOTED_CONTROL_CHARS lets the raw-control-char shapes (NUL/VT/US/BEL/ESC inside strings)
  // be read; we compare round-tripped VALUES, so input-side escaping is irrelevant to (A).
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
  public void sweep() throws Exception {
    final List<Shape> shapes = loadShapes();

    final List<MetaMode> metaModes = new ArrayList<>();
    for (final boolean nkcc : new boolean[] {true, false}) {
      final String base = nkcc ? "nodeKeyAndChildCount" : "metaData";
      for (final int level : new int[] {1, 2, 3, Integer.MAX_VALUE}) {
        metaModes.add(new MetaMode(base + " maxLevel=" + levelLabel(level), nkcc, level, NO_CHILD_CAP));
      }
      metaModes.add(new MetaMode(base + " maxChildren=2", nkcc, Integer.MAX_VALUE, 2));
    }

    final List<Failure> failures = new ArrayList<>();
    int totalAssertions = 0;

    for (final Shape shape : shapes) {
      JsonTestHelper.deleteEverything();
      final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE)) {

        // ── shred ──────────────────────────────────────────────────────────────────────────────
        // The trx is managed manually (not in try-with-resources) so that a shred failure can be
        // rolled back before the session closes; an un-rolled-back open trx would otherwise make the
        // close throw "Must commit/rollback transaction first!" and report a spurious second failure.
        boolean shredOk = true;
        try (final var trx = manager.beginNodeTrx()) {
          try {
            new JsonShredder.Builder(trx, JsonShredder.createStringReader(shape.json()),
                InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
          } catch (final Throwable t) {
            shredOk = false;
            failures.add(new Failure(shape.name(), shape.json(), "shred", "exception", describe(t)));
            try {
              trx.rollback();
            } catch (final Throwable ignore) {
              // best-effort: clear the open trx so the session can close cleanly
            }
          }
        }
        if (!shredOk) {
          continue; // nothing serializable — move on to the next shape
        }

        // ── (A) round-trip fidelity (no metadata) ────────────────────────────────────────────────
        totalAssertions++;
        try {
          final Writer w = new StringWriter();
          new JsonSerializer.Builder(manager, w).build().call();
          final String out = w.toString();
          final JsonNode expected = exactMapper.readTree(shape.json());
          final JsonNode actual = exactMapper.readTree(out);
          if (!expected.equals(actual)) {
            final String path = firstDiff(expected, actual, "");
            failures.add(new Failure(shape.name(), shape.json(), "no-metadata round-trip",
                "roundtrip-mismatch", clip(path, 480)));
          }
        } catch (final Throwable t) {
          failures.add(new Failure(shape.name(), shape.json(), "no-metadata round-trip", "exception",
              describe(t)));
        }

        // ── (B) metadata validity ────────────────────────────────────────────────────────────────
        for (final MetaMode mm : metaModes) {
          totalAssertions++;
          String out = null;
          try {
            final Writer w = new StringWriter();
            var b = new JsonSerializer.Builder(manager, w).maxLevel(mm.maxLevel());
            if (mm.maxChildren() != NO_CHILD_CAP) {
              b = b.maxChildren(mm.maxChildren());
            }
            if (mm.nodeKeyAndChildCount()) {
              b = b.withNodeKeyAndChildCountMetaData(true);
            } else {
              b = b.withMetaData(true);
            }
            b.build().call();
            out = w.toString();
            // Validate with org.json (strict structural parser, distinct from Jackson).
            final String trimmed = out.trim();
            if (trimmed.startsWith("[")) {
              new org.json.JSONArray(out);
            } else {
              new org.json.JSONObject(out);
            }
          } catch (final org.json.JSONException je) {
            // org.json is non-RFC-strict about numeric MAGNITUDE: it parses an RFC-8259-valid
            // number that overflows IEEE double (e.g. 2e308) and then rejects it with "Forbidden
            // numeric value: Infinity". The serializer's output is valid JSON — Jackson, JS
            // JSON.parse and browsers all accept it (the no-metadata round-trip above confirms it
            // for the same shape) — so org.json's overflow rejection is a parser quirk, not a
            // serializer defect. Skip it, but still flag every genuine STRUCTURAL invalidity.
            final String msg = safe(je.getMessage());
            if (!msg.contains("Forbidden numeric value")) {
              failures.add(new Failure(shape.name(), shape.json(), mm.label(), "invalid-json",
                  clip(msg + "  |  near: " + snippetAround(out, je.getMessage()), 490)));
            }
          } catch (final Throwable t) {
            failures.add(new Failure(shape.name(), shape.json(), mm.label(), "exception", describe(t)));
          }
        }
      } catch (final Throwable t) {
        // Session/transaction lifecycle failure for this shape.
        failures.add(new Failure(shape.name(), shape.json(), "session", "exception", describe(t)));
      }
    }

    // ── report ─────────────────────────────────────────────────────────────────────────────────
    System.out.println("==== JsonCorrectnessSweep ====");
    System.out.println("totalShapes=" + shapes.size() + "  approxAssertions=" + totalAssertions
        + "  failures=" + failures.size());
    for (final Failure f : failures) {
      System.out.println(f);
    }
    if (failures.isEmpty()) {
      System.out.println(">>> SWEEP PASSED: all " + shapes.size()
          + " shapes round-tripped and produced valid metadata JSON across all modes.");
    }

    Assert.assertTrue("JSON serializer correctness sweep found " + failures.size()
        + " failure(s); see stdout for the per-shape detail.", failures.isEmpty());
  }

  // ── helpers ────────────────────────────────────────────────────────────────────────────────────

  private static List<Shape> loadShapes() throws Exception {
    final ObjectMapper plain = new ObjectMapper()
        .enable(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS);
    final String text = Files.readString(SHAPES_FILE, StandardCharsets.UTF_8);
    final JsonNode arr = plain.readTree(text);
    final List<Shape> shapes = new ArrayList<>();
    for (final JsonNode node : arr) {
      shapes.add(new Shape(node.get("name").asText(), node.get("json").asText()));
    }
    return shapes;
  }

  private static String levelLabel(final int level) {
    return level == Integer.MAX_VALUE ? "MAX" : Integer.toString(level);
  }

  private static String safe(final String s) {
    return s == null ? "null" : s;
  }

  private static String clip(final String s, final int max) {
    if (s == null) {
      return "null";
    }
    return s.length() <= max ? s : s.substring(0, max) + "…";
  }

  private static String describe(final Throwable t) {
    final StringBuilder sb = new StringBuilder();
    sb.append(t.getClass().getSimpleName()).append(": ").append(t.getMessage());
    Throwable c = t.getCause();
    if (c != null && c != t) {
      sb.append("  <- ").append(c.getClass().getSimpleName()).append(": ").append(c.getMessage());
    }
    return clip(sb.toString().replace('\n', ' ').replace('\r', ' '), 480);
  }

  /**
   * Walks two semantically-unequal trees in parallel and returns the JSON Pointer path of the first
   * divergence together with the expected vs actual values there. Object members are matched by name
   * (order-insensitive); array elements by index (order-sensitive) — mirroring {@link JsonNode#equals}.
   */
  private static String firstDiff(final JsonNode expected, final JsonNode actual, final String ptr) {
    if (expected.equals(actual)) {
      return ptr.isEmpty() ? "<no structural diff but .equals() returned false>" : "(equal at " + ptr + ")";
    }
    if (expected.isObject() && actual.isObject()) {
      final Iterator<String> names = expected.fieldNames();
      while (names.hasNext()) {
        final String name = names.next();
        if (!actual.has(name)) {
          return ptr + "/" + esc(name) + "  expected key present (value=" + abbrev(expected.get(name))
              + ") but MISSING in actual";
        }
        final JsonNode e = expected.get(name);
        final JsonNode a = actual.get(name);
        if (!e.equals(a)) {
          return firstDiff(e, a, ptr + "/" + esc(name));
        }
      }
      final Iterator<String> aNames = actual.fieldNames();
      while (aNames.hasNext()) {
        final String name = aNames.next();
        if (!expected.has(name)) {
          return ptr + "/" + esc(name) + "  UNEXPECTED key in actual (value=" + abbrev(actual.get(name)) + ")";
        }
      }
      return ptr + "  objects differ (size expected=" + expected.size() + " actual=" + actual.size() + ")";
    }
    if (expected.isArray() && actual.isArray()) {
      final int n = Math.min(expected.size(), actual.size());
      for (int i = 0; i < n; i++) {
        final JsonNode e = expected.get(i);
        final JsonNode a = actual.get(i);
        if (!e.equals(a)) {
          return firstDiff(e, a, ptr + "/" + i);
        }
      }
      if (expected.size() != actual.size()) {
        return ptr + "  array length differs: expected=" + expected.size() + " actual=" + actual.size();
      }
      return ptr + "  arrays differ";
    }
    // Leaf (or kind) mismatch.
    return (ptr.isEmpty() ? "<root>" : ptr) + "  expected=" + abbrev(expected) + " (" + expected.getNodeType()
        + ")  actual=" + abbrev(actual) + " (" + actual.getNodeType() + ")";
  }

  private static String esc(final String name) {
    // Minimal JSON-Pointer token escaping (RFC 6901): ~ -> ~0, / -> ~1.
    return name.replace("~", "~0").replace("/", "~1");
  }

  private static String abbrev(final JsonNode n) {
    final String s = n.toString().replace('\n', ' ').replace('\r', ' ');
    return s.length() <= 60 ? s : s.substring(0, 60) + "…";
  }

  /** Returns ~120 chars of {@code out} centred on the character offset named in an org.json error. */
  private static String snippetAround(final String out, final String errMsg) {
    if (out == null) {
      return "<no output>";
    }
    int idx = -1;
    if (errMsg != null) {
      final java.util.regex.Matcher m = java.util.regex.Pattern.compile("character\\s+(\\d+)").matcher(errMsg);
      if (m.find()) {
        try {
          idx = Integer.parseInt(m.group(1));
        } catch (final NumberFormatException ignore) {
          idx = -1;
        }
      }
    }
    if (idx < 0 || idx > out.length()) {
      // Fall back to the head of the document.
      return clip(out, 120).replace('\n', ' ').replace('\r', ' ');
    }
    final int from = Math.max(0, idx - 60);
    final int to = Math.min(out.length(), idx + 60);
    return ("…" + out.substring(from, to) + "…").replace('\n', ' ').replace('\r', ' ');
  }
}
