/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.DatabaseType;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.page.NamePage;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link GlobalValueDictionary.ReadView#stringOpVerdict} against the interpreter's own string
 * semantics, value by value.
 *
 * <p>
 * The reference is INDEPENDENT of the code under test: every value decodes to a {@link String} and
 * the op evaluates with {@code String.compareTo} (the interpreter's {@code Str#cmp} collation —
 * UTF-16 code-unit order), {@code String.contains}, and {@code String.equals}. The corpus sets the
 * one trap that separates UTF-16 collation from raw byte order: a supplementary character (4-byte
 * UTF-8, lead {@code >= 0xF0}) orders AFTER U+E000..U+FFFF byte-wise but BEFORE it in UTF-16,
 * because surrogates live at 0xD800..0xDFFF. A verdict builder that compared bytes without the
 * supplementary gate would order ids 5 and 6 backwards — for packed slices AND for the spill lane,
 * which reaches its verdict through a different entry point ({@code compareToRange}).
 *
 * <p>
 * One value exceeds {@code MAX_BLOCK_BYTES}, forcing the SPILL representation, so both dispatch
 * arms of the sweep are exercised and asserted against the same reference.
 */
final class GlobalValueDictionaryStringOpVerdictTest {

  private static final String RESOURCE = "stringOpVerdict";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  /** Intern order = id order (1-based). */
  private static final List<String> VALUES = List.of("", "alpha", "alphabet", "left-google-right", "\uE000-high-bmp",
      "\uD83D\uDE00-supplementary", "x".repeat(70_000) + "google", "ZZZ");

  private static final List<String> LITERALS = List.of("", "alpha", "google", "\uE000", "\uD83D\uDE00", "zz");

  private static final List<ProjectionIndexScan.Op> OPS = List.of(ProjectionIndexScan.Op.EQ, ProjectionIndexScan.Op.NE,
      ProjectionIndexScan.Op.STR_LT, ProjectionIndexScan.Op.STR_LE, ProjectionIndexScan.Op.STR_GT,
      ProjectionIndexScan.Op.STR_GE, ProjectionIndexScan.Op.STR_CONTAINS);

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  private static boolean reference(final String value, final ProjectionIndexScan.Op op, final String literal) {
    return switch (op) {
      case EQ -> value.equals(literal);
      case NE -> !value.equals(literal);
      case STR_LT -> value.compareTo(literal) < 0;
      case STR_LE -> value.compareTo(literal) <= 0;
      case STR_GT -> value.compareTo(literal) > 0;
      case STR_GE -> value.compareTo(literal) >= 0;
      case STR_CONTAINS -> value.contains(literal);
      default -> throw new IllegalStateException("not a string op: " + op);
    };
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  @DisplayName("every (op, literal) verdict agrees with String semantics, packed and spilled alike")
  void verdictAgreesWithStringSemantics(final VersioningType versioning) {
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE).versioningApproach(versioning).build());
    }
    final long headerKey;
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = db.beginResourceSession(RESOURCE);
        final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"k\":\"v\"}"), JsonNodeTrx.Commit.NO);
      final GlobalValueDictionaryWriter dictionary = new GlobalValueDictionaryWriter();
      for (final String value : VALUES) {
        final byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
        dictionary.intern(utf8, 0, utf8.length);
      }
      final var writer = wtx.getStorageEngineWriter();
      final NamePage namePage = writer.getNamePage(writer.getActualRevisionRootPage());
      headerKey = dictionary.flush(namePage, DatabaseType.JSON, writer, writer.getLog());
      wtx.commit();
    }
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = db.beginResourceSession(RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      final GlobalValueDictionary.ReadView view =
          GlobalValueDictionary.readView(headerKey, rtx.getStorageEngineReader());
      assertNotNull(view, "dictionary must be readable after a cold reopen");
      assertEquals(VALUES.size(), view.entryCount(), "every interned value must be readable");
      for (final ProjectionIndexScan.Op op : OPS) {
        for (final String literal : LITERALS) {
          final long[] verdict = view.stringOpVerdict(op, literal.getBytes(StandardCharsets.UTF_8));
          for (int id = 1; id <= VALUES.size(); id++) {
            final boolean expected = reference(VALUES.get(id - 1), op, literal);
            final boolean actual = (verdict[id >>> 6] & 1L << (id & 63)) != 0L;
            assertEquals(expected, actual, op + " vs " + compact(literal) + " for id " + id + " ("
                + compact(VALUES.get(id - 1)) + ") — the verdict disagrees with the interpreter's String semantics");
          }
        }
      }
      // Non-vacuity: the trap must actually be armed — the corpus must order differently under
      // UTF-16 and raw bytes for at least one (value, literal) pair, and the oversized value must
      // really have spilled rather than packed.
      assertTrue(
          "\uD83D\uDE00".compareTo("\uE000") < 0
              && Arrays.compareUnsigned("\uD83D\uDE00".getBytes(StandardCharsets.UTF_8), 0, 4,
                  "\uE000".getBytes(StandardCharsets.UTF_8), 0, 3) > 0,
          "the collation trap is no longer armed — UTF-16 and byte order agree on this corpus");
      assertTrue(VALUES.get(6).getBytes(StandardCharsets.UTF_8).length > 1 << 16,
          "the spill value no longer exceeds MAX_BLOCK_BYTES — the spill arm is not exercised");
    }
  }

  private static String compact(final String s) {
    return s.length() <= 24
        ? s
        : s.substring(0, 12) + "…(" + s.length() + ")";
  }
}
