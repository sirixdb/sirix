package io.sirix.access;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import io.brackit.query.atomic.QNm;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.backup.BackupManager;
import io.sirix.exception.SirixIOException;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.service.xml.shredder.XmlShredder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Specification-derived conformance tests for the resource incarnation id: FMT-209
 * (conditional-write resource metadata), CONF-102 (resource settings schema), TXN-404 (explicit
 * truncate), TXN-613 and TXN-614 (backup and restore), and the creation/recreation clauses of
 * REST-210 and TXN-615.
 *
 * <p>
 * Every test is written from the normative text alone and goes through the public surface only:
 * creating, opening, committing to, reverting, truncating, removing, backing up and restoring
 * resources, and the persisted {@code ressetting.obj} settings document that CONF-102 specifies.
 * The incarnation id is read as the {@code incarnationId} member of that document, exactly as the
 * spec states its persistence, so these tests bind to no accessor, field or class of an
 * implementation. The 1.0 resource format has exactly one shape and no compatibility path, so no
 * test asks for an opt-in switch or accepts a settings document without the member.
 *
 * <p>
 * Written before the implementation and enabled unchanged by the change that implemented FMT-209;
 * no assertion may be weakened (docs/DEVELOPMENT.md section 2).
 */
final class IncarnationIdConformanceTest {

  /** CONF-102: a 16-digit lowercase hexadecimal string. */
  private static final Pattern INCARNATION_ID = Pattern.compile("[0-9a-f]{16}");

  private static final String MEMBER = "incarnationId";

  private static final String RESOURCE = "resource";

  /** The two resource models; every behaviour in scope holds for both. */
  enum Model {
    JSON, XML;

    void createDatabase(final Path database) {
      assertTrue(this == JSON
          ? Databases.createJsonDatabase(new DatabaseConfiguration(database))
          : Databases.createXmlDatabase(new DatabaseConfiguration(database)));
    }

    void createResource(final Path database, final String name) {
      createResource(database, ResourceConfiguration.newBuilder(name).build());
    }

    void createResource(final Path database, final ResourceConfiguration configuration) {
      if (this == JSON) {
        try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(database)) {
          assertTrue(db.createResource(configuration));
        }
      } else {
        try (final Database<XmlResourceSession> db = Databases.openXmlDatabase(database)) {
          assertTrue(db.createResource(configuration));
        }
      }
    }

    void removeResource(final Path database, final String name) {
      if (this == JSON) {
        try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(database)) {
          db.removeResource(name);
        }
      } else {
        try (final Database<XmlResourceSession> db = Databases.openXmlDatabase(database)) {
          db.removeResource(name);
        }
      }
    }

    /** Commits {@code count} further revisions (the first commit of a fresh resource is revision 1). */
    void commit(final Path database, final String name, final int count) {
      if (this == JSON) {
        try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(database);
            final JsonResourceSession session = db.beginResourceSession(name)) {
          for (int i = 0; i < count; i++) {
            try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
              if (session.getMostRecentRevisionNumber() == 0) {
                wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[\"r1\"]"), JsonNodeTrx.Commit.NO);
              } else {
                wtx.moveToDocumentRoot();
                wtx.moveToFirstChild();
                wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"r\":" + i + "}"),
                    JsonNodeTrx.Commit.NO);
              }
              wtx.commit();
            }
          }
        }
      } else {
        try (final Database<XmlResourceSession> db = Databases.openXmlDatabase(database);
            final XmlResourceSession session = db.beginResourceSession(name)) {
          for (int i = 0; i < count; i++) {
            try (final XmlNodeTrx wtx = session.beginNodeTrx()) {
              if (session.getMostRecentRevisionNumber() == 0) {
                wtx.insertSubtreeAsFirstChild(XmlShredder.createStringReader("<root/>"), XmlNodeTrx.Commit.No);
              } else {
                wtx.moveToDocumentRoot();
                wtx.moveToFirstChild();
                wtx.insertElementAsFirstChild(new QNm("e" + i));
              }
              wtx.commit();
            }
          }
        }
      }
    }

    void truncateTo(final Path database, final String name, final int revision) {
      if (this == JSON) {
        try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(database);
            final JsonResourceSession session = db.beginResourceSession(name);
            final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.truncateTo(revision);
        }
      } else {
        try (final Database<XmlResourceSession> db = Databases.openXmlDatabase(database);
            final XmlResourceSession session = db.beginResourceSession(name);
            final XmlNodeTrx wtx = session.beginNodeTrx()) {
          wtx.truncateTo(revision);
        }
      }
    }

    /** Publishes a revert to {@code revision} (TXN-403: appends head+1). */
    void revertTo(final Path database, final String name, final int revision) {
      if (this == JSON) {
        try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(database);
            final JsonResourceSession session = db.beginResourceSession(name);
            final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.revertTo(revision);
          wtx.commit();
        }
      } else {
        try (final Database<XmlResourceSession> db = Databases.openXmlDatabase(database);
            final XmlResourceSession session = db.beginResourceSession(name);
            final XmlNodeTrx wtx = session.beginNodeTrx()) {
          wtx.revertTo(revision);
          wtx.commit();
        }
      }
    }

    /** Opens a write transaction, modifies, and rolls back without committing. */
    void rollbackOnly(final Path database, final String name) {
      if (this == JSON) {
        try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(database);
            final JsonResourceSession session = db.beginResourceSession(name);
            final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild();
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"discarded\":true}"), JsonNodeTrx.Commit.NO);
          wtx.rollback();
        }
      } else {
        try (final Database<XmlResourceSession> db = Databases.openXmlDatabase(database);
            final XmlResourceSession session = db.beginResourceSession(name);
            final XmlNodeTrx wtx = session.beginNodeTrx()) {
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild();
          wtx.insertElementAsFirstChild(new QNm("discarded"));
          wtx.rollback();
        }
      }
    }

    int mostRecentRevision(final Path database, final String name) {
      if (this == JSON) {
        try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(database);
            final JsonResourceSession session = db.beginResourceSession(name)) {
          return session.getMostRecentRevisionNumber();
        }
      }
      try (final Database<XmlResourceSession> db = Databases.openXmlDatabase(database);
          final XmlResourceSession session = db.beginResourceSession(name)) {
        return session.getMostRecentRevisionNumber();
      }
    }

    /** Opens the resource through the public API; throws when the resource is refused. */
    void open(final Path database, final String name) {
      mostRecentRevision(database, name);
    }
  }

  @TempDir
  Path temporary;

  // ---------------------------------------------------------------------------------------------
  // FMT-209 / CONF-102 — the member exists on every new resource, in the specified encoding
  // ---------------------------------------------------------------------------------------------

  @ParameterizedTest
  @EnumSource(Model.class)
  @DisplayName("FMT-209, CONF-102: a new resource persists incarnationId as 16 lowercase hex digits")
  void fmt209NewResourcePersistsAnIncarnationIdInTheSpecifiedEncoding(final Model model) {
    final Path database = newDatabase(model);
    model.createResource(database, RESOURCE);

    final JsonObject settings = settings(database, RESOURCE);
    assertTrue(settings.has(MEMBER), "every 1.0 resource MUST contain the incarnationId member");
    final JsonElement member = settings.get(MEMBER);
    assertTrue(member.isJsonPrimitive() && member.getAsJsonPrimitive().isString(),
        "CONF-102: the id is a JSON string, because JSON numbers cannot hold every unsigned 64-bit value");
    assertTrue(INCARNATION_ID.matcher(member.getAsString()).matches(),
        "CONF-102: exactly 16 lowercase hexadecimal digits, was '" + member.getAsString() + "'");
  }

  @ParameterizedTest
  @EnumSource(Model.class)
  @DisplayName("FMT-209, CONF-104: independent creations draw distinct ids that vary across all 64 bits")
  void fmt209IndependentCreationsDrawDistinctUniformIds(final Model model) {
    final Path database = newDatabase(model);
    final int resources = 64;
    for (int i = 0; i < resources; i++) {
      model.createResource(database, "r" + i);
    }

    final Set<String> ids = new HashSet<>();
    long ones = 0L;
    long zeros = 0L;
    for (int i = 0; i < resources; i++) {
      final String id = incarnationId(database, "r" + i);
      assertTrue(ids.add(id), "no sentinel or reused identity: duplicate id " + id);
      final long value = Long.parseUnsignedLong(id, 16);
      ones |= value;
      zeros |= ~value;
    }
    // Under 64 independent uniform draws a bit that never varies has probability 2^-63. A counter, a
    // timestamp, a 32-bit RNG or a fixed sentinel leaves some bit constant and is caught here.
    assertEquals(-1L, ones, "every one of the 64 bits must be set in at least one draw");
    assertEquals(-1L, zeros, "every one of the 64 bits must be clear in at least one draw");
  }

  @ParameterizedTest
  @EnumSource(Model.class)
  @DisplayName("FMT-209: recreating a removed resource under the same name draws a fresh id")
  void fmt209RecreationAfterResourceRemovalDrawsAFreshId(final Model model) {
    final Path database = newDatabase(model);
    // Creation, not configuration construction, owns the fresh draw even when this object is reused.
    final ResourceConfiguration configuration = ResourceConfiguration.newBuilder(RESOURCE).build();
    model.createResource(database, configuration);
    model.commit(database, RESOURCE, 2);
    final String first = incarnationId(database, RESOURCE);

    // Whole-resource replacement is removal followed by creation of the same name (REST-210).
    model.removeResource(database, RESOURCE);
    model.createResource(database, configuration);
    final String second = incarnationId(database, RESOURCE);

    assertNotEquals(first, second, "a recreated resource restarts history and MUST carry a new incarnation id");
    assertTrue(INCARNATION_ID.matcher(second).matches());
    assertEquals(0, model.mostRecentRevision(database, RESOURCE), "recreation restarts revision numbering");
    model.removeResource(database, RESOURCE);
    model.createResource(database, configuration);
    final String third = incarnationId(database, RESOURCE);
    assertNotEquals(second, third);
    assertNotEquals(first, third);
  }

  @ParameterizedTest
  @EnumSource(Model.class)
  @DisplayName("FMT-209: recreating a deleted database and its resource draws a fresh id")
  void fmt209RecreationAfterDatabaseRemovalDrawsAFreshId(final Model model) {
    final Path database = newDatabase(model);
    model.createResource(database, RESOURCE);
    final String first = incarnationId(database, RESOURCE);

    Databases.removeDatabase(database);
    model.createDatabase(database);
    model.createResource(database, RESOURCE);

    assertNotEquals(first, incarnationId(database, RESOURCE));
  }

  @Test
  @DisplayName("FMT-209, TXN-615: an IN_MEMORY resource carries an id, and a recreation after final close draws a new one")
  void fmt209InMemoryResourceDrawsAFreshIdWhenRecreatedUnderItsFormerName() {
    final Path database = newDatabase(Model.JSON);
    final String first = createInMemoryResourceAndReadItsId(database);
    // Closing the only handle discarded the IN_MEMORY resource (TXN-615 clause 3); the name is free
    // again.
    final String second = createInMemoryResourceAndReadItsId(database);

    assertNotEquals(first, second,
        "TXN-615 clause 5: a resource created under a former name has a fresh incarnation id");
  }

  /**
   * The settings of an IN_MEMORY resource exist only while its database is open, so read them there.
   */
  private static String createInMemoryResourceAndReadItsId(final Path database) {
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(database)) {
      assertTrue(
          db.createResource(ResourceConfiguration.newBuilder(RESOURCE).storageType(StorageType.IN_MEMORY).build()));
      final JsonObject settings = settings(database, RESOURCE);
      assertTrue(settings.has(MEMBER), "an IN_MEMORY resource is a 1.0 resource and MUST contain the member");
      final String id = settings.get(MEMBER).getAsString();
      assertTrue(INCARNATION_ID.matcher(id).matches(), "malformed incarnationId '" + id + "'");
      return id;
    }
  }

  // ---------------------------------------------------------------------------------------------
  // FMT-209 — ordinary operations preserve the id
  // ---------------------------------------------------------------------------------------------

  @ParameterizedTest
  @EnumSource(Model.class)
  @DisplayName("FMT-209: commits, reverts, rollbacks and reopen preserve the id")
  void fmt209OrdinaryOperationsAndReopenPreserveTheId(final Model model) {
    final Path database = newDatabase(model);
    model.createResource(database, RESOURCE);
    final String id = incarnationId(database, RESOURCE);

    model.commit(database, RESOURCE, 3);
    assertEquals(id, incarnationId(database, RESOURCE), "commits MUST preserve the id");

    model.rollbackOnly(database, RESOURCE);
    assertEquals(id, incarnationId(database, RESOURCE), "rollback MUST preserve the id");

    model.revertTo(database, RESOURCE, 1);
    assertEquals(4, model.mostRecentRevision(database, RESOURCE), "TXN-403: a revert appends head+1");
    assertEquals(id, incarnationId(database, RESOURCE), "a revert is not a new incarnation (TXN-403)");

    // Cold reopen: a fresh database handle and session read the persisted value back.
    model.open(database, RESOURCE);
    assertEquals(id, incarnationId(database, RESOURCE), "reopen MUST preserve the id");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"0000000000000000", "0123456789abcdef", "7fffffffffffffff", "8000000000000000", "ffffffffffffffff"})
  @DisplayName("CONF-102, FMT-209: an unsigned 64-bit id, including zero and the high-bit values, survives a settings rewrite")
  void conf102SettingsRewritePreservesTheId(final String id) throws Exception {
    final Path database = newDatabase(Model.JSON);
    Model.JSON.createResource(database, RESOURCE);
    final Path resource = database.resolve("resources").resolve(RESOURCE);
    final JsonObject settings = settings(database, RESOURCE);
    settings.addProperty(MEMBER, id);
    Files.writeString(resource.resolve("ressetting.obj"), settings.toString());

    final ResourceConfiguration configuration = ResourceConfiguration.deserialize(resource);
    ResourceConfiguration.serialize(configuration);

    assertEquals(id, incarnationId(database, RESOURCE),
        "settings rewrites MUST preserve the id, whatever its unsigned 64-bit value");
  }

  // ---------------------------------------------------------------------------------------------
  // CONF-102 / FMT-209 — a settings document without a valid member is refused, never defaulted
  // ---------------------------------------------------------------------------------------------

  @ParameterizedTest
  @EnumSource(Model.class)
  @DisplayName("CONF-102, FMT-209: a settings document without incarnationId is refused and never gets a default")
  void conf102MissingMemberIsRefusedWithoutASentinelDefault(final Model model) throws Exception {
    final Path database = newDatabase(model);
    final ResourceConfiguration original = ResourceConfiguration.newBuilder(RESOURCE).build();
    model.createResource(database, original);
    final Path file = database.resolve("resources").resolve(RESOURCE).resolve("ressetting.obj");
    final JsonObject settings = settings(database, RESOURCE);
    // Control: a freshly created resource has the member and opens, so the refusal below is the
    // verdict on its absence alone.
    assertTrue(settings.has(MEMBER), "every 1.0 resource MUST contain the incarnationId member");
    model.open(database, RESOURCE);
    settings.remove(MEMBER);
    Files.writeString(file, settings.toString());
    final byte[] withoutMember = Files.readAllBytes(file);

    assertTrue(
        assertThrows(SirixIOException.class, () -> ResourceConfiguration.deserialize(file.getParent())).getMessage()
                                                                                                       .contains(
                                                                                                           MEMBER));
    assertTrue(
        assertThrows(SirixIOException.class, () -> model.open(database, RESOURCE)).getMessage().contains(MEMBER));
    assertTrue(assertThrows(SirixIOException.class, () -> ResourceConfiguration.serialize(original)).getMessage()
                                                                                                    .contains(MEMBER));
    assertTrue(assertThrows(SirixIOException.class, original::publishFreshIncarnationId).getMessage().contains(MEMBER));
    assertTrue(assertThrows(SirixIOException.class,
        () -> ResourceConfiguration.publishFreshIncarnationId(file.getParent())).getMessage().contains(MEMBER));
    assertArrayEquals(withoutMember, Files.readAllBytes(file),
        "the refusal MUST NOT repair the document by writing a default or a freshly drawn id");
  }

  static Stream<Arguments> malformedMembers() {
    return Stream.of(Arguments.of("number", new JsonPrimitive(1234567890123L)),
        Arguments.of("zero as a number", new JsonPrimitive(0)), Arguments.of("boolean", new JsonPrimitive(true)),
        Arguments.of("null", com.google.gson.JsonNull.INSTANCE), Arguments.of("empty string", new JsonPrimitive("")),
        Arguments.of("15 digits", new JsonPrimitive("0123456789abcde")),
        Arguments.of("17 digits", new JsonPrimitive("0123456789abcdef0")),
        Arguments.of("uppercase hex", new JsonPrimitive("0123456789ABCDEF")),
        Arguments.of("mixed case hex", new JsonPrimitive("0123456789abCDef")),
        Arguments.of("0x prefix", new JsonPrimitive("0x0123456789abcd")),
        Arguments.of("non-hex digits", new JsonPrimitive("ghijklmnopqrstuv")),
        Arguments.of("surrounding space", new JsonPrimitive(" 0123456789abcde")),
        Arguments.of("signed decimal", new JsonPrimitive("-123456789012345")),
        Arguments.of("hyphenated uuid", new JsonPrimitive("01234567-89ab-cdef")));
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @MethodSource("malformedMembers")
  @DisplayName("CONF-102: a numeric, wrong-length or non-lowercase-hex incarnationId is refused")
  void conf102MalformedMemberIsRefused(final String description, final JsonElement value) throws Exception {
    final Path database = newDatabase(Model.JSON);
    final ResourceConfiguration original = ResourceConfiguration.newBuilder(RESOURCE).build();
    Model.JSON.createResource(database, original);
    final Path file = database.resolve("resources").resolve(RESOURCE).resolve("ressetting.obj");
    final JsonObject settings = settings(database, RESOURCE);
    // Control: the same document with a well-formed id is accepted, so the refusals below are the
    // verdict on the value alone and not a blanket refusal of the member.
    settings.addProperty(MEMBER, "0123456789abcdef");
    Files.writeString(file, settings.toString());
    ResourceConfiguration.deserialize(file.getParent());
    Model.JSON.open(database, RESOURCE);
    settings.add(MEMBER, value);
    Files.writeString(file, settings.toString());
    final byte[] malformed = Files.readAllBytes(file);

    assertTrue(
        assertThrows(SirixIOException.class, () -> ResourceConfiguration.deserialize(file.getParent())).getMessage()
                                                                                                       .contains(
                                                                                                           MEMBER),
        description);
    assertTrue(
        assertThrows(SirixIOException.class, () -> Model.JSON.open(database, RESOURCE)).getMessage().contains(MEMBER),
        description);
    assertTrue(
        assertThrows(SirixIOException.class, () -> ResourceConfiguration.serialize(original)).getMessage()
                                                                                             .contains(MEMBER),
        description);
    assertTrue(assertThrows(SirixIOException.class, original::publishFreshIncarnationId).getMessage().contains(MEMBER),
        description);
    assertTrue(
        assertThrows(SirixIOException.class,
            () -> ResourceConfiguration.publishFreshIncarnationId(file.getParent())).getMessage().contains(MEMBER),
        description);
    assertArrayEquals(malformed, Files.readAllBytes(file), "the refusal MUST NOT rewrite the document");
  }

  // ---------------------------------------------------------------------------------------------
  // FMT-209 / TXN-404 — truncate
  // ---------------------------------------------------------------------------------------------

  @ParameterizedTest
  @EnumSource(Model.class)
  @DisplayName("FMT-209, CONF-102: stale configurations adopt the installed id on rewrite after truncate")
  void conf102StaleSettingsRewritePreservesTheTruncatedIncarnation(final Model model) {
    final Path database = newDatabase(model);
    final ResourceConfiguration original = ResourceConfiguration.newBuilder(RESOURCE).build();
    model.createResource(database, original);
    final ResourceConfiguration retained = ResourceConfiguration.deserialize(original.getResource());
    final long before = original.getIncarnationId();
    model.commit(database, RESOURCE, 3);
    model.truncateTo(database, RESOURCE, 1);
    final JsonObject installed = settings(database, RESOURCE);
    final long after = Long.parseUnsignedLong(installed.get(MEMBER).getAsString(), 16);
    assertNotEquals(before, after);

    for (final ResourceConfiguration stale : new ResourceConfiguration[] {original, retained}) {
      ResourceConfiguration.serialize(stale);
      assertEquals(after, stale.getIncarnationId(), "the serialized configuration MUST adopt the installed id");
      assertEquals(installed, settings(database, RESOURCE), "a rewrite MUST preserve the installed settings");
      try (final Database<?> reopened = model == Model.JSON
          ? Databases.openJsonDatabase(database)
          : Databases.openXmlDatabase(database); final var session = reopened.beginResourceSession(RESOURCE)) {
        assertEquals(after, session.getResourceConfig().getIncarnationId());
        assertEquals(1, session.getMostRecentRevisionNumber());
      }
    }
  }

  @ParameterizedTest
  @EnumSource(Model.class)
  @DisplayName("FMT-209, TXN-404: truncateTo draws a fresh id that persists and survives later commits and reopen")
  void txn404TruncateDrawsAFreshIdThatLaterCommitsKeep(final Model model) {
    final Path database = newDatabase(model);
    model.createResource(database, RESOURCE);
    model.commit(database, RESOURCE, 3);
    final String before = incarnationId(database, RESOURCE);

    model.truncateTo(database, RESOURCE, 1);

    final String after = incarnationId(database, RESOURCE);
    assertNotEquals(before, after, "truncateTo MUST generate a new resource incarnation id");
    assertTrue(INCARNATION_ID.matcher(after).matches());
    assertEquals(1, model.mostRecentRevision(database, RESOURCE), "truncate keeps revision 1 as the head");

    // Later commits reissue revision numbers 2 and 3; only the id distinguishes the timelines.
    model.commit(database, RESOURCE, 2);
    assertEquals(3, model.mostRecentRevision(database, RESOURCE));
    assertEquals(after, incarnationId(database, RESOURCE), "commits after a truncate keep the post-truncate id");
    model.open(database, RESOURCE);
    assertEquals(after, incarnationId(database, RESOURCE), "reopen keeps the post-truncate id");
  }

  @ParameterizedTest
  @EnumSource(Model.class)
  @DisplayName("FMT-209, TXN-404: every truncate draws its own id")
  void txn404RepeatedTruncatesDrawIndependentIds(final Model model) {
    final Path database = newDatabase(model);
    model.createResource(database, RESOURCE);
    model.commit(database, RESOURCE, 4);
    final Set<String> ids = new HashSet<>();
    assertTrue(ids.add(incarnationId(database, RESOURCE)));

    model.truncateTo(database, RESOURCE, 3);
    assertTrue(ids.add(incarnationId(database, RESOURCE)), "first truncate reused an earlier id");
    model.commit(database, RESOURCE, 1);
    model.truncateTo(database, RESOURCE, 2);
    assertTrue(ids.add(incarnationId(database, RESOURCE)), "second truncate reused an earlier id");
    model.truncateTo(database, RESOURCE, 1);
    assertTrue(ids.add(incarnationId(database, RESOURCE)), "third truncate reused an earlier id");
  }

  @ParameterizedTest
  @EnumSource(Model.class)
  @DisplayName("TXN-404: a truncate that is refused for a target that is not retained changes neither id nor history")
  void txn404RefusedTruncateLeavesTheIdUntouched(final Model model) {
    final Path database = newDatabase(model);
    model.createResource(database, RESOURCE);
    model.commit(database, RESOURCE, 2);
    final String before = incarnationId(database, RESOURCE);

    assertThrows(RuntimeException.class, () -> model.truncateTo(database, RESOURCE, 99),
        "truncateTo(r) MUST validate that r is retained");

    assertEquals(before, incarnationId(database, RESOURCE),
        "a rejected truncate MUST NOT change metadata (TXN-404), so it MUST NOT draw an id");
    assertEquals(2, model.mostRecentRevision(database, RESOURCE));
  }

  // ---------------------------------------------------------------------------------------------
  // FMT-209 / TXN-613 / TXN-614 — backup retains, restore draws
  // ---------------------------------------------------------------------------------------------

  @ParameterizedTest
  @EnumSource(Model.class)
  @DisplayName("FMT-209, TXN-613/614: a backup retains the copied id and each restore draws a fresh id per resource")
  void txn613RestoreDrawsAFreshIdForEveryRestoredResource(final Model model) throws IOException {
    final Path source = newDatabase(model);
    final String[] names = {"alpha", "beta", "gamma"};
    for (final String name : names) {
      model.createResource(source, name);
      model.commit(source, name, 2);
    }
    final String[] original = new String[names.length];
    for (int i = 0; i < names.length; i++) {
      original[i] = incarnationId(source, names[i]);
    }
    final Path backup = temporary.resolve("backup");
    BackupManager.backupDatabase(source, backup);
    // Restore also accepts cold copies; a stray file in that input is not a resource to publish.
    final Path note = Path.of("resources", "note.txt");
    Files.writeString(backup.resolve(note), "not a resource");
    for (int i = 0; i < names.length; i++) {
      assertEquals(original[i], incarnationId(backup, names[i]),
          "the backup retains the copied metadata, including the id of '" + names[i] + "'");
    }

    final Path firstRestore = temporary.resolve("restore-1");
    final Path secondRestore = temporary.resolve("restore-2");
    BackupManager.restoreDatabase(backup, firstRestore);
    BackupManager.restoreDatabase(backup, secondRestore);
    assertEquals("not a resource", Files.readString(firstRestore.resolve(note)));
    assertEquals("not a resource", Files.readString(secondRestore.resolve(note)));

    final Set<String> allIds = new HashSet<>(List.of(original));
    for (int i = 0; i < names.length; i++) {
      final String restoredOnce = incarnationId(firstRestore, names[i]);
      final String restoredTwice = incarnationId(secondRestore, names[i]);
      assertTrue(INCARNATION_ID.matcher(restoredOnce).matches());
      assertTrue(INCARNATION_ID.matcher(restoredTwice).matches());
      assertTrue(allIds.add(restoredOnce),
          "restore MUST regenerate the id of '" + names[i] + "', not reuse the source or another resource's id");
      assertTrue(allIds.add(restoredTwice),
          "repeated restore of the same backup MUST draw a fresh id each time ('" + names[i] + "')");
    }

    // The restore preserved copied history, left the source and the backup untouched, and the
    // restored resources are usable and keep their new ids across commit and reopen.
    for (int i = 0; i < names.length; i++) {
      assertEquals(original[i], incarnationId(source, names[i]), "restore MUST NOT touch the source resource");
      assertEquals(original[i], incarnationId(backup, names[i]), "restore MUST NOT rewrite the backup");
      assertEquals(2, model.mostRecentRevision(firstRestore, names[i]), "restore preserves copied history");
    }
    final String restoredAlpha = incarnationId(firstRestore, "alpha");
    model.commit(firstRestore, "alpha", 1);
    assertEquals(restoredAlpha, incarnationId(firstRestore, "alpha"), "commits after a restore preserve the new id");
  }

  @ParameterizedTest
  @EnumSource(Model.class)
  @DisplayName("FMT-209, TXN-613: a restore of a database whose resources were truncated keeps the truncate's timeline apart")
  void txn613RestoreOfATruncatedResourceDrawsAnIdDistinctFromBothTimelines(final Model model) {
    final Path source = newDatabase(model);
    model.createResource(source, RESOURCE);
    model.commit(source, RESOURCE, 3);
    final String beforeTruncate = incarnationId(source, RESOURCE);
    final Path backupOfOldTimeline = temporary.resolve("old-timeline");
    BackupManager.backupDatabase(source, backupOfOldTimeline);

    model.truncateTo(source, RESOURCE, 1);
    final String afterTruncate = incarnationId(source, RESOURCE);
    final Path restored = temporary.resolve("restored");
    BackupManager.restoreDatabase(backupOfOldTimeline, restored);

    final String restoredId = incarnationId(restored, RESOURCE);
    assertNotEquals(beforeTruncate, restoredId, "restoring an older backup MUST NOT resurrect that backup's id");
    assertNotEquals(afterTruncate, restoredId, "nor MAY it collide with the live resource's post-truncate id");
    assertEquals(3, model.mostRecentRevision(restored, RESOURCE),
        "the restored resource carries the old timeline's history");
  }

  // ---------------------------------------------------------------------------------------------
  // helpers — public surface only: the persisted settings document specified by CONF-102
  // ---------------------------------------------------------------------------------------------

  private Path newDatabase(final Model model) {
    final Path database = temporary.resolve("database-" + model.name().toLowerCase() + "-" + databaseCounter++);
    model.createDatabase(database);
    return database;
  }

  private int databaseCounter;

  private static JsonObject settings(final Path database, final String resource) {
    try {
      return JsonParser.parseString(
          Files.readString(database.resolve("resources").resolve(resource).resolve("ressetting.obj")))
                       .getAsJsonObject();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** The persisted id, asserting the member exists and is well formed. */
  private static String incarnationId(final Path database, final String resource) {
    final JsonObject settings = settings(database, resource);
    assertTrue(settings.has(MEMBER), "resource '" + resource + "' lacks the incarnationId member");
    final String id = settings.get(MEMBER).getAsString();
    assertTrue(INCARNATION_ID.matcher(id).matches(), "malformed incarnationId '" + id + "'");
    return id;
  }
}
