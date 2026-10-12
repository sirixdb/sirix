package io.sirix.index.projection;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.access.trx.node.json.objectvalue.BooleanValue;
import io.sirix.access.trx.node.json.objectvalue.NumberValue;
import io.sirix.access.trx.node.json.objectvalue.ObjectRecordValue;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.io.StorageType;
import io.sirix.node.NodeKind;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Carrying a touched row group's unchanged rows over from the decoded row group persists exactly
 * what re-reading every row from the document persists.
 *
 * <p>
 * Two databases take the same randomized edit script, commit by commit. One maintains its
 * projection the ordinary way: a row the commit did not change is carried over from the decoded row
 * group, and only changed rows are read from the document. The other runs the reference behaviour,
 * which reads every row of every touched row group from the document. After each commit the two
 * projections must hold the same row groups in the same document order, byte for byte. Byte
 * equality is the claim: the reference behaviour is what the rest of the projection suite proves
 * correct, so nothing weaker transfers that proof.
 * </p>
 *
 * <p>
 * The script covers what the carry-over has to get right: value updates in place and by replacing a
 * field, fields removed and added back (absent cells), inserts at both ends and in the middle of a
 * row group, deletes, a row group that splits, per-row-group and resource-wide string dictionaries,
 * booleans, doubles, timestamps and string sets.
 * </p>
 *
 * <p>
 * It also covers the row-local witnesses a row group keeps only as one sticky flag per column,
 * because they decide whether a row can be carried over at all:
 * </p>
 * <ul>
 * <li>canonical Z-suffixed timestamps, which raise the lexical flag for every cell, and timestamps
 * without the suffix, which lack UTC provenance in every cell; both are carried over (columns
 * {@code t} and {@code tn});</li>
 * <li>a double column fed by the shredder, whose cells lack {@code Double} typing, and one fed only
 * through the API, which is pure until the script plants an integer in it and pure again after the
 * repair (columns {@code d} and {@code e}): the cells are carried over and one of them is probed
 * for the flag;</li>
 * <li>flags that name no row: a string and a truncated number in the long column, a timestamp
 * without the suffix among suffixed ones. Such a row group must fall back to reading every row, and
 * its flag must clear exactly when the reference clears it.</li>
 * </ul>
 *
 * <p>
 * It runs under every versioning algorithm, because what a commit writes for a touched index page
 * and how a later commit reads it back differs between them; the revision window is kept short so
 * the sliding snapshot and the incremental and differential chains all roll over inside the script.
 * Each algorithm runs twice, with per-row-group and with resource-wide string dictionaries.
 * </p>
 */
@Isolated
final class ProjectionUnchangedRowCarryOverTest {

  private static final int INDEX = 0;

  /** Two row groups from the start, the second one open. */
  private static final int INITIAL_RECORDS = ProjectionIndexRowGroupPage.MAX_ROWS + 280;

  private static final int COMMITS = 17;

  private static final int EDITS_PER_COMMIT = 18;

  /**
   * Ordinal, in the loaded array, of a record of the first row group that the script never deletes.
   */
  private static final int PINNED_RECORD = 40;

  /** The open row group grows past its capacity and splits. */
  private static final int SPLIT_COMMIT = 3;

  /** The witnesses whose flag names no row are planted, and repaired two commits later. */
  private static final int PLANT_UNNAMED_COMMIT = 6;
  private static final int REPAIR_UNNAMED_COMMIT = 8;

  /** One cell of the API-fed double column turns impure, among ordinary edits. */
  private static final int PLANT_BY_REPLACE_COMMIT = 9;

  /** Single-purpose commits; each isolates one way the purity flag of a carried column is settled. */
  private static final int REPAIR_BY_REBUILD_COMMIT = 11;
  private static final int PLANT_IN_PLACE_COMMIT = 12;
  private static final int REPAIR_IN_PLACE_COMMIT = 13;
  private static final int DELETE_ONLY_COMMIT = 14;
  private static final int PURE_UPDATE_ONLY_COMMIT = 15;
  private static final int REMOVE_NO_UTC_COMMIT = 16;

  private static final String[] SCALAR_FIELDS = {"n", "d", "e", "s", "b", "t", "u", "tn"};

  /** Column ordinal of {@code e}, the double column only the API writes. */
  private static final int API_DOUBLE_COLUMN = 2;

  /** Column ordinals of {@code s} and {@code u}, the scalar string columns. */
  private static final int LOW_CARDINALITY_STRING_COLUMN = 3;
  private static final int UNIQUE_STRING_COLUMN = 6;

  private static final String GLOBAL_DICTIONARY_PROPERTY = "sirix.projection.globalDict";

  /** The fields the script plants a witness in, and repairs. */
  private static final String[] WITNESS_FIELDS = {"n", "d", "e", "t"};

  private static final String[] WORDS = {"alpha", "beta", "gamma", "delta", ""};

  @TempDir
  Path directory;

  private String priorGlobalDictionaryMode;
  private boolean globalDictionaryModeSet;

  @AfterEach
  void restoreOrdinaryMaintenance() {
    if (globalDictionaryModeSet) {
      if (priorGlobalDictionaryMode == null) {
        System.clearProperty(GLOBAL_DICTIONARY_PROPERTY);
      } else {
        System.setProperty(GLOBAL_DICTIONARY_PROPERTY, priorGlobalDictionaryMode);
      }
      globalDictionaryModeSet = false;
    }
    ProjectionIndexChangeListener.reextractUnchangedRows(false);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Databases.clearGlobalCaches();
  }

  static Stream<Arguments> versioningAndStringDictionaries() {
    final Stream.Builder<Arguments> matrix = Stream.builder();
    for (final VersioningType versioning : VersioningType.values()) {
      matrix.add(Arguments.of(versioning, "never"));
      matrix.add(Arguments.of(versioning, "always"));
    }
    return matrix.build();
  }

  @ParameterizedTest(name = "{0}, resource-wide string dictionaries: {1}")
  @MethodSource("versioningAndStringDictionaries")
  void carriedRowsPersistTheSameRowGroupsAsReExtractedRows(final VersioningType versioning,
      final String globalDictionaries) throws Exception {
    // A string column keeps one dictionary per row group or resolves its cells against one
    // dictionary of the resource; the build decides when the index is created. Both are carried
    // over differently (bytes are re-interned, ids are copied), so both are run.
    priorGlobalDictionaryMode = System.getProperty(GLOBAL_DICTIONARY_PROPERTY);
    globalDictionaryModeSet = true;
    System.setProperty(GLOBAL_DICTIONARY_PROPERTY, globalDictionaries);
    final byte stringKind = "always".equals(globalDictionaries)
        ? ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL
        : ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT;
    final Path carryPath = directory.resolve("carry-" + versioning + '-' + globalDictionaries);
    final Path referencePath = directory.resolve("reference-" + versioning + '-' + globalDictionaries);
    create(carryPath, versioning);
    create(referencePath, versioning);
    try (final Database<JsonResourceSession> carryDatabase = Databases.openJsonDatabase(carryPath);
        final Database<JsonResourceSession> referenceDatabase = Databases.openJsonDatabase(referencePath);
        final JsonResourceSession carry = carryDatabase.beginResourceSession("resource");
        final JsonResourceSession reference = referenceDatabase.beginResourceSession("resource")) {
      final Model model = new Model();
      final String initial = initialRecords();
      load(carry, initial, model, false);
      load(reference, initial, null, true);
      assertEquals('-', assertSameProjection(carry, reference, "after the build"),
          "no loaded record holds the API-fed double field");
      try (final JsonNodeReadOnlyTrx reader = carry.beginNodeReadOnlyTrx(carry.getMostRecentRevisionNumber())) {
        final byte[] kinds = ProjectionIndexMetadata.requireReady(reader.getStorageEngineReader(), INDEX).columnKinds();
        assertEquals(stringKind, kinds[LOW_CARDINALITY_STRING_COLUMN], "kind of the low-cardinality string column");
        assertEquals(stringKind, kinds[UNIQUE_STRING_COLUMN], "kind of the unique string column");
      }

      // One script for every versioning algorithm: what it covers must not depend on the algorithm.
      final Random random = new Random(0x5EEDL);
      int cleanCommits = 0;
      int flaggedCommits = 0;
      final StringBuilder apiDoublePurity = new StringBuilder(COMMITS);
      for (int commit = 1; commit <= COMMITS; commit++) {
        final List<Edit> edits = plan(model, random, commit);

        final Model replayed = model.copy();
        final boolean witnessPersisted = model.holdsUnnamedWitness();
        apply(carry, edits, model, false);
        ProjectionIndexChangeListener.MaintenanceLocality locality =
            ProjectionIndexChangeListener.lastMaintenanceLocality();
        final int carriedOver = locality.rowsCarriedOver();
        final int extracted = locality.rowsExtracted();
        if (witnessPersisted) {
          // A persisted row group whose raised flag names no row reads every row.
          flaggedCommits++;
          assertTrue(extracted > 4 * edits.size(), "commit " + commit + " read only " + extracted
              + " rows although a touched row group carried a flag that names no row");
        } else {
          // Every persisted flag pins its rows down: only the records the commit changed are read.
          cleanCommits++;
          assertTrue(extracted <= edits.size(),
              "commit " + commit + " read " + extracted + " rows for " + edits.size() + " edits");
          assertTrue(carriedOver > 8 * extracted && carriedOver > 200, "commit " + commit + " carried over only "
              + carriedOver + " rows beside " + extracted + " extracted ones");
        }

        apply(reference, edits, replayed, true);
        locality = ProjectionIndexChangeListener.lastMaintenanceLocality();
        assertEquals(0, locality.rowsCarriedOver(), "the reference behaviour reads every row from the document");
        assertEquals(carriedOver + extracted, locality.rowsExtracted(),
            "both behaviours rebuild the same rows in commit " + commit);
        assertEquals(model.recordKeys, replayed.recordKeys,
            "both databases assign the same node keys, so one script addresses both");

        apiDoublePurity.append(assertSameProjection(carry, reference, "after commit " + commit));
      }
      // Non-vacuity: the script reached both the carry-over and its fallback, and the API-fed double
      // column was pure, became impure and became pure again in a row group that holds cells of it.
      assertEquals(COMMITS - 2, cleanCommits, "commits whose persisted flags pin every row down");
      assertEquals(2, flaggedCommits, "commits that start from a flag naming no row");
      assertTrue(apiDoublePurity.toString().matches("-*P+IIPIPP+"),
          "purity of the API-fed double column per commit: " + apiDoublePurity);
    }
  }

  private static void create(final Path path, final VersioningType versioning) {
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                              .storageType(StorageType.FILE_CHANNEL)
                                                              .versioningApproach(versioning)
                                                              .maxNumberOfRevisionsToRestore(3)
                                                              .buildPathStatistics(true)
                                                              .build()));
    }
  }

  private static void load(final JsonResourceSession session, final String records, final Model model,
      final boolean reextract) {
    ProjectionIndexChangeListener.reextractUnchangedRows(reextract);
    try (final JsonNodeTrx writer = session.beginNodeTrx()) {
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(records), JsonNodeTrx.Commit.NO);
      ((JsonIndexController) session.getWtxIndexController(writer.getRevisionNumber())).createIndexes(
          Set.of(definition()), writer);
      if (model != null) {
        assertTrue(writer.moveToDocumentRoot() && writer.moveToFirstChild(), "the record array");
        model.arrayKey = writer.getNodeKey();
        int ordinal = 0;
        if (writer.moveToFirstChild()) {
          do {
            model.add(model.recordKeys.size(), writer.getNodeKey(), initialTypes(ordinal++));
          } while (writer.moveToRightSibling());
        }
        assertEquals(INITIAL_RECORDS, model.recordKeys.size(), "the loaded record array");
        model.pinnedKey = model.recordKeys.get(PINNED_RECORD);
      }
      writer.commit();
    }
  }

  private static IndexDef definition() {
    return IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
        List.of(parse("/[]/n", PathParser.Type.JSON), parse("/[]/d", PathParser.Type.JSON),
            parse("/[]/e", PathParser.Type.JSON), parse("/[]/s", PathParser.Type.JSON),
            parse("/[]/b", PathParser.Type.JSON), parse("/[]/t", PathParser.Type.JSON),
            parse("/[]/u", PathParser.Type.JSON), parse("/[]/tn", PathParser.Type.JSON),
            parse("/[]/tags/[]", PathParser.Type.JSON)),
        List.of(Type.LON, Type.DBL, Type.DBL, Type.STR, Type.BOOL, Type.DATI, Type.STR, Type.DATI, Type.STR), INDEX,
        IndexDef.DbType.JSON);
  }

  // ---------------------------------------------------------------- the script

  /** One edit of the script; node keys are the same in both databases. */
  private record Edit(Kind kind, long recordKey, String field, Object value, char type, String json) {
    private enum Kind {
      /** Set the value of an existing field in place. */
      SET,
      /** Remove the field (if present) and insert it with a value of any type. */
      REPLACE,
      /** Remove the field. */
      REMOVE,
      /** Rewrite the first element of the {@code tags} array. */
      RETAG,
      /** Insert a record as the right sibling of {@code recordKey}, or first when it is {@code -1}. */
      INSERT,
      /** Delete the record. */
      DELETE
    }
  }

  private static List<Edit> plan(final Model model, final Random random, final int commit) {
    final List<Edit> edits = new ArrayList<>(EDITS_PER_COMMIT + 48);
    final Model planned = model.copy();
    // A record of the first row group that no commit deletes: the single-purpose commits edit it
    // and delete the record after it.
    final long pinned = planned.pinnedKey;
    final Map<String, Character> pinnedTypes = planned.types.get(pinned);
    final long neighbour = planned.recordKeys.get(planned.recordKeys.indexOf(pinned) + 1);
    switch (commit) {
      case REPAIR_BY_REBUILD_COMMIT -> {
        // The only impure cell of the API-fed double column is rewritten while its row group is
        // rebuilt (a neighbour is deleted): every carried cell is pure, so the flag must clear.
        edits.add(set(pinnedTypes, pinned, "e", Double.valueOf(4.5), 'f', null));
        edits.add(new Edit(Edit.Kind.DELETE, neighbour, null, null, ' ', null));
        return edits;
      }
      case PLANT_IN_PLACE_COMMIT -> {
        edits.add(set(pinnedTypes, pinned, "e", Long.valueOf(11), 'l', null));
        return edits;
      }
      case REPAIR_IN_PLACE_COMMIT -> {
        // The same through a column patch: no row moves, the one impure cell becomes pure.
        edits.add(set(pinnedTypes, pinned, "e", Double.valueOf(6.5), 'f', null));
        return edits;
      }
      case DELETE_ONLY_COMMIT -> {
        // A rebuilt row group that takes no extracted row at all: the purity flag of its
        // shredder-fed double column can only come from a carried cell.
        edits.add(new Edit(Edit.Kind.DELETE, neighbour, null, null, ' ', null));
        return edits;
      }
      case PURE_UPDATE_ONLY_COMMIT -> {
        // A column patch whose one extracted cell is Double-typed beside shredded ones.
        edits.add(set(pinnedTypes, pinned, "d", Double.valueOf(8.5), 'f', null));
        return edits;
      }
      case REMOVE_NO_UTC_COMMIT -> {
        // A column patch whose one extracted cell is absent: the missing UTC provenance of the
        // unsuffixed timestamps beside it can only come from carried cells.
        pinnedTypes.remove("tn");
        edits.add(new Edit(Edit.Kind.REMOVE, pinned, "tn", null, ' ', null));
        return edits;
      }
      default -> {
        // An ordinary commit of mixed edits, below.
      }
    }
    for (int i = 0; i < EDITS_PER_COMMIT; i++) {
      final int choice = random.nextInt(100);
      final long recordKey = planned.recordKeys.get(random.nextInt(planned.recordKeys.size()));
      final Map<String, Character> types = planned.types.get(recordKey);
      if (choice < 50) {
        final String field = SCALAR_FIELDS[random.nextInt(SCALAR_FIELDS.length)];
        final Object value = cleanValue(field, random, commit * 1000 + i);
        edits.add(set(types, recordKey, field, value, cleanType(field), random));
      } else if (choice < 60) {
        final String field = SCALAR_FIELDS[random.nextInt(SCALAR_FIELDS.length)];
        if (types.remove(field) != null) {
          edits.add(new Edit(Edit.Kind.REMOVE, recordKey, field, null, ' ', null));
        }
      } else if (choice < 68) {
        if (types.containsKey("tags")) {
          edits.add(new Edit(Edit.Kind.RETAG, recordKey, "tags", "tag" + random.nextInt(9), 'a', null));
        }
      } else if (choice < 90) {
        final int ordinal = 1_000_000 + commit * 1000 + i;
        final int position = random.nextInt(3);
        final long anchor = position == 0
            ? -1L
            : position == 1
                ? planned.recordKeys.get(planned.recordKeys.size() - 1)
                : recordKey;
        // The key is assigned when the edit is applied, so later edits of this commit do not address
        // the new record.
        edits.add(new Edit(Edit.Kind.INSERT, anchor, null, initialTypes(ordinal), ' ', record(ordinal)));
      } else if (recordKey != pinned && planned.recordKeys.size() > INITIAL_RECORDS / 2) {
        edits.add(new Edit(Edit.Kind.DELETE, recordKey, null, null, ' ', null));
        planned.remove(recordKey);
      }
    }
    if (commit == SPLIT_COMMIT) {
      // Grow the open row group past its capacity in one commit, so a row group splits.
      final long last = planned.recordKeys.get(planned.recordKeys.size() - 1);
      for (int i = 0; i < 40; i++) {
        final int ordinal = 2_000_000 + i;
        edits.add(new Edit(Edit.Kind.INSERT, last, null, initialTypes(ordinal), ' ', record(ordinal)));
      }
    }
    if (commit == PLANT_UNNAMED_COMMIT) {
      // Plant the witnesses whose flag names no row, in records spread over the record set: a string
      // and a truncated number in the long column, a timestamp without the Z suffix.
      final Object[][] witnesses =
          {{"n", "not a number", 'x'}, {"n", Double.valueOf(12.25), 'f'}, {"t", "2026-03-05T10:00:00", 'w'}};
      for (final Object[] witness : witnesses) {
        final long recordKey = planned.recordKeys.get(random.nextInt(planned.recordKeys.size()));
        edits.add(set(planned.types.get(recordKey), recordKey, (String) witness[0], witness[1], (Character) witness[2],
            random));
      }
    }
    if (commit == REPAIR_UNNAMED_COMMIT) {
      for (final long recordKey : planned.recordKeys) {
        final Map<String, Character> types = planned.types.get(recordKey);
        for (final String field : new String[] {"n", "t"}) {
          final Character type = types.get(field);
          if (type != null && type != cleanType(field)) {
            edits.add(set(types, recordKey, field, cleanValue(field, random, commit), cleanType(field), random));
          }
        }
      }
    }
    if (commit == PLANT_BY_REPLACE_COMMIT) {
      // An integer in the API-fed double column: the column turns impure in one cell.
      edits.add(set(pinnedTypes, pinned, "e", Long.valueOf(9), 'l', random));
    }
    return edits;
  }

  /**
   * Give {@code field} a value: in place when the field exists with the same JSON type (half of the
   * time with a {@code random}, always without one), else by removing it and inserting it again.
   */
  private static Edit set(final Map<String, Character> types, final long recordKey, final String field,
      final Object value, final char type, final Random random) {
    final Character prior = types.get(field);
    final boolean inPlace =
        prior != null && jsonKind(prior) == jsonKind(type) && (random == null || random.nextBoolean());
    types.put(field, type);
    return new Edit(inPlace
        ? Edit.Kind.SET
        : Edit.Kind.REPLACE, recordKey, field, value, type, null);
  }

  /** The type a field has when its cell is clean. */
  private static char cleanType(final String field) {
    return switch (field) {
      case "n" -> 'l';
      case "d", "e" -> 'f';
      case "b" -> 'b';
      default -> 's';
    };
  }

  /** JSON kind of a model type: number, boolean, string or array. */
  private static char jsonKind(final char type) {
    return switch (type) {
      case 'l', 'f' -> 'n';
      case 'b' -> 'b';
      case 'a' -> 'a';
      default -> 's';
    };
  }

  private static Object cleanValue(final String field, final Random random, final int salt) {
    return switch (field) {
      case "n" -> Long.valueOf(random.nextInt(100_000) - 500);
      case "d", "e" -> Double.valueOf(random.nextInt(10_000) / 8.0);
      case "s" -> WORDS[random.nextInt(WORDS.length)];
      case "b" -> Boolean.valueOf(random.nextBoolean());
      case "t" ->
        "2026-0" + (1 + random.nextInt(9)) + "-1" + random.nextInt(10) + "T0" + random.nextInt(10) + ":00:00Z";
      case "tn" -> "2026-0" + (1 + random.nextInt(9)) + "-2" + random.nextInt(9) + "T1" + random.nextInt(10) + ":30:00";
      default -> "unique-" + salt + '-' + random.nextInt(1_000_000);
    };
  }

  @SuppressWarnings("unchecked")
  private static void apply(final JsonResourceSession session, final List<Edit> edits, final Model model,
      final boolean reextract) {
    ProjectionIndexChangeListener.reextractUnchangedRows(reextract);
    try (final JsonNodeTrx writer = session.beginNodeTrx()) {
      for (final Edit edit : edits) {
        switch (edit.kind()) {
          case SET -> {
            moveToFieldValue(writer, edit.recordKey(), edit.field());
            switch (edit.value()) {
              case Boolean flag -> writer.setBooleanValue(flag);
              case Number number -> writer.setNumberValue(number);
              default -> writer.setStringValue((String) edit.value());
            }
          }
          case REPLACE -> {
            if (moveToField(writer, edit.recordKey(), edit.field())) {
              writer.remove();
            }
            assertTrue(writer.moveTo(edit.recordKey()), "record " + edit.recordKey());
            final ObjectRecordValue<?> value = switch (edit.value()) {
              case Boolean flag -> new BooleanValue(flag);
              case Number number -> new NumberValue(number);
              default -> new StringValue((String) edit.value());
            };
            writer.insertObjectRecordAsFirstChild(edit.field(), value);
          }
          case REMOVE -> {
            assertTrue(moveToField(writer, edit.recordKey(), edit.field()), "field " + edit.field());
            writer.remove();
          }
          case RETAG -> {
            assertTrue(moveToField(writer, edit.recordKey(), "tags"), "tags of record " + edit.recordKey());
            assertEquals(NodeKind.OBJECT_NAMED_ARRAY, writer.getKind(), "an array field is one named array node");
            if (writer.moveToFirstChild()) {
              writer.setStringValue((String) edit.value());
            } else {
              writer.insertStringValueAsFirstChild((String) edit.value());
            }
          }
          case INSERT -> {
            final int at;
            if (edit.recordKey() < 0) {
              assertTrue(writer.moveTo(model.arrayKey), "the record array");
              writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(edit.json()), JsonNodeTrx.Commit.NO);
              at = 0;
            } else {
              assertTrue(writer.moveTo(edit.recordKey()), "record " + edit.recordKey());
              writer.insertSubtreeAsRightSibling(JsonShredder.createStringReader(edit.json()), JsonNodeTrx.Commit.NO);
              at = model.recordKeys.indexOf(edit.recordKey()) + 1;
            }
            model.add(at, writer.getNodeKey(), (Map<String, Character>) edit.value());
          }
          case DELETE -> {
            assertTrue(writer.moveTo(edit.recordKey()), "record " + edit.recordKey());
            writer.remove();
            model.remove(edit.recordKey());
          }
          default -> throw new AssertionError(edit.kind());
        }
        if (edit.kind() == Edit.Kind.SET || edit.kind() == Edit.Kind.REPLACE) {
          model.types.get(edit.recordKey()).put(edit.field(), edit.type());
        } else if (edit.kind() == Edit.Kind.REMOVE) {
          model.types.get(edit.recordKey()).remove(edit.field());
        }
      }
      writer.commit();
    }
  }

  /** Move to the object-key node (or fused named node) of {@code field}; {@code false} if absent. */
  private static boolean moveToField(final JsonNodeTrx writer, final long recordKey, final String field) {
    assertTrue(writer.moveTo(recordKey), "record " + recordKey);
    if (!writer.moveToFirstChild()) {
      return false;
    }
    do {
      if (writer.getName() != null && field.equals(writer.getName().getLocalName())) {
        return true;
      }
    } while (writer.moveToRightSibling());
    return false;
  }

  /**
   * Move to the value of {@code field}. A primitive object record is stored either as an object key
   * with a value child or as one fused named node; both report the field name, only the former has a
   * child to enter.
   */
  private static void moveToFieldValue(final JsonNodeTrx writer, final long recordKey, final String field) {
    assertTrue(moveToField(writer, recordKey, field), "field " + field + " of record " + recordKey);
    if (writer.hasFirstChild()) {
      writer.moveToFirstChild();
    }
  }

  // ---------------------------------------------------------------- the fixture's records

  private static String initialRecords() {
    final StringBuilder json = new StringBuilder(INITIAL_RECORDS * 120).append('[');
    for (int i = 0; i < INITIAL_RECORDS; i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append(record(i));
    }
    return json.append(']').toString();
  }

  /** A record with every field clean; every seventh lacks {@code s}, every fifth has no tags. */
  private static String record(final int i) {
    final StringBuilder json = new StringBuilder(120).append("{\"n\":").append(i * 3L - 7);
    json.append(",\"d\":").append(i / 4.0 + 0.5);
    if (i % 7 != 0) {
      json.append(",\"s\":\"").append(WORDS[i % WORDS.length]).append('"');
    }
    json.append(",\"b\":").append(i % 3 == 0);
    json.append(",\"t\":\"2026-01-").append(10 + i % 18).append("T00:00:00Z\"");
    json.append(",\"u\":\"unique-").append(i).append('"');
    json.append(",\"tn\":\"2026-02-").append(10 + i % 18).append("T12:00:00\"");
    if (i % 5 != 0) {
      json.append(",\"tags\":[\"tag").append(i % 4).append('"');
      if (i % 2 == 0) {
        json.append(",\"tag").append(i % 6).append('"');
      }
      json.append(']');
    }
    return json.append('}').toString();
  }

  private static Map<String, Character> initialTypes(final int i) {
    final Map<String, Character> types = new HashMap<>(8);
    types.put("n", 'l');
    types.put("d", 'f');
    if (i % 7 != 0) {
      types.put("s", 's');
    }
    types.put("b", 'b');
    types.put("t", 's');
    types.put("u", 's');
    types.put("tn", 's');
    if (i % 5 != 0) {
      types.put("tags", 'a');
    }
    return types;
  }

  /**
   * The records in document order and the JSON type of each field, which decides how it is edited.
   */
  private static final class Model {
    private long arrayKey;
    private long pinnedKey;
    private final List<Long> recordKeys = new ArrayList<>(INITIAL_RECORDS + 256);
    private final Map<Long, Map<String, Character>> types = new HashMap<>(INITIAL_RECORDS * 2);

    private void add(final int at, final long recordKey, final Map<String, Character> fieldTypes) {
      recordKeys.add(at, recordKey);
      types.put(recordKey, new HashMap<>(fieldTypes));
    }

    private void remove(final long recordKey) {
      recordKeys.remove(Long.valueOf(recordKey));
      types.remove(recordKey);
    }

    /**
     * Whether some record holds a value whose sticky column flag names no row: anything but an integer
     * in the long column, or a timestamp without the Z suffix.
     */
    private boolean holdsUnnamedWitness() {
      for (final Map<String, Character> fieldTypes : types.values()) {
        final Character number = fieldTypes.get("n");
        final Character timestamp = fieldTypes.get("t");
        if (number != null && number != 'l' || timestamp != null && timestamp == 'w') {
          return true;
        }
      }
      return false;
    }

    private Model copy() {
      final Model copy = new Model();
      copy.arrayKey = arrayKey;
      copy.pinnedKey = pinnedKey;
      copy.recordKeys.addAll(recordKeys);
      for (final Map.Entry<Long, Map<String, Character>> entry : types.entrySet()) {
        copy.types.put(entry.getKey(), new HashMap<>(entry.getValue()));
      }
      return copy;
    }
  }

  // ---------------------------------------------------------------- the comparison

  /**
   * @return {@code 'I'} when some row group of the reference projection holds an impure cell in the
   *         API-fed double column, {@code 'P'} when the column is pure wherever it holds a cell, and
   *         {@code '-'} when no row group holds a cell of it
   */
  private static char assertSameProjection(final JsonResourceSession carry, final JsonResourceSession reference,
      final String when) {
    final int revision = carry.getMostRecentRevisionNumber();
    assertEquals(revision, reference.getMostRecentRevisionNumber(), "both databases commit in step");
    try (final JsonNodeReadOnlyTrx carryReader = carry.beginNodeReadOnlyTrx(revision);
        final JsonNodeReadOnlyTrx referenceReader = reference.beginNodeReadOnlyTrx(revision)) {
      final StorageEngineReader carryStorage = carryReader.getStorageEngineReader();
      final StorageEngineReader referenceStorage = referenceReader.getStorageEngineReader();
      final ProjectionIndexMetadata carryMetadata = ProjectionIndexMetadata.requireReady(carryStorage, INDEX);
      final ProjectionIndexMetadata referenceMetadata = ProjectionIndexMetadata.requireReady(referenceStorage, INDEX);
      assertEquals(referenceMetadata.rowGroupCount(), carryMetadata.rowGroupCount(), "row groups " + when);
      assertArrayEquals(referenceMetadata.columnKinds(), carryMetadata.columnKinds(), "column kinds " + when);
      final int[] order =
          ProjectionIndexFences.readPhysicalOrder(referenceStorage, INDEX, referenceMetadata.rowGroupCount());
      assertArrayEquals(order,
          ProjectionIndexFences.readPhysicalOrder(carryStorage, INDEX, carryMetadata.rowGroupCount()),
          "document order of the row groups " + when);
      assertTrue(order.length >= 2, "the fixture spans several row groups");
      char purity = '-';
      for (final int slot : order) {
        final byte[] expected =
            ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(referenceStorage, INDEX, slot);
        assertNotNull(expected, "reference row group " + slot + ' ' + when);
        assertArrayEquals(expected,
            ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(carryStorage, INDEX, slot),
            "row group " + slot + ' ' + when + " differs from the re-extracted one");
        final ProjectionIndexRowGroupPage page = ProjectionIndexRowGroupPage.deserialize(expected);
        boolean holdsCell = false;
        for (final long word : page.presenceColumnBits(API_DOUBLE_COLUMN)) {
          holdsCell |= word != 0;
        }
        if (holdsCell && !page.columnPureDoubleSource(API_DOUBLE_COLUMN)) {
          purity = 'I';
        } else if (holdsCell && purity == '-') {
          purity = 'P';
        }
      }
      return purity;
    }
  }
}
