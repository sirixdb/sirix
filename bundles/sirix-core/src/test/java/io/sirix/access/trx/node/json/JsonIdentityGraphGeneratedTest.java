package io.sirix.access.trx.node.json;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.objectvalue.ArrayValue;
import io.sirix.access.trx.node.json.objectvalue.BooleanValue;
import io.sirix.access.trx.node.json.objectvalue.NullValue;
import io.sirix.access.trx.node.json.objectvalue.NumberValue;
import io.sirix.access.trx.node.json.objectvalue.ObjectRecordValue;
import io.sirix.access.trx.node.json.objectvalue.ObjectValue;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.DescendantAxis;
import io.sirix.io.StorageType;
import io.sirix.node.NodeKind;
import io.sirix.service.json.replay.JsonIdentityDelta;
import io.sirix.service.json.replay.JsonIdentityDeltaReader;
import io.sirix.service.json.replay.JsonReplayGraphValidator;
import io.sirix.service.json.replay.JsonReplaySnapshotOracle;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static io.sirix.access.trx.node.json.JsonIdentityImportTest.assertPaths;
import static io.sirix.access.trx.node.json.JsonIdentityImportTest.assertSnapshot;
import static io.sirix.access.trx.node.json.JsonStructuralHashInvariantTest.assertGraph;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Fixed, serializable operation streams with state-relative selectors and bounded delta debugging.
 */
@Isolated
final class JsonIdentityGraphGeneratedTest {
  private static final long[] SEEDS = {0x16A11CEL, 0x5EED18L};

  @TempDir
  Path directory;
  private int attempt;

  private enum Kind {
    INSERT, UPDATE, REMOVE, MOVE, REPLACE, RENAME, RESERVE, NOOP, RESTORE, ROLLBACK, CLEAR, LATER_PARENT, EQUAL_SWAP
  }

  private record Op(Kind kind, int selector, int value, int position) {
  }

  private record Configuration(VersioningType versioning, HashType hash, boolean dewey, AfterCommitState mode,
      int threshold) {
  }

  static Stream<Arguments> configurations() {
    return JsonIdentityEpochTest.commitModes().flatMap(arguments -> Stream.of(0, 3).map(threshold -> {
      final Object[] values = arguments.get();
      return Arguments.of(new Configuration((VersioningType) values[0], (HashType) values[1], (boolean) values[2],
          (AfterCommitState) values[3], threshold));
    })).limit(Long.getLong("sirix.replay.generated.configurations", Long.MAX_VALUE));
  }

  @ParameterizedTest
  @MethodSource("configurations")
  void generatedIdentityHistoryMatchesColdSnapshots(final Configuration config) throws Exception {
    for (int seedIndex = 0; seedIndex < SEEDS.length; seedIndex++) {
      final long seed = SEEDS[seedIndex];
      final boolean diffs = seedIndex == 0;
      final List<Op> operations = generate(seed);
      try {
        verify(config, operations, diffs);
      } catch (final AssertionError | RuntimeException failure) {
        final List<Op> minimal = shrink(config, operations, diffs, fingerprint(failure));
        fail("Identity history failure: seed=" + seed + ", config=" + config + ", diffs=" + diffs + "\noperations="
            + operations + "\nminimal=" + minimal, failure);
      }
    }
  }

  private static List<Op> generate(final long seed) {
    final var random = new Random(seed);
    final var operations = new ArrayList<Op>();
    // Every stream includes all insertion positions and the two identity-sensitive move classes.
    for (int position = 0; position < 4; position++) {
      operations.add(new Op(Kind.INSERT, position, position, position));
    }
    operations.add(new Op(Kind.EQUAL_SWAP, 0, 0, 0));
    operations.add(new Op(Kind.LATER_PARENT, 0, 0, 0));
    for (final Kind kind : List.of(Kind.UPDATE, Kind.RENAME, Kind.REPLACE, Kind.MOVE, Kind.REMOVE, Kind.RESERVE,
        Kind.NOOP, Kind.RESTORE, Kind.ROLLBACK)) {
      operations.add(new Op(kind, random.nextInt(32), random.nextInt(8), random.nextInt(4)));
    }
    for (int index = 0; index < 10; index++) {
      operations.add(new Op(Kind.values()[random.nextInt(Kind.values().length)], random.nextInt(32), random.nextInt(8),
          random.nextInt(4)));
    }
    operations.add(new Op(Kind.CLEAR, 0, 0, 0));
    operations.add(new Op(Kind.NOOP, 0, 0, 0));
    operations.add(new Op(Kind.RESTORE, 0, 0, 0));
    return List.copyOf(operations);
  }

  private List<Op> shrink(final Configuration config, final List<Op> original, final boolean diffs,
      final String failureFingerprint) throws IOException {
    List<Op> minimal = original;
    int remaining = 16;
    for (int width = Math.max(1, minimal.size() / 2); width > 0 && remaining > 0; width /= 2) {
      for (int start = 0; start + width <= minimal.size() && remaining > 0;) {
        final var candidate = new ArrayList<>(minimal);
        candidate.subList(start, start + width).clear();
        remaining--;
        try {
          verify(config, candidate, diffs);
          start += width;
        } catch (final AssertionError | RuntimeException failure) {
          if (failureFingerprint.equals(fingerprint(failure))) {
            minimal = List.copyOf(candidate);
          } else {
            start += width;
          }
        }
      }
    }
    return minimal;
  }

  private static String fingerprint(final Throwable failure) {
    return failure.getClass().getName() + Arrays.stream(failure.getStackTrace())
                                                .filter(frame -> frame.getClassName().startsWith("io.sirix."))
                                                .findFirst()
                                                .map(StackTraceElement::toString)
                                                .orElse("");
  }

  private void verify(final Configuration config, final List<Op> operations, final boolean diffs) throws IOException {
    final Path trial = directory.resolve("attempt-" + attempt++);
    final Path sourcePath = trial.resolve("source");
    Files.createDirectories(trial);
    try (final var database = create(sourcePath, config, diffs);
        final var source = database.beginResourceSession("resource");
        final var writer = source.beginNodeTrx(config.threshold(), config.mode())) {
      try {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[0,0,{\"x\":[1,{\"y\":true}]}]"),
            JsonNodeTrx.Commit.NO);
        writer.commit();
        for (int index = 0; index < operations.size(); index++) {
          apply(writer, source, operations.get(index), index);
          writer.commit();
        }
      } catch (final RuntimeException | Error failure) {
        writer.rollback();
        throw failure;
      }
    }
    JsonIdentityIndexOracle.clearCaches();
    Databases.clearGlobalCaches();
    // All three sidecar states read the same authoritative immutable history. The second fixed
    // seed builds its history with diffs disabled, so that contract is tested independently too.
    for (int sidecars = 0; sidecars < 3; sidecars++) {
      final Path targetPath = trial.resolve("target-" + sidecars);
      try (final var sourceDb = Databases.openJsonDatabase(sourcePath);
          final var targetDb = create(targetPath, config, diffs);
          final var source = sourceDb.beginResourceSession("resource");
          final var target = targetDb.beginResourceSession("resource");
          final var writer = target.beginNodeTrx(config.threshold(), config.mode())) {
        alterSidecars(source, sidecars);
        JsonIdentityIndexOracle.declare(writer);
        for (int revision = 1; revision <= source.getMostRecentRevisionNumber(); revision++) {
          try (final var reader = source.beginNodeReadOnlyTrx(revision)) {
            assertGraph(reader, config.hash());
            JsonReplayGraphValidator.validate(reader);
            final JsonIdentityDelta delta;
            if (revision == 1) {
              delta = JsonIdentityDeltaReader.snapshot(reader, 1);
            } else {
              try (final var base = source.beginNodeReadOnlyTrx(revision - 1)) {
                delta = JsonIdentityDeltaReader.between(base, reader, revision);
                final var reference = JsonReplaySnapshotOracle.between(base, reader, revision);
                assertEquals(reference.manifest(), delta.manifest());
                assertEquals(reference.puts(), delta.puts(), "changed records, revision=" + revision);
                assertEquals(reference.deletes(), delta.deletes(), "deleted records, revision=" + revision);
              }
            }
            ((InternalJsonNodeTrx) writer).importRevision(delta, reader);
            assertEquals(revision, target.getMostRecentRevisionNumber());
          }
        }
      }
      JsonIdentityIndexOracle.clearCaches();
      Databases.clearGlobalCaches();
      try (final var sourceDb = Databases.openJsonDatabase(sourcePath);
          final var targetDb = Databases.openJsonDatabase(targetPath);
          final var source = sourceDb.beginResourceSession("resource");
          final var target = targetDb.beginResourceSession("resource")) {
        assertEquals(source.getMostRecentRevisionNumber(), target.getMostRecentRevisionNumber());
        for (int revision = 1; revision <= source.getMostRecentRevisionNumber(); revision++) {
          try (final var original = source.beginNodeReadOnlyTrx(revision);
              final var copied = target.beginNodeReadOnlyTrx(revision)) {
            assertSnapshot(original, copied, 0);
            JsonIdentityIndexOracle.assertIndexes(original, copied);
            assertGraph(copied, config.hash());
            JsonReplayGraphValidator.validate(copied);
          }
          assertPaths(source, revision, target, revision);
        }
      }
      Databases.removeDatabase(targetPath);
    }
    Databases.removeDatabase(sourcePath);
  }

  private static void alterSidecars(final JsonResourceSession source, final int mode) throws IOException {
    if (mode == 0) {
      return;
    }
    final Path updates = source.getResourceConfig()
                               .getResource()
                               .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath());
    Files.createDirectories(updates);
    try (final var files = Files.list(updates)) {
      for (final Path file : files.toList()) {
        Files.delete(file);
      }
    }
    if (mode == 2) {
      for (int revision = 1; revision <= source.getMostRecentRevisionNumber(); revision++) {
        Files.writeString(updates.resolve("diffFromRev" + (revision - 1) + "toRev" + revision + ".json"),
            "{corrupt public sidecar: identity replay must not read me");
      }
    }
  }

  private static Database<JsonResourceSession> create(final Path path, final Configuration config,
      final boolean diffs) {
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    final var database = Databases.openJsonDatabase(path);
    database.createResource(ResourceConfiguration.newBuilder("resource")
                                                 .storageType(StorageType.FILE_CHANNEL)
                                                 .versioningApproach(config.versioning())
                                                 .hashKind(config.hash())
                                                 .useDeweyIDs(config.dewey())
                                                 .buildPathStatistics(true)
                                                 .storeDiffs(diffs)
                                                 .build());
    return database;
  }

  private static void apply(final JsonNodeTrx writer, final JsonResourceSession source, final Op op,
      final int sequence) {
    switch (op.kind()) {
      case INSERT -> insert(writer, op, sequence);
      case UPDATE -> {
        if (select(writer, op.selector(),
            kind -> kind == NodeKind.STRING_VALUE || kind == NodeKind.OBJECT_NAMED_STRING
                || kind == NodeKind.NUMBER_VALUE || kind == NodeKind.OBJECT_NAMED_NUMBER
                || kind == NodeKind.BOOLEAN_VALUE || kind == NodeKind.OBJECT_NAMED_BOOLEAN)) {
          switch (writer.getKind()) {
            case STRING_VALUE, OBJECT_NAMED_STRING -> writer.setStringValue("雪-" + op.value());
            case NUMBER_VALUE, OBJECT_NAMED_NUMBER -> writer.setNumberValue(op.value());
            case BOOLEAN_VALUE, OBJECT_NAMED_BOOLEAN -> writer.setBooleanValue(op.value() % 2 == 0);
            default -> throw new AssertionError("unexpected selected primitive");
          }
        }
      }
      case REMOVE -> {
        if (select(writer, op.selector(), kind -> true)) {
          writer.remove();
        }
      }
      case MOVE -> move(writer, op);
      case REPLACE -> {
        if (select(writer, op.selector(), NodeKind::playsObjectKeyRole)) {
          writer.replaceObjectRecordValue(value(op.value()));
        }
      }
      case RENAME -> {
        if (select(writer, op.selector(), NodeKind::playsObjectKeyRole)) {
          writer.setObjectKeyName("renamed-" + sequence);
        }
      }
      case RESERVE -> writer.getStorageEngineReader()
                            .getActualRevisionRootPage()
                            .setMaxNodeKeyInDocumentIndex(writer.getMaxNodeKey() + (op.value() + 1L) * 1_000_000_000L);
      case NOOP -> {
      }
      case RESTORE -> writer.revertTo(1 + op.selector() % source.getMostRecentRevisionNumber());
      case ROLLBACK -> {
        insert(writer, op, sequence);
        writer.rollback();
      }
      case CLEAR -> {
        writer.moveToDocumentRoot();
        if (writer.moveToFirstChild()) {
          writer.remove();
        }
      }
      case LATER_PARENT -> laterParent(writer, sequence);
      case EQUAL_SWAP -> equalSwap(writer);
    }
  }

  private static void insert(final JsonNodeTrx writer, final Op op, final int sequence) {
    writer.moveToDocumentRoot();
    if (!writer.hasFirstChild()) {
      writer.insertArrayAsFirstChild();
    }
    if (!select(writer, op.selector(), JsonIdentityGraphGeneratedTest::container)) {
      return;
    }
    final boolean object = object(writer.getKind());
    int position = op.position();
    if (position >= 2) {
      if (!writer.moveToFirstChild()) {
        position = 0;
      } else {
        for (int step = 0; step < op.value() && writer.hasRightSibling(); step++) {
          writer.moveToRightSibling();
        }
      }
    }
    if (object) {
      final String name = "field-" + sequence;
      final ObjectRecordValue<?> value = value(op.value());
      switch (position) {
        case 0 -> writer.insertObjectRecordAsFirstChild(name, value);
        case 1 -> writer.insertObjectRecordAsLastChild(name, value);
        case 2 -> writer.insertObjectRecordAsLeftSibling(name, value);
        case 3 -> writer.insertObjectRecordAsRightSibling(name, value);
        default -> throw new AssertionError(position);
      }
    } else {
      final String json = switch (op.value() % 4) {
        case 0 -> "[7,7]";
        case 1 -> "{\"Aa\":1,\"BB\":null,\"nested\":[true,\"雪\"]}";
        case 2 -> "{\"value\":\"same\"}";
        default -> "[false,null,3.5]";
      };
      switch (position) {
        case 0 -> writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
        case 1 -> writer.insertSubtreeAsLastChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
        case 2 -> writer.insertSubtreeAsLeftSibling(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
        case 3 -> writer.insertSubtreeAsRightSibling(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
        default -> throw new AssertionError(position);
      }
    }
  }

  private static void move(final JsonNodeTrx writer, final Op op) {
    if (!select(writer, op.selector(), kind -> true) || writer.getParentKey() == 0) {
      return;
    }
    final long from = writer.getNodeKey();
    final NodeKind kind = writer.getKind();
    if (!select(writer, op.value(), parent -> JsonReplayGraphValidator.compatible(parent, kind))) {
      return;
    }
    final long parent = writer.getNodeKey();
    do {
      if (writer.getNodeKey() == from) {
        return;
      }
    } while (writer.moveToParent());
    assertTrue(writer.moveTo(parent));
    if (op.position() < 2 || !writer.hasFirstChild()) {
      writer.moveSubtreeToFirstChild(from);
    } else if (writer.moveToFirstChild() && writer.getNodeKey() != from) {
      if (op.position() == 2) {
        writer.moveSubtreeToLeftSibling(from);
      } else {
        writer.moveSubtreeToRightSibling(from);
      }
    }
  }

  private static void laterParent(final JsonNodeTrx writer, final int sequence) {
    if (!select(writer, 0, JsonIdentityGraphGeneratedTest::array)) {
      return;
    }
    final long array = writer.getNodeKey();
    writer.insertObjectAsFirstChild();
    final long oldParent = writer.getNodeKey();
    writer.insertObjectRecordAsFirstChild("later-" + sequence, new NumberValue(1));
    final long field = writer.getNodeKey();
    assertTrue(writer.moveTo(array));
    writer.insertObjectAsLastChild();
    writer.moveSubtreeToFirstChild(field);
    assertTrue(writer.moveTo(oldParent));
    writer.remove();
  }

  private static void equalSwap(final JsonNodeTrx writer) {
    if (!select(writer, 0, JsonIdentityGraphGeneratedTest::array)) {
      return;
    }
    final long parent = writer.getNodeKey();
    writer.insertArrayAsFirstChild();
    final long first = writer.getNodeKey();
    writer.insertNumberValueAsFirstChild(7);
    assertTrue(writer.moveTo(parent));
    writer.insertArrayAsLastChild();
    final long second = writer.getNodeKey();
    writer.insertNumberValueAsFirstChild(7);
    assertTrue(writer.moveTo(first));
    writer.moveSubtreeToLeftSibling(second);
  }

  private static boolean select(final JsonNodeReadOnlyTrx reader, final int selector,
      final Predicate<NodeKind> predicate) {
    reader.moveToDocumentRoot();
    final var keys = new LongArrayList();
    final var axis = new DescendantAxis(reader);
    while (axis.hasNext()) {
      final long key = axis.nextLong();
      if (predicate.test(reader.getKind())) {
        keys.add(key);
      }
    }
    return !keys.isEmpty() && reader.moveTo(keys.getLong(selector % keys.size()));
  }

  private static ObjectRecordValue<?> value(final int selector) {
    return switch (selector % 6) {
      case 0 -> new NumberValue(selector);
      case 1 -> new StringValue("雪-" + selector);
      case 2 -> new BooleanValue(true);
      case 3 -> new NullValue();
      case 4 -> new ArrayValue();
      default -> new ObjectValue();
    };
  }

  private static boolean container(final NodeKind kind) {
    return array(kind) || object(kind);
  }

  private static boolean array(final NodeKind kind) {
    return kind == NodeKind.ARRAY || kind == NodeKind.OBJECT_NAMED_ARRAY;
  }

  private static boolean object(final NodeKind kind) {
    return kind == NodeKind.OBJECT || kind == NodeKind.OBJECT_NAMED_OBJECT;
  }
}
