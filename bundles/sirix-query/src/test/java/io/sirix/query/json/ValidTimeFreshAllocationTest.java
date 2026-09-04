package io.sirix.query.json;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.ValidTimeConfig;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/** Pins fresh VALIDTIME/CAS slot allocation in both shared valid-time creation paths. */
public final class ValidTimeFreshAllocationTest {

  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();
  private static final String RESOURCE = JsonTestHelper.RESOURCE;
  private static final String VALID_FROM = "validFrom";
  private static final String VALID_TO = "validTo";
  private static final String DOCUMENT = """
      [{"id":1,"validFrom":"2020-01-01T00:00:00Z","validTo":"2020-12-31T23:59:59Z"}]
      """;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  void autoAndExplicitRecreationNeverReuseDroppedPhysicalSlots() {
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    final ValidTimeConfig validTimeConfig = new ValidTimeConfig(VALID_FROM, VALID_TO);
    final Allocation initial;
    final Allocation recreated;
    final int explicitValidTimeId;

    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .validTimePaths(VALID_FROM, VALID_TO)
                                                   .buildPathSummary(true)
                                                   .build());

      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(DOCUMENT), JsonNodeTrx.Commit.NO);
        ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, wtx, null);
        wtx.commit();
      }
      initial = latestAllocation(database);
      assertEquals(2, initial.casIds().size(), "initial auto-create must reserve two distinct CAS slots");

      drop(database, Set.of(IndexType.VALIDTIME, IndexType.CAS));
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, wtx, null);
        wtx.commit();
      }
      recreated = latestAllocation(database);

      assertNotEquals(initial.validTimeId(), recreated.validTimeId());
      assertEquals(2, recreated.casIds().size(), "auto-create must reserve two distinct CAS slots");
      final Set<Integer> reusedCasIds = new HashSet<>(initial.casIds());
      reusedCasIds.retainAll(recreated.casIds());
      assertEquals(Set.of(), reusedCasIds, "auto-create reused a dropped CAS physical tree");

      drop(database, Set.of(IndexType.VALIDTIME));
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        final JsonIndexController controller = session.getWtxIndexController(wtx.getRevisionNumber());
        final IndexDef explicit = ValidTimeIndexes.createIntervalIndex(controller, wtx,
            ValidTimeIndexes.defaultPaths(validTimeConfig), null, RESOURCE);
        explicitValidTimeId = explicit.getID();
        wtx.commit();
      }
      assertNotEquals(initial.validTimeId(), explicitValidTimeId);
      assertNotEquals(recreated.validTimeId(), explicitValidTimeId);
    }

    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> reopened = Databases.openJsonDatabase(DATABASE_PATH)) {
      final Allocation cold = latestAllocation(reopened);
      assertEquals(explicitValidTimeId, cold.validTimeId());
      assertEquals(recreated.casIds(), cold.casIds());
      assertFalse(cold.casIds().containsAll(initial.casIds()), "cold catalog resurrected dropped CAS definitions");
    }
  }

  private static void drop(final Database<JsonResourceSession> database, final Set<IndexType> types) {
    try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final var controller = session.getWtxIndexController(wtx.getRevisionNumber());
      final Set<IndexDef> toDrop = controller.getIndexes()
                                             .getIndexDefs()
                                             .stream()
                                             .filter(indexDef -> types.contains(indexDef.getType()))
                                             .collect(Collectors.toUnmodifiableSet());
      assertFalse(toDrop.isEmpty(), "drop fixture found no matching definitions");
      controller.dropIndexes(toDrop, wtx);
      wtx.commit();
    }
  }

  private static Allocation latestAllocation(final Database<JsonResourceSession> database) {
    try (JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      final var controller = session.getRtxIndexController(session.getMostRecentRevisionNumber());
      int validTimeId = -1;
      final Set<Integer> casIds = new HashSet<>();
      for (final IndexDef indexDef : controller.getIndexes().getIndexDefs()) {
        if (indexDef.getType() == IndexType.VALIDTIME) {
          assertEquals(-1, validTimeId, "expected one VALIDTIME definition");
          validTimeId = indexDef.getID();
        } else if (indexDef.getType() == IndexType.CAS) {
          casIds.add(indexDef.getID());
        }
      }
      assertNotEquals(-1, validTimeId, "missing VALIDTIME definition");
      return new Allocation(validTimeId, Set.copyOf(casIds));
    }
  }

  private record Allocation(int validTimeId, Set<Integer> casIds) {
  }
}
