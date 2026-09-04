package io.sirix.query.function.jn.index;

import io.sirix.JsonTestHelper;
import io.sirix.access.Databases;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.query.AbstractJsonTest;
import org.junit.jupiter.api.Test;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Pins durable physical-slot allocation for the public JSON secondary-index creation functions. */
public final class SecondaryIndexFreshAllocationTest extends AbstractJsonTest {

  private static final String RESOURCE = "mydoc.jn";
  private static final Set<IndexType> TYPES = EnumSet.of(IndexType.PATH, IndexType.CAS, IndexType.NAME);
  private static final String CREATE_INDEXES = """
      let $doc := jn:doc('json-path1','mydoc.jn')
      let $path := jn:create-path-index($doc, ('/[]/oldName', '/[]/newName'))
      let $cas := jn:create-cas-index($doc, 'xs:string', ('/[]/oldName', '/[]/newName'))
      let $name := jn:create-name-index($doc, ('oldName', 'newName'))
      return sdb:commit($doc)
      """;

  @Test
  void recreateAfterCommittedDropUsesFreshPhysicalSlots() {
    query("jn:store('json-path1','mydoc.jn','[{\"oldName\":\"old-value\"}]')");
    query(CREATE_INDEXES);
    final Map<IndexType, Integer> firstIds = latestIds();

    dropAllSecondaryIndexes();
    Databases.clearGlobalCaches();

    query(CREATE_INDEXES);
    Databases.clearGlobalCaches();
    final Map<IndexType, Integer> recreatedIds = latestIds();

    for (final IndexType type : TYPES) {
      final int firstId = firstIds.get(type);
      final int recreatedId = recreatedIds.get(type);
      assertNotEquals(firstId, recreatedId, type + " recreated a dropped physical tree");
      assertTrue(recreatedId > firstId, type + " physical allocation did not advance");
    }
  }

  private static Map<IndexType, Integer> latestIds() {
    final Map<IndexType, Integer> ids = new EnumMap<>(IndexType.class);
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      final var controller = session.getRtxIndexController(session.getMostRecentRevisionNumber());
      for (final IndexDef indexDef : controller.getIndexes().getIndexDefs()) {
        if (TYPES.contains(indexDef.getType())) {
          final Integer duplicate = ids.put(indexDef.getType(), indexDef.getID());
          assertNull(duplicate, "expected one " + indexDef.getType() + " definition");
        }
      }
    }
    assertEquals(TYPES, ids.keySet());
    return ids;
  }

  private static void dropAllSecondaryIndexes() {
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final var controller = session.getWtxIndexController(wtx.getRevisionNumber());
      final Set<IndexDef> toDrop = controller.getIndexes()
                                             .getIndexDefs()
                                             .stream()
                                             .filter(indexDef -> TYPES.contains(indexDef.getType()))
                                             .collect(Collectors.toUnmodifiableSet());
      assertEquals(3, toDrop.size());
      controller.dropIndexes(toDrop, wtx);
      wtx.commit();
    }
  }
}
