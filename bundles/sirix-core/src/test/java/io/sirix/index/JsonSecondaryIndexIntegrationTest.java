package io.sirix.index;

import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.index.hot.CASKeySerializer;
import io.sirix.index.hot.HOTIndexReader;
import io.sirix.index.hot.HOTLongIndexReader;
import io.sirix.index.hot.NameKeySerializer;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JsonShredder;
import io.brackit.query.atomic.Dbl;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.sirix.JsonTestHelper;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public final class JsonSecondaryIndexIntegrationTest {
  private static final Path JSON = Paths.get("src", "test", "resources", "json");

  /**
   * Reflects the {@code sirix.json.fuseNamedPrimitives} system property at class-load time. When
   * fusion is on, the shredder collapses each primitive-valued object field into a single
   * OBJECT_NAMED_* record, halving the node count for those fields and shifting the nodekey of
   * records that follow fused fields in document order.
   */
  private static final boolean FUSE_NAMED_PRIMITIVES = true;

  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @After
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  public void testCreateCASIndexWhileListeningAndCASIndexOnDemandWithInvalidQName() {
    final var jsonPath = JSON.resolve("business-service-providers.json");
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx()) {
      var indexController = manager.getWtxIndexController(trx.getRevisionNumber());

      final var pathToGetSummary =
          parse("/paths/\\/business_service_providers\\/search/get/summary", PathParser.Type.JSON);

      final var idxDefOfFeatureType =
          IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(pathToGetSummary), 0, IndexDef.DbType.JSON);

      indexController.createIndexes(Set.of(idxDefOfFeatureType), trx);

      final var shredder = new JsonShredder.Builder(trx, JsonShredder.createFileReader(jsonPath),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();

      final var pathNodeKeys = trx.getPathSummary().getPCRsForPath(pathToGetSummary);

      assertEquals(1, pathNodeKeys.size());

      final var casIndexForGetSummary = indexController.openCASIndex(trx.getStorageEngineReader(), idxDefOfFeatureType,
          indexController.createCASFilter(Set.of("/paths/\\/business_service_providers\\/search/get/summary"),
              new Str("Business Service Providers API"), SearchMode.EQUAL, new JsonPCRCollector(trx)));

      assertTrue(casIndexForGetSummary.hasNext());

      final var nodeReferences = casIndexForGetSummary.next();

      // Legacy shred produces OBJECT_KEY + OBJECT_STRING_VALUE for each primitive field, so the
      // summary-string node lands at nodekey 29. With iter#32 P1+P2 fusion every primitive-valued
      // AND structural-valued field upstream of "summary" (swagger, info+title+description+version,
      // host, schemes+[0], basePath, produces+[0], paths→search→get) each collapse to a single
      // OBJECT_NAMED_* record — pulling the target nodekey down to 16.
      final long expectedNodeKey = FUSE_NAMED_PRIMITIVES
          ? 16L
          : 29L;
      assertEquals("nodeKey should match", expectedNodeKey, (long) nodeReferences.getNodeKeys().iterator().next());
      assertFalse(casIndexForGetSummary.hasNext());
    }
  }

  @Test
  public void testCreateNameIndexWhileListeningAndNameIndexOnDemand() {
    final var jsonPath = JSON.resolve("abc-location-stations.json");
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = session.beginNodeTrx()) {
      var indexController = session.getWtxIndexController(trx.getRevisionNumber());

      final var allObjectKeyNames = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);

      indexController.createIndexes(Set.of(allObjectKeyNames), trx);

      final var shredder = new JsonShredder.Builder(trx, JsonShredder.createFileReader(jsonPath),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();

      final var allStreetAddressesAndTwitterAccounts = indexController.openNameIndex(trx.getStorageEngineReader(),
          allObjectKeyNames, indexController.createNameFilter(Set.of("streetaddress", "twitteraccount")));

      assertTrue(allStreetAddressesAndTwitterAccounts.hasNext());

      final var allStreetAddressesNodeReferences = allStreetAddressesAndTwitterAccounts.next();
      assertEquals(53, allStreetAddressesNodeReferences.getNodeKeys().getLongCardinality());

      assertTrue(allStreetAddressesAndTwitterAccounts.hasNext());

      final var allTwitterAccountsNodeReferences = allStreetAddressesAndTwitterAccounts.next();
      assertEquals(53, allTwitterAccountsNodeReferences.getNodeKeys().getLongCardinality());

      final var allObjectKeyNamesExceptStreetAddress =
          IndexDefs.createFilteredNameIdxDef(Set.of(new QNm("streetaddress")), 1, IndexDef.DbType.JSON);

      indexController.createIndexes(Set.of(allObjectKeyNamesExceptStreetAddress), trx);

      final var allTwitterAccounts =
          indexController.openNameIndex(trx.getStorageEngineReader(), allObjectKeyNamesExceptStreetAddress,
              indexController.createNameFilter(Set.of("streetaddress", "twitteraccount")));

      assertTrue(allTwitterAccounts.hasNext());
      final var allTwitterAccounts2NodeReferences = allTwitterAccounts.next();
      assertEquals(53, allTwitterAccounts2NodeReferences.getNodeKeys().getLongCardinality());

      assertFalse(allTwitterAccounts.hasNext());

      final var allStreetAddresses =
          IndexDefs.createSelectiveNameIdxDef(Set.of(new QNm("streetaddress")), 2, IndexDef.DbType.JSON);

      indexController.createIndexes(Set.of(allStreetAddresses), trx);

      final var allStreetAddressesIndex = indexController.openNameIndex(trx.getStorageEngineReader(),
          allStreetAddresses, indexController.createNameFilter(Set.of("streetaddress")));

      assertTrue(allStreetAddressesIndex.hasNext());
      final var allStreetAddressesIndexNodeReferences = allStreetAddressesIndex.next();
      assertEquals(53, allStreetAddressesIndexNodeReferences.getNodeKeys().getLongCardinality());

      assertFalse(allStreetAddressesIndex.hasNext());

      final HOTIndexReader<QNm> allObjectKeyNamesIndexReader = HOTIndexReader.create(trx.getStorageEngineReader(),
          NameKeySerializer.INSTANCE, allObjectKeyNames.getType(), allObjectKeyNames.getID());
      final var name = new QNm("streetaddress");
      final NodeReferences exactStreetAddress = allObjectKeyNamesIndexReader.get(name, SearchMode.EQUAL);
      assertNotNull(exactStreetAddress);
      assertEquals(53, exactStreetAddress.getNodeKeys().getLongCardinality());

      final var greater = allObjectKeyNamesIndexReader.iteratorFrom(name, false);
      assertTrue(greater.hasNext());
      // HOT range iteration follows the canonical unsigned serialized-key order. The document's
      // top-level timeStamp key is the immediate successor of streetaddress; twitteraccount follows
      // later in the same range.
      assertEquals("timeStamp", greater.next().getKey().getLocalName());
      assertFalse(allObjectKeyNamesIndexReader.iteratorFrom(new QNm("type"), false).hasNext());

      final var greaterOrEqual = allObjectKeyNamesIndexReader.iteratorFrom(name, true);
      assertTrue(greaterOrEqual.hasNext());
      assertEquals("streetaddress", greaterOrEqual.next().getKey().getLocalName());

      final var lower = allObjectKeyNamesIndexReader.iteratorTo(name, false);
      QNm immediatePredecessor = null;
      while (lower.hasNext()) {
        immediatePredecessor = lower.next().getKey();
      }
      assertNotNull(immediatePredecessor);
      assertEquals("siteurl", immediatePredecessor.getLocalName());

      final var lowerOrEqual = allObjectKeyNamesIndexReader.iteratorTo(name, true);
      QNm lastAtOrBelow = null;
      while (lowerOrEqual.hasNext()) {
        lastAtOrBelow = lowerOrEqual.next().getKey();
      }
      assertNotNull(lastAtOrBelow);
      assertEquals("streetaddress", lastAtOrBelow.getLocalName());

      final var nameIndex = indexController.getIndexes().findNameIndex(new QNm("twitteraccount"), new QNm("type"));

      assertTrue(nameIndex.isPresent());
    }
  }

  @Test
  public void testCreateCASIndexWhileListeningAndCASIndexOnDemand() {
    final var jsonPath = JSON.resolve("abc-location-stations.json");
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx()) {
      var indexController = manager.getWtxIndexController(trx.getRevisionNumber());

      final var pathToFeatureType = parse("/features/[]/type", PathParser.Type.JSON);

      final var idxDefOfFeatureType =
          IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(pathToFeatureType), 0, IndexDef.DbType.JSON);

      indexController.createIndexes(Set.of(idxDefOfFeatureType), trx);

      final var shredder = new JsonShredder.Builder(trx, JsonShredder.createFileReader(jsonPath),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();

      final var indexDef = indexController.getIndexes().getIndexDef(0, IndexType.CAS);

      final HOTIndexReader<CASValue> reader = HOTIndexReader.create(trx.getStorageEngineReader(),
          CASKeySerializer.INSTANCE, indexDef.getType(), indexDef.getID());

      final var pathNodeKeys = trx.getPathSummary().getPCRsForPath(pathToFeatureType);

      assertEquals(1, pathNodeKeys.size());

      final var references =
          reader.get(new CASValue(new Str("Feature"), Type.STR, pathNodeKeys.iterator().next()), SearchMode.EQUAL);

      assertNotNull(references);
      assertEquals(53, references.getNodeKeys().getLongCardinality());

      final var pathToName = parse("/features/[]/properties/name", PathParser.Type.JSON);
      final var idxDefOfPathToName =
          IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(pathToName), 1, IndexDef.DbType.JSON);

      indexController.createIndexes(Set.of(idxDefOfPathToName), trx);

      final var casIndexDef = indexController.getIndexes().getIndexDef(1, IndexType.CAS);

      final var index = indexController.openCASIndex(trx.getStorageEngineReader(), casIndexDef,
          indexController.createCASFilter(Set.of("/features/[]/properties/name"), new Str("ABC Radio Adelaide"),
              SearchMode.EQUAL, new JsonPCRCollector(trx)));

      assertTrue(index.hasNext());

      index.forEachRemaining(nodeReferences -> {
        assertEquals(1, nodeReferences.getNodeKeys().getLongCardinality());
        final long nodeKey = nodeReferences.getNodeKeys().getLongIterator().next();
        trx.moveTo(nodeKey);
        assertEquals("ABC Radio Adelaide", trx.getValue());
      });

      final var indexWithAllEntries = indexController.openCASIndex(trx.getStorageEngineReader(), casIndexDef,
          indexController.createCASFilter(Set.of(), null, SearchMode.EQUAL, new JsonPCRCollector(trx)));

      assertTrue(indexWithAllEntries.hasNext());

      final var stream =
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(indexWithAllEntries, Spliterator.ORDERED), false);

      assertEquals(53, stream.count());

      final var pathToCoordinates = parse("/features/[]/geometry/coordinates/[]", PathParser.Type.JSON);
      final var idxDefOfPathToCoordinates =
          IndexDefs.createCASIdxDef(false, Type.DEC, Collections.singleton(pathToCoordinates), 2, IndexDef.DbType.JSON);

      indexController.createIndexes(Set.of(idxDefOfPathToCoordinates), trx);

      final var casIndexDefForCoordinates = indexController.getIndexes().findCASIndex(pathToCoordinates, Type.DEC);

      final var casIndexForCoordinates =
          indexController.openCASIndex(trx.getStorageEngineReader(), casIndexDefForCoordinates.get(),
              indexController.createCASFilterRange(Set.of("/features/[]/geometry/coordinates/[]"), new Dbl(0),
                  new Dbl(160), true, true, new JsonPCRCollector(trx)));

      assertTrue(casIndexForCoordinates.hasNext());

      final var streamOfCasIndexForCoordinates =
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(casIndexForCoordinates, Spliterator.ORDERED), false);

      assertEquals(53, streamOfCasIndexForCoordinates.count());

      final var casIndex = indexController.getIndexes().findCASIndex(pathToFeatureType, Type.STR);

      assertTrue(casIndex.isPresent());

      final var pathToGeometry = parse("/features/[]/geometry", PathParser.Type.JSON);

      final var idxDefOfThreePaths = IndexDefs.createCASIdxDef(false, Type.STR,
          Set.of(pathToFeatureType, pathToGeometry, pathToCoordinates), 3, IndexDef.DbType.JSON);

      indexController.createIndexes(Set.of(idxDefOfThreePaths), trx);

      final var casIndexDefOfGeometryPath = indexController.getIndexes().findCASIndex(pathToGeometry, Type.STR);

      assertTrue(casIndexDefOfGeometryPath.isPresent());

      final var casIndexForGeometry = indexController.openCASIndex(trx.getStorageEngineReader(),
          casIndexDefOfGeometryPath.get(), indexController.createCASFilter(Set.of("/features/[]/geometry"),
              new Str("bla"), SearchMode.EQUAL, new JsonPCRCollector(trx)));

      assertFalse(casIndexForGeometry.hasNext());

      final var casIndexForGeometryCoordinates = indexController.openCASIndex(trx.getStorageEngineReader(),
          idxDefOfThreePaths, indexController.createCASFilter(Set.of("/features/[]/geometry/coordinates/[]"),
              new Str("0"), SearchMode.GREATER, new JsonPCRCollector(trx)));

      assertTrue(casIndexForGeometryCoordinates.hasNext());

      assertEquals(53, StreamSupport
                                    .stream(Spliterators.spliteratorUnknownSize(casIndexForGeometryCoordinates,
                                        Spliterator.ORDERED), false)
                                    .count());
    }
  }

  @Test
  public void testPathIndexWhileListeningAndPathIndexOnDemand() {
    final var jsonPath = JSON.resolve("abc-location-stations.json");
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx()) {
      var indexController = manager.getWtxIndexController(trx.getRevisionNumber());

      final var pathToFeatureType = parse("/features/[]/type", PathParser.Type.JSON);

      final var idxDefOfFeatureType =
          IndexDefs.createPathIdxDef(Collections.singleton(pathToFeatureType), 0, IndexDef.DbType.JSON);

      indexController.createIndexes(Set.of(idxDefOfFeatureType), trx);

      final var shredder = new JsonShredder.Builder(trx, JsonShredder.createFileReader(jsonPath),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();

      final var indexDef = indexController.getIndexes().getIndexDef(0, IndexType.PATH);

      final HOTLongIndexReader reader =
          HOTLongIndexReader.create(trx.getStorageEngineReader(), indexDef.getType(), indexDef.getID());

      final var pathNodeKeys = trx.getPathSummary().getPCRsForPath(pathToFeatureType);

      assertEquals(1, pathNodeKeys.size());

      final var references = reader.get(pathNodeKeys.iterator().nextLong(), SearchMode.EQUAL);

      assertNotNull(references);
      assertEquals(53, references.getNodeKeys().getLongCardinality());

      final var pathToName = parse("/features/[]/properties/name", PathParser.Type.JSON);
      final var idxDefOfPathToName =
          IndexDefs.createPathIdxDef(Collections.singleton(pathToName), 1, IndexDef.DbType.JSON);

      indexController.createIndexes(Set.of(idxDefOfPathToName), trx);

      final var pathIndexDef = indexController.getIndexes().getIndexDef(1, IndexType.PATH);

      final var index = indexController.openPathIndex(trx.getStorageEngineReader(), pathIndexDef, null);

      assertTrue(index.hasNext());

      index.forEachRemaining(nodeReferences -> {
        assertEquals(53, nodeReferences.getNodeKeys().getLongCardinality());
        final var nodeKeyIter = nodeReferences.getNodeKeys().getLongIterator();
        while (nodeKeyIter.hasNext()) {
          final long nodeKey = nodeKeyIter.next();
          trx.moveTo(nodeKey);
          assertEquals("name", trx.getName().getLocalName());
        }
      });

      final var pathIndex = indexController.getIndexes().findPathIndex(pathToFeatureType);

      assertTrue(pathIndex.isPresent());
    }
  }

  /** Exercises repeated canonical HOT point and range reads through a read-only transaction. */
  @Test
  public void testReadOnlyTransactionLookupsForNameIndex() {
    final var jsonPath = JSON.resolve("abc-location-stations.json");
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

    // First, create index and shred data using write transaction
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

      final var allObjectKeyNames = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
      indexController.createIndexes(Set.of(allObjectKeyNames), wtx);

      final var shredder = new JsonShredder.Builder(wtx, JsonShredder.createFileReader(jsonPath),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();
    }

    // Query through a committed read-only transaction.
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var rtx = session.beginNodeReadOnlyTrx()) {
      var indexController = session.getRtxIndexController(rtx.getRevisionNumber());

      final var allObjectKeyNamesOpt = indexController.getIndexes().findNameIndex();
      assertTrue("Name index should exist", allObjectKeyNamesOpt.isPresent());
      final var allObjectKeyNames = allObjectKeyNamesOpt.get();

      final HOTIndexReader<QNm> reader = HOTIndexReader.create(rtx.getStorageEngineReader(), NameKeySerializer.INSTANCE,
          allObjectKeyNames.getType(), allObjectKeyNames.getID());

      // Query multiple times to exercise cache hit path
      final var name = new QNm("streetaddress");

      NodeReferences nodeResult = reader.get(name, SearchMode.EQUAL);
      assertNotNull(nodeResult);
      assertEquals(53, nodeResult.getNodeKeys().getLongCardinality());

      nodeResult = reader.get(name, SearchMode.EQUAL);
      assertNotNull(nodeResult);
      assertEquals(53, nodeResult.getNodeKeys().getLongCardinality());

      // Query for different key - exercises tree traversal with cache lookups
      final var nameTwitter = new QNm("twitteraccount");
      final NodeReferences nodeResultTwitter = reader.get(nameTwitter, SearchMode.EQUAL);
      assertNotNull(nodeResultTwitter);
      assertEquals(53, nodeResultTwitter.getNodeKeys().getLongCardinality());

      // Test range query which traverses multiple nodes
      final var nodeGreater = reader.iteratorFrom(name, false);
      assertTrue(nodeGreater.hasNext());
      assertEquals("timeStamp", nodeGreater.next().getKey().getLocalName());

      final var nodeLess = reader.iteratorTo(name, false);
      QNm predecessor = null;
      while (nodeLess.hasNext()) {
        predecessor = nodeLess.next().getKey();
      }
      assertNotNull(predecessor);
      assertEquals("siteurl", predecessor.getLocalName());
    }
  }
}
