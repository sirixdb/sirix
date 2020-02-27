package org.sirix.index;

import com.google.common.collect.ImmutableSet;
import org.brackit.xquery.atomic.Dbl;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.xdm.Type;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.JsonTestHelper;
import org.sirix.index.avltree.AVLTreeReader;
import org.sirix.index.avltree.keyvalue.CASValue;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.index.path.json.JsonPCRCollector;
import org.sirix.service.json.shredder.JsonShredder;
import org.sirix.service.xml.shredder.InsertPosition;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

import static org.brackit.xquery.util.path.Path.parse;
import static org.junit.Assert.*;

public final class JsonAVLTreeIntegrationTest {
  private static final Path JSON = Paths.get("src", "test", "resources", "json");

  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @After
  public void tearDown() {
    JsonTestHelper.closeEverything();
  }

  @Test
  public void testCreateNameIndexWhileListeningAndNameIndexOnDemand() {
    final var jsonPath = JSON.resolve("abc-location-stations.json");
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final var trx = manager.beginNodeTrx()) {
      var indexController = manager.getWtxIndexController(trx.getRevisionNumber() - 1);

      final var allObjectKeyNames = IndexDefs.createNameIdxDef(0, IndexDefs.NameIndexType.JSON);

      indexController.createIndexes(ImmutableSet.of(allObjectKeyNames), trx);

      final var shredder = new JsonShredder.Builder(trx, JsonShredder.createFileReader(jsonPath),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();

      final var allStreetAddressesAndTwitterAccounts =
          indexController.openNameIndex(trx.getPageTrx(), allObjectKeyNames,
              indexController.createNameFilter(Set.of("streetaddress", "twitteraccount")));

      assertTrue(allStreetAddressesAndTwitterAccounts.hasNext());

      final var allStreetAddressesNodeReferences = allStreetAddressesAndTwitterAccounts.next();

      assertEquals(53, allStreetAddressesNodeReferences.getNodeKeys().size());
      assertTrue(allStreetAddressesAndTwitterAccounts.hasNext());

      final var allTwitterAccountsNodeReferences = allStreetAddressesAndTwitterAccounts.next();
      assertEquals(53, allTwitterAccountsNodeReferences.getNodeKeys().size());

      final var allObjectKeyNamesExceptStreetAddress =
          IndexDefs.createFilteredNameIdxDef(Set.of(new QNm("streetaddress")), 1, IndexDefs.NameIndexType.JSON);

      indexController.createIndexes(ImmutableSet.of(allObjectKeyNamesExceptStreetAddress), trx);

      final var allTwitterAccounts =
          indexController.openNameIndex(trx.getPageTrx(), allObjectKeyNamesExceptStreetAddress,
              indexController.createNameFilter(Set.of("streetaddress", "twitteraccount")));

      assertTrue(allTwitterAccounts.hasNext());
      final var allTwitterAccounts2NodeReferences = allTwitterAccounts.next();
      assertEquals(53, allTwitterAccounts2NodeReferences.getNodeKeys().size());

      assertFalse(allTwitterAccounts.hasNext());

      final var allStreetAddresses =
          IndexDefs.createSelectiveNameIdxDef(Set.of(new QNm("streetaddress")), 1, IndexDefs.NameIndexType.JSON);

      indexController.createIndexes(ImmutableSet.of(allStreetAddresses), trx);

      final var allStreetAddressesIndex =
          indexController.openNameIndex(trx.getPageTrx(), allObjectKeyNamesExceptStreetAddress,
              indexController.createNameFilter(Set.of("streetaddress")));

      assertTrue(allStreetAddressesIndex.hasNext());
      final var allStreetAddressesIndexNodeReferences = allStreetAddressesIndex.next();
      assertEquals(53, allStreetAddressesIndexNodeReferences.getNodeKeys().size());

      assertFalse(allStreetAddressesIndex.hasNext());
    }
  }

  @Test
  public void testCreateCASIndexWhileListeningAndCASIndexOnDemand() {
    final var jsonPath = JSON.resolve("abc-location-stations.json");
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final var trx = manager.beginNodeTrx()) {
      var indexController = manager.getWtxIndexController(trx.getRevisionNumber() - 1);

      final var pathToFeatureType = parse("/features/__array__/type");

      final var idxDefOfFeatureType =
          IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(pathToFeatureType), 0);

      indexController.createIndexes(ImmutableSet.of(idxDefOfFeatureType), trx);

      final var shredder = new JsonShredder.Builder(trx, JsonShredder.createFileReader(jsonPath),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();

      final var indexDef = indexController.getIndexes().getIndexDef(0, IndexType.CAS);

      AVLTreeReader<CASValue, NodeReferences> reader =
          AVLTreeReader.getInstance(trx.getPageTrx(), indexDef.getType(), indexDef.getID());

      final var pathNodeKeys = trx.getPathSummary().getPCRsForPath(pathToFeatureType, false);

      assertEquals(1, pathNodeKeys.size());

      final var references =
          reader.get(new CASValue(new Str("Feature"), Type.STR, pathNodeKeys.iterator().next()), SearchMode.EQUAL);

      assertTrue(references.isPresent());
      assertEquals(53, references.get().getNodeKeys().size());

      final var pathToName = parse("/features/__array__/properties/name");
      final var idxDefOfPathToName = IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(pathToName), 1);

      indexController.createIndexes(ImmutableSet.of(idxDefOfPathToName), trx);

      final var casIndexDef = indexController.getIndexes().getIndexDef(1, IndexType.CAS);

      final var index = indexController.openCASIndex(trx.getPageTrx(), casIndexDef,
          indexController.createCASFilter(new String[] { "/features/__array__/properties/name" },
              new Str("ABC Radio Adelaide"), SearchMode.EQUAL, new JsonPCRCollector(trx)));

      assertTrue(index.hasNext());

      index.forEachRemaining(nodeReferences -> {
        assertEquals(1, nodeReferences.getNodeKeys().size());
        for (final long nodeKey : nodeReferences.getNodeKeys()) {
          trx.moveTo(nodeKey);
          assertEquals("ABC Radio Adelaide", trx.getValue());
        }
      });

      final var indexWithAllEntries = indexController.openCASIndex(trx.getPageTrx(), casIndexDef,
          indexController.createCASFilter(new String[] {}, null, SearchMode.EQUAL, new JsonPCRCollector(trx)));

      assertTrue(indexWithAllEntries.hasNext());

      final var stream =
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(indexWithAllEntries, Spliterator.ORDERED), false);

      assertEquals(53, stream.count());

      final var pathToCoordinates = parse("/features/__array__/geometry/coordinates/__array__");
      final var idxDefOfPathToCoordinates =
          IndexDefs.createCASIdxDef(false, Type.DEC, Collections.singleton(pathToCoordinates), 2);

      indexController.createIndexes(ImmutableSet.of(idxDefOfPathToCoordinates), trx);

      final var casIndexDefForCoordinates = indexController.getIndexes().findCASIndex(pathToCoordinates, Type.DEC);

      final var casIndexForCoordinates = indexController.openCASIndex(trx.getPageTrx(), casIndexDefForCoordinates.get(),
          indexController.createCASFilterRange(new String[] { "/features/__array__/geometry/coordinates/__array__" },
              new Dbl(0), new Dbl(160), true, true, new JsonPCRCollector(trx)));

      assertTrue(casIndexForCoordinates.hasNext());

      final var streamOfCasIndexForCoordinates =
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(casIndexForCoordinates, Spliterator.ORDERED), false);

      assertEquals(53, streamOfCasIndexForCoordinates.count());
    }
  }

  @Test
  public void testPathIndexWhileListeningAndPathIndexOnDemand() {
    final var jsonPath = JSON.resolve("abc-location-stations.json");
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final var trx = manager.beginNodeTrx()) {
      var indexController = manager.getWtxIndexController(trx.getRevisionNumber() - 1);

      final var pathToFeatureType = parse("/features/__array__/type");

      final var idxDefOfFeatureType = IndexDefs.createPathIdxDef(Collections.singleton(pathToFeatureType), 0);

      indexController.createIndexes(ImmutableSet.of(idxDefOfFeatureType), trx);

      final var shredder = new JsonShredder.Builder(trx, JsonShredder.createFileReader(jsonPath),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();

      final var indexDef = indexController.getIndexes().getIndexDef(0, IndexType.PATH);

      AVLTreeReader<Long, NodeReferences> reader =
          AVLTreeReader.getInstance(trx.getPageTrx(), indexDef.getType(), indexDef.getID());

      final var pathNodeKeys = trx.getPathSummary().getPCRsForPath(pathToFeatureType, false);

      assertEquals(1, pathNodeKeys.size());

      final var references = reader.get(pathNodeKeys.iterator().next(), SearchMode.EQUAL);

      assertTrue(references.isPresent());
      assertEquals(53, references.get().getNodeKeys().size());

      final var pathToName = parse("/features/__array__/properties/name");
      final var idxDefOfPathToName = IndexDefs.createPathIdxDef(Collections.singleton(pathToName), 1);

      indexController.createIndexes(ImmutableSet.of(idxDefOfPathToName), trx);

      final var pathIndexDef = indexController.getIndexes().getIndexDef(1, IndexType.PATH);

      final var index = indexController.openPathIndex(trx.getPageTrx(), pathIndexDef, null);

      assertTrue(index.hasNext());

      index.forEachRemaining(nodeReferences -> {
        assertEquals(53, nodeReferences.getNodeKeys().size());
        for (final long nodeKey : nodeReferences.getNodeKeys()) {
          trx.moveTo(nodeKey);
          assertEquals("name", trx.getName().getLocalName());
        }
      });
    }
  }
}
