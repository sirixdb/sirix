package io.sirix.index.vector;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.DocumentException;
import io.brackit.query.jdm.node.Node;
import io.brackit.query.util.path.Path;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for vector index definition creation, materialization, and deserialization.
 */
class VectorIndexDefTest {

  @Test
  void testCreateVectorIndexDefWithDefaultParams() {
    final Set<Path<QNm>> paths = new HashSet<>();
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(128, "L2", paths, 0, IndexDef.DbType.JSON);

    assertNotNull(indexDef);
    assertTrue(indexDef.isVectorIndex());
    assertEquals(IndexType.VECTOR, indexDef.getType());
    assertEquals(128, indexDef.getDimension());
    assertEquals("L2", indexDef.getDistanceType());
    assertEquals(16, indexDef.getHnswM());
    assertEquals(200, indexDef.getHnswEfConstruction());
    assertEquals(0, indexDef.getID());
  }

  @Test
  void testCreateVectorIndexDefWithCustomHnswParams() {
    final Set<Path<QNm>> paths = new HashSet<>();
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(256, "COSINE", paths, 32, 400, 1, IndexDef.DbType.JSON);

    assertNotNull(indexDef);
    assertTrue(indexDef.isVectorIndex());
    assertEquals(256, indexDef.getDimension());
    assertEquals("COSINE", indexDef.getDistanceType());
    assertEquals(32, indexDef.getHnswM());
    assertEquals(400, indexDef.getHnswEfConstruction());
    assertEquals(1, indexDef.getID());
  }

  @Test
  void testIsVectorIndexReturnsTrue() {
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(64, "INNER_PRODUCT", Set.of(), 0, IndexDef.DbType.JSON);
    assertTrue(indexDef.isVectorIndex());
  }

  @Test
  void testOtherIndexTypeMethodsReturnFalseForVector() {
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(64, "L2", Set.of(), 0, IndexDef.DbType.JSON);
    assertFalse(indexDef.isPathIndex());
    assertFalse(indexDef.isCasIndex());
    assertFalse(indexDef.isNameIndex());
  }

  @Test
  void testMaterializeAndInitRoundtrip() throws DocumentException {
    final Set<Path<QNm>> paths = new HashSet<>();
    final IndexDef original = IndexDefs.createVectorIdxDef(128, "L2", paths, 16, 200, 5, IndexDef.DbType.JSON);

    // Materialize to node tree
    final Node<?> materialized = original.materialize();
    assertNotNull(materialized);

    // Deserialize back via init()
    final IndexDef restored = new IndexDef(IndexDef.DbType.JSON);
    restored.init(materialized);

    assertEquals(IndexType.VECTOR, restored.getType());
    assertEquals(128, restored.getDimension());
    assertEquals("L2", restored.getDistanceType());
    assertEquals(16, restored.getHnswM());
    assertEquals(200, restored.getHnswEfConstruction());
    assertEquals(5, restored.getID());
  }

  @Test
  void testMaterializeAndInitRoundtripWithCustomParams() throws DocumentException {
    final Set<Path<QNm>> paths = new HashSet<>();
    final IndexDef original = IndexDefs.createVectorIdxDef(512, "COSINE", paths, 48, 600, 7, IndexDef.DbType.XML);

    final Node<?> materialized = original.materialize();
    assertNotNull(materialized);

    final IndexDef restored = new IndexDef(IndexDef.DbType.XML);
    restored.init(materialized);

    assertEquals(IndexType.VECTOR, restored.getType());
    assertEquals(512, restored.getDimension());
    assertEquals("COSINE", restored.getDistanceType());
    assertEquals(48, restored.getHnswM());
    assertEquals(600, restored.getHnswEfConstruction());
    assertEquals(7, restored.getID());
  }

  @Test
  void testCasIndexIsNotVector() {
    final IndexDef casIndex = IndexDefs.createCASIdxDef(false, null, Set.of(), 0, IndexDef.DbType.JSON);
    assertFalse(casIndex.isVectorIndex());
    assertTrue(casIndex.isCasIndex());
  }

  @Test
  void testPathIndexIsNotVector() {
    final IndexDef pathIndex = IndexDefs.createPathIdxDef(Set.of(), 0, IndexDef.DbType.JSON);
    assertFalse(pathIndex.isVectorIndex());
    assertTrue(pathIndex.isPathIndex());
  }

  @Test
  void testVectorIndexGetContentTypeReturnsNull() {
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(128, "L2", Set.of(), 0, IndexDef.DbType.JSON);
    assertNull(indexDef.getContentType());
  }

  @Test
  void testVectorIndexToStringContainsDimension() {
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(128, "L2", Set.of(), 0, IndexDef.DbType.JSON);
    final String str = indexDef.toString();
    assertNotNull(str);
    assertTrue(str.contains("128"), "toString should contain dimension value");
    assertTrue(str.contains("L2"), "toString should contain distance type");
  }
}
