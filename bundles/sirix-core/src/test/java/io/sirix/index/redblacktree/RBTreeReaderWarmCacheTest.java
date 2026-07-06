package io.sirix.index.redblacktree;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.access.trx.node.xml.XmlIndexController;
import io.sirix.api.Movement;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.cache.Cache;
import io.sirix.cache.RBIndexKey;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.node.interfaces.Node;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Regression test for the {@link RBTreeReader} constructor warm-up loop (issue #1063).
 * <p>
 * The warm-up loop must cache each {@link RBNodeKey} under its OWN {@link RBIndexKey}. Before
 * the fix it cached the read-cursor's current node, which the iterator's stack operation had
 * already parked on the node's LEFT CHILD — so index searches that hit the cache compared
 * against the wrong key and descended the wrong subtree.
 */
public final class RBTreeReaderWarmCacheTest {

  private Holder holder;

  @Before
  public void setUp() {
    XmlTestHelper.deleteEverything();
    holder = Holder.openResourceSessionWithRedBlackTreeIndexes();
  }

  @After
  public void tearDown() {
    holder.close();
    XmlTestHelper.closeEverything();
  }

  @Test
  public void testWarmUpCachesEachNodeUnderItsOwnKey() {
    final XmlNodeTrx wtx = holder.getResourceSession().beginNodeTrx();

    final XmlIndexController indexController =
        holder.getResourceSession().getWtxIndexController(wtx.getRevisionNumber());

    final IndexDef idxDef = IndexDefs.createCASIdxDef(false, Type.STR,
        Collections.singleton(Path.parse("//bla/@foobar")), 0, IndexDef.DbType.XML);

    indexController.createIndexes(Set.of(idxDef), wtx);

    // Five distinct attribute values on the matching path => at least five CAS index entries,
    // so the red-black tree has inner nodes with left children.
    wtx.insertElementAsFirstChild(new QNm("bla"));
    wtx.insertAttribute(new QNm("foobar"), "value-1", Movement.TOPARENT);
    wtx.insertElementAsFirstChild(new QNm("bla"));
    wtx.insertAttribute(new QNm("foobar"), "value-2", Movement.TOPARENT);
    wtx.insertElementAsFirstChild(new QNm("bla"));
    wtx.insertAttribute(new QNm("foobar"), "value-3", Movement.TOPARENT);
    wtx.insertElementAsFirstChild(new QNm("bla"));
    wtx.insertAttribute(new QNm("foobar"), "value-4", Movement.TOPARENT);
    wtx.insertElementAsFirstChild(new QNm("bla"));
    wtx.insertAttribute(new QNm("foobar"), "value-5", Movement.TOPARENT);
    wtx.commit();

    final IndexDef indexDef = indexController.getIndexes().getIndexDef(0, IndexType.CAS);
    wtx.close();

    try (final XmlNodeReadOnlyTrx rtx = holder.getResourceSession().beginNodeReadOnlyTrx()) {
      final StorageEngineReader storageEngineReader = rtx.getStorageEngineReader();
      final Cache<RBIndexKey, Node> cache = holder.getResourceSession().getIndexCache();

      // Constructing the reader with a read-only storage engine reader runs the warm-up loop
      // that populates the shared index cache.
      final RBTreeReader<CASValue, NodeReferences> reader =
          RBTreeReader.getInstance(cache, storageEngineReader, indexDef.getType(), indexDef.getID());

      final long databaseId = storageEngineReader.getDatabaseId();
      final long resourceId = storageEngineReader.getResourceId();
      final int revisionNumber = storageEngineReader.getRevisionNumber();

      int entryCount = 0;
      int checkedCacheHits = 0;
      boolean sawNodeWithLeftChild = false;

      for (final RBTreeReader<CASValue, NodeReferences>.RBNodeIterator it = reader.new RBNodeIterator(0);
          it.hasNext();) {
        final RBNodeKey<CASValue> node = it.next();
        entryCount++;
        if (node.hasLeftChild()) {
          sawNodeWithLeftChild = true;
        }

        final RBIndexKey cacheKey = new RBIndexKey(databaseId, resourceId, node.getNodeKey(), revisionNumber,
            indexDef.getType(), indexDef.getID());
        final Node cached = cache.get(cacheKey);
        if (cached != null) {
          // Pre-fix the cached node was the LEFT CHILD of the node the key was built from, so
          // the node keys mismatched for every inner node with a left child.
          assertEquals("cached node must be stored under its own node key", node.getNodeKey(), cached.getNodeKey());
          checkedCacheHits++;
        }
      }

      assertTrue("expected at least three index entries, but got " + entryCount, entryCount >= 3);
      assertTrue("the tree must contain at least one node with a left child (guard against a vacuous pass)",
          sawNodeWithLeftChild);
      assertTrue("expected at least one non-null cache hit (guard against a vacuous pass)", checkedCacheHits >= 1);
    }
  }
}
