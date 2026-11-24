package io.sirix.index.name;

import com.sun.management.ThreadMXBean;
import io.brackit.query.atomic.QNm;
import io.sirix.access.trx.page.AbstractForwardingStorageEngineReader;
import io.sirix.api.StorageEngineReader;
import io.sirix.index.IndexType;
import io.sirix.index.hot.NameKeySerializer;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.xml.ElementNode;
import io.sirix.node.xml.XmlDocumentRootNode;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.management.ManagementFactory;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Isolated
final class NameIndexRecheckAllocationBudgetTest {
  private static final String LOCAL = "a".repeat(NameKeySerializer.MAX_NAME_KEY_BYTES) + "x";
  private static final QNm REQUESTED = new QNm("urn:a", "alias", LOCAL);
  private static final int CANDIDATES = 4096;
  private static final int BYTES_PER_READ = 1024;
  private static volatile QNm nameSink;
  private static volatile NodeReferences postingSink;

  @Test
  void dictionaryReconstructionMayReleaseTheCandidateFrameBeforeMatching() {
    for (int mode = 0; mode < 3; mode++) {
      final Arena arena = Arena.ofConfined();
      try {
        final DictionaryReader reader = new DictionaryReader(2);
        final ElementNode source = reader.put(1, "urn:a", LOCAL);
        final MemorySegment page = arena.allocate(source.estimateSerializedSize());
        source.serializeToHeap(page, 0L);
        final ElementNode bound = new ElementNode(1, LongHashFunction.xx3());
        bound.bind(page, 0L, 1, 0);
        reader.records[1] = bound;
        reader.beforeDictionaryRead = arena::close;
        final NodeReferences candidates = postings(1, 2);
        final NodeReferences actual = switch (mode) {
          case 0 -> NameIndexRecheck.exactPostings(reader, candidates, REQUESTED, true);
          case 1 -> NameIndexRecheck.filteredPostings(reader, candidates,
              NameIndexRecheck.filter(Set.of(REQUESTED), Set.of()));
          default -> NameIndexRecheck.filteredPostings(reader, candidates,
              NameIndexRecheck.filter(Set.of(), Set.of(REQUESTED)));
        };
        if (mode < 2) {
          assertEquals(candidates, actual);
        } else {
          assertNull(actual);
        }
        assertEquals(1, reader.reads);
        assertFalse(arena.scope().isAlive());
        assertThrows(IllegalStateException.class, bound::getURIKey,
            "the dictionary read must really invalidate the candidate's native frame");
      } finally {
        if (arena.scope().isAlive()) {
          arena.close();
        }
      }
    }
  }

  @Test
  void rechecksPreserveExpandedNameEqualityAndRejectUnresolvableCandidates() {
    final DictionaryReader reader = new DictionaryReader(10);
    reader.put(1, "urn:a", LOCAL);
    reader.put(2, "urn:b", LOCAL);
    reader.put(3, "urn:a", LOCAL + "y");
    reader.put(4, "", LOCAL);
    reader.put(5, null, LOCAL);
    reader.put(6, "urn:a", LOCAL).setLocalNameKey(-1);
    reader.put(7, "urn:a", null);
    reader.records[9] = new XmlDocumentRootNode(9, null);
    final NodeReferences candidates = postings(1, 10);

    assertEquals(postings(1, 2), NameIndexRecheck.exactPostings(reader, candidates, REQUESTED, true));
    assertEquals(9, reader.reads);
    assertEquals(postings(4, 6), NameIndexRecheck.exactPostings(reader, candidates, new QNm(LOCAL), true));
    assertEquals(18, reader.reads);

    final NameIndexRecheck.NamePredicate included = NameIndexRecheck.filter(
        Set.of(new QNm("urn:a", "different-prefix", LOCAL), new QNm(LOCAL)), Set.of());
    final NodeReferences selected = NameIndexRecheck.filteredPostings(reader, candidates, included);
    assertEquals(postings(1, 2).addNodeKey(4).addNodeKey(5), selected);
    assertEquals(27, reader.reads);

    final NameIndexRecheck.NamePredicate excluded = NameIndexRecheck.filter(Set.of(), Set.of(REQUESTED));
    assertEquals(postings(2, 6), NameIndexRecheck.filteredPostings(reader, candidates, excluded));
    assertEquals(36, reader.reads);
    assertNull(NameIndexRecheck.filteredPostings(reader, candidates,
        NameIndexRecheck.filter(Set.of(REQUESTED), Set.of(new QNm("urn:a", "other", LOCAL)))));
    assertEquals(45, reader.reads);

    final NodeReferences ordinary = postings(1, 2);
    assertSame(ordinary, NameIndexRecheck.exactPostings(reader, ordinary, new QNm("field"), false));
    assertEquals(45, reader.reads);
  }

  @Test
  void rejectingCandidatesAllocatesOnlyPerReadWorkspace() {
    final ThreadMXBean bean = ManagementFactory.getPlatformMXBean(ThreadMXBean.class);
    assumeTrue(bean != null && bean.isThreadAllocatedMemorySupported());
    if (!bean.isThreadAllocatedMemoryEnabled()) {
      bean.setThreadAllocatedMemoryEnabled(true);
    }
    final DictionaryReader reader = new DictionaryReader(1);
    reader.put(0, "urn:b", LOCAL);
    final NodeReferences candidates = postings(0, CANDIDATES);
    final NameIndexRecheck.NamePredicate included = NameIndexRecheck.filter(Set.of(REQUESTED), Set.of());
    final NameIndexRecheck.NamePredicate excluded = NameIndexRecheck.filter(Set.of(),
        Set.of(new QNm("urn:b", "ignored", LOCAL)));
    final long thread = Thread.currentThread().threadId();
    long allocated = 0;
    for (int pass = 0; pass < 256; pass++) {
      final long before = bean.getThreadAllocatedBytes(thread);
      postingSink = NameIndexRecheck.exactPostings(reader, candidates, REQUESTED, true);
      postingSink = NameIndexRecheck.filteredPostings(reader, candidates, included);
      postingSink = NameIndexRecheck.filteredPostings(reader, candidates, excluded);
      allocated = bean.getThreadAllocatedBytes(thread) - before;
    }
    assertNull(postingSink);
    assertEquals(256L * 3 * CANDIDATES, reader.reads);
    assertTrue(allocated <= 3L * BYTES_PER_READ,
        "FMT-305: rejecting 4096 candidates must not allocate a name per candidate: " + allocated);

    final long before = bean.getThreadAllocatedBytes(thread);
    for (int candidate = 0; candidate < CANDIDATES; candidate++) {
      nameSink = new QNm("urn:b", "", LOCAL);
    }
    assertTrue(bean.getThreadAllocatedBytes(thread) - before > 3L * BYTES_PER_READ,
        "the allocation meter must detect the per-candidate name-object control");
    assertEquals(LOCAL, nameSink.getLocalName());
  }

  private static NodeReferences postings(final int from, final int to) {
    final NodeReferences references = new NodeReferences();
    for (int key = from; key < to; key++) {
      references.addNodeKey(key);
    }
    return references;
  }

  private static final class DictionaryReader extends AbstractForwardingStorageEngineReader {
    private final DataRecord[] records;
    private final String[] dictionary;
    private long reads;
    private Runnable beforeDictionaryRead;

    private DictionaryReader(final int size) {
      records = new DataRecord[size];
      dictionary = new String[size * 2];
    }

    private ElementNode put(final int key, final String uri, final String localName) {
      final ElementNode record = new ElementNode(key, LongHashFunction.xx3());
      record.setLocalNameKey(key * 2);
      record.setURIKey("".equals(uri) ? -1 : key * 2 + 1);
      record.setName(new QNm("stale-cached-name"));
      records[key] = record;
      dictionary[key * 2] = localName;
      dictionary[key * 2 + 1] = uri;
      return record;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V extends DataRecord> V getRecord(final long key, final IndexType type, final int index) {
      if (type != IndexType.DOCUMENT || index != -1) {
        throw new AssertionError("candidate reads must address document records");
      }
      reads++;
      return (V) records[(int) (key % records.length)];
    }

    @Override
    public String getName(final int key, final NodeKind kind) {
      final Runnable beforeRead = beforeDictionaryRead;
      beforeDictionaryRead = null;
      if (beforeRead != null) {
        beforeRead.run();
      }
      return dictionary[key];
    }

    @Override
    protected StorageEngineReader delegate() {
      throw new AssertionError("unexpected storage operation");
    }
  }
}
