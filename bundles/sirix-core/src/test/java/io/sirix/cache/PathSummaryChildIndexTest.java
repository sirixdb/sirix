package io.sirix.cache;

import io.brackit.query.atomic.QNm;
import io.sirix.node.NodeKind;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Unit coverage for the open-addressed {@code (parent, name, kind) -> child path node key} table.
 *
 * <p>It replaced a {@code Long2LongOpenHashMap} keyed on a LOSSY pack of the triple — 24 bits of
 * {@code String.hashCode} plus the low 32 bits of the parent key — whose value was returned as the
 * authoritative child with no verification. The cases below pin the two properties that key could
 * not offer: a hit is the child that was actually inserted, and a miss really means absent.
 */
final class PathSummaryChildIndexTest {

  private static final NodeKind KIND = NodeKind.OBJECT_NAMED_OBJECT;

  /** {@code "Aa"} and {@code "BB"} both have {@code String.hashCode() == 2112}. */
  private static final QNm AA = new QNm("Aa");
  private static final QNm BB = new QNm("BB");

  private static PathSummaryChildIndex index() {
    return new PathSummaryChildIndex(16);
  }

  @Test
  @DisplayName("names whose hashes collide keep separate entries")
  void collidingNamesAreDistinct() {
    assertEquals("Aa".hashCode(), "BB".hashCode(), "precondition: the two names must collide");

    final PathSummaryChildIndex index = index();
    index.put(1L, AA, KIND, 11L);
    index.put(1L, BB, KIND, 22L);

    assertEquals(2, index.size(), "the second name must not overwrite the first");
    assertEquals(11L, index.get(1L, AA, KIND));
    assertEquals(22L, index.get(1L, BB, KIND));
  }

  @Test
  @DisplayName("removing one of two colliding names leaves the other reachable")
  void removingOneCollidingNameKeepsTheOther() {
    // The lossy key gave both names ONE entry, so removing either dropped it and orphaned the
    // survivor — it then read as absent and the caller inserted a duplicate path node for it.
    final PathSummaryChildIndex index = index();
    index.put(1L, AA, KIND, 11L);
    index.put(1L, BB, KIND, 22L);

    index.remove(1L, AA, KIND);

    assertEquals(1, index.size());
    assertEquals(PathSummaryChildIndex.NO_VALUE, index.get(1L, AA, KIND), "the removed name is gone");
    assertEquals(22L, index.get(1L, BB, KIND), "the survivor must still resolve");
  }

  @Test
  @DisplayName("the same name under different parents and kinds stays separate")
  void parentAndKindArePartOfTheKey() {
    final PathSummaryChildIndex index = index();
    index.put(1L, AA, KIND, 11L);
    index.put(2L, AA, KIND, 22L);
    index.put(1L, AA, NodeKind.OBJECT_NAMED_ARRAY, 33L);

    assertEquals(3, index.size());
    assertEquals(11L, index.get(1L, AA, KIND));
    assertEquals(22L, index.get(2L, AA, KIND));
    assertEquals(33L, index.get(1L, AA, NodeKind.OBJECT_NAMED_ARRAY));
    assertEquals(PathSummaryChildIndex.NO_VALUE, index.get(3L, AA, KIND), "an unused parent misses");
  }

  @Test
  @DisplayName("re-putting a triple overwrites in place")
  void putOverwrites() {
    final PathSummaryChildIndex index = index();
    index.put(1L, AA, KIND, 11L);
    index.put(1L, AA, KIND, 99L);

    assertEquals(1, index.size(), "an overwrite must not add an entry");
    assertEquals(99L, index.get(1L, AA, KIND));
  }

  @Test
  @DisplayName("removing an absent triple is a no-op")
  void removeAbsentIsNoOp() {
    final PathSummaryChildIndex index = index();
    index.put(1L, AA, KIND, 11L);

    index.remove(1L, BB, KIND);
    index.remove(7L, AA, KIND);

    assertEquals(1, index.size());
    assertEquals(11L, index.get(1L, AA, KIND));
  }

  @Test
  @DisplayName("growth past the initial capacity preserves every entry")
  void growthPreservesEntries() {
    // Starts far below the entry count so the table rehashes several times.
    final PathSummaryChildIndex index = new PathSummaryChildIndex(0);
    final Map<String, Long> expected = new HashMap<>();
    for (int i = 0; i < 2_000; i++) {
      final String name = "field" + i;
      final long parent = i % 7;
      index.put(parent, new QNm(name), KIND, 1_000L + i);
      expected.put(parent + "/" + name, 1_000L + i);
    }

    assertEquals(2_000, index.size());
    for (int i = 0; i < 2_000; i++) {
      final String name = "field" + i;
      final long parent = i % 7;
      assertEquals(expected.get(parent + "/" + name), index.get(parent, new QNm(name), KIND),
                   "entry lost or moved across a rehash: " + parent + "/" + name);
    }
  }

  @Test
  @DisplayName("backward-shift deletion keeps later probe chains intact")
  void deletionKeepsChainsIntact() {
    // Insert many entries, delete every other one, and check the survivors still resolve. A
    // deletion that simply blanked its slot would truncate the probe chain of anything that had
    // collided past it, so the survivors would read as absent.
    final PathSummaryChildIndex index = new PathSummaryChildIndex(0);
    for (int i = 0; i < 500; i++) {
      index.put(1L, new QNm("f" + i), KIND, i);
    }
    for (int i = 0; i < 500; i += 2) {
      index.remove(1L, new QNm("f" + i), KIND);
    }

    assertEquals(250, index.size());
    for (int i = 0; i < 500; i++) {
      final long actual = index.get(1L, new QNm("f" + i), KIND);
      if (i % 2 == 0) {
        assertEquals(PathSummaryChildIndex.NO_VALUE, actual, "f" + i + " was removed");
      } else {
        assertEquals(i, actual, "f" + i + " must survive the deletions around it");
      }
    }
  }

  @Test
  @DisplayName("the copy constructor shares no state")
  void copyIsIndependent() {
    final PathSummaryChildIndex index = index();
    index.put(1L, AA, KIND, 11L);

    final PathSummaryChildIndex copy = new PathSummaryChildIndex(index);
    copy.put(1L, BB, KIND, 22L);
    copy.put(1L, AA, KIND, 99L);

    assertEquals(1, index.size(), "the original must not see the copy's inserts");
    assertEquals(11L, index.get(1L, AA, KIND), "the original must not see the copy's overwrite");
    assertNotEquals(index.size(), copy.size());
    assertEquals(22L, copy.get(1L, BB, KIND));
  }
}
