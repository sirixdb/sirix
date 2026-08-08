package io.sirix.index.name;

import io.brackit.query.atomic.QNm;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.DescendantAxis;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end coverage for object member names whose {@code String.hashCode()} collides.
 *
 * <p>{@code "Aa"} and {@code "BB"} both hash to 2112, and three separate places treated that hash as
 * an identity:
 * <ul>
 * <li>the path summary's child lookup was keyed on 24 bits of it, so both names resolved to ONE
 * path node — the summary held a single node (name {@code Aa}, references 2), {@code match("BB")}
 * was empty, and every {@code BB} record was filed under {@code Aa}'s path class;</li>
 * <li>{@code Names.setName} only examined the primary slot, so each later occurrence of the
 * collision-losing name got a brand new dictionary entry — the three {@code BB} records below were
 * assigned keys 2113, 2114 and 2115;</li>
 * <li>path nodes derived their name key from the bare hash instead of the dictionary, so a path
 * node reloaded from disk reported the OTHER name.</li>
 * </ul>
 *
 * <p>Each test states the value it would have produced before the fix.
 */
final class HashCollidingNamesTest {

  private static final String RESOURCE = "collide";

  /** Both names of each pair share a {@code String.hashCode()}. */
  private static final String DOC = """
      [{"Aa":1,"BB":2},{"Aa":3,"BB":4},{"Aa":5,"BB":6}]""";

  private Path databasePath;

  @BeforeEach
  void setUp() throws IOException {
    databasePath = Files.createTempDirectory("sirix-colliding-names");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
  }

  @AfterEach
  void tearDown() {
    if (databasePath != null) {
      Databases.removeDatabase(databasePath);
    }
  }

  private void shred(final String json) {
    try (final var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE).buildPathSummary(true).build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
           final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json));
        wtx.commit();
      }
    }
  }

  /** Every object-key record in document order, as {@code (name, nameKey, pathNodeKey)}. */
  private record Member(String name, int nameKey, long pathNodeKey) {}

  private List<Member> members() {
    final List<Member> members = new ArrayList<>();
    try (final var database = Databases.openJsonDatabase(databasePath);
         final JsonResourceSession session = database.beginResourceSession(RESOURCE);
         final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      final var axis = new DescendantAxis(rtx);
      while (axis.hasNext()) {
        axis.nextLong();
        if (rtx.isObjectKey()) {
          members.add(new Member(new String(rtx.getNameBytes(), StandardCharsets.UTF_8), rtx.getNameKey(),
                                 rtx.getPathNodeKey()));
        }
      }
    }
    return members;
  }

  @Test
  @DisplayName("the two names really do collide")
  void precondition() {
    assertEquals("Aa".hashCode(), "BB".hashCode(),
                 "the whole test rests on these two names sharing a String.hashCode()");
  }

  @Test
  @DisplayName("every occurrence of a name gets the same dictionary key")
  void oneDictionaryKeyPerName() {
    shred(DOC);

    final Map<String, Integer> keyOf = new HashMap<>();
    for (final Member member : members()) {
      final Integer seen = keyOf.putIfAbsent(member.name(), member.nameKey());
      if (seen != null) {
        // Pre-fix the three BB records were assigned 2113, 2114 and 2115: setName checked only the
        // primary slot, so it never recognised a name that lived further down the probe chain and
        // minted a fresh entry (two persisted records) for each occurrence.
        assertEquals(seen.intValue(), member.nameKey(),
                     "every '" + member.name() + "' record must share one name key");
      }
    }

    assertEquals(2, keyOf.size(), "exactly two distinct member names");
    assertNotEquals(keyOf.get("Aa"), keyOf.get("BB"),
                    "colliding names must not end up on the same dictionary key");
  }

  @Test
  @DisplayName("colliding names get their own path node, with the right reference count")
  void onePathNodePerName() {
    shred(DOC);

    try (final var database = Databases.openJsonDatabase(databasePath);
         final JsonResourceSession session = database.beginResourceSession(RESOURCE);
         final PathSummaryReader summary = session.openPathSummary()) {
      final BitSet aa = summary.match(new QNm("Aa"), 0);
      final BitSet bb = summary.match(new QNm("BB"), 0);

      // Pre-fix: match("Aa") had one bit and match("BB") was EMPTY — BB shared Aa's path node.
      assertEquals(1, aa.cardinality(), "Aa must have exactly one path node");
      assertEquals(1, bb.cardinality(), "BB must have exactly one path node");
      assertNotEquals(aa.nextSetBit(0), bb.nextSetBit(0), "and it must not be the same node");

      summary.moveTo(aa.nextSetBit(0));
      assertEquals("Aa", summary.getName().getLocalName(), "the Aa path node must report Aa");
      assertEquals(3, summary.getReferences(), "one reference per Aa record");

      summary.moveTo(bb.nextSetBit(0));
      assertEquals("BB", summary.getName().getLocalName(),
                   "the BB path node must report BB, not the name it collided with");
      assertEquals(3, summary.getReferences(), "one reference per BB record");
    }
  }

  @Test
  @DisplayName("records point at the path node of their own name")
  void recordsPointAtTheirOwnPathClass() {
    shred(DOC);

    final Map<String, Long> pathNodeOf = new HashMap<>();
    for (final Member member : members()) {
      final Long seen = pathNodeOf.putIfAbsent(member.name(), member.pathNodeKey());
      if (seen != null) {
        assertEquals(seen.longValue(), member.pathNodeKey(),
                     "every '" + member.name() + "' record must share one path class");
      }
    }

    // Pre-fix both names shared one path node key, so a path-based index filed BB's values under
    // Aa's path class and a lookup of either returned the union.
    assertEquals(2, pathNodeOf.size(), "exactly two path classes");
    assertNotEquals(pathNodeOf.get("Aa"), pathNodeOf.get("BB"),
                    "colliding names must not share a path class");
  }

  @Test
  @DisplayName("values are readable back under the right member name")
  void valuesRoundTrip() {
    shred(DOC);

    final List<String> pairs = new ArrayList<>();
    try (final var database = Databases.openJsonDatabase(databasePath);
         final JsonResourceSession session = database.beginResourceSession(RESOURCE);
         final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      final var axis = new DescendantAxis(rtx);
      while (axis.hasNext()) {
        axis.nextLong();
        if (rtx.isObjectKey()) {
          final String name = new String(rtx.getNameBytes(), StandardCharsets.UTF_8);
          pairs.add(name + "=" + rtx.getValue());
        }
      }
    }

    assertEquals(List.of("Aa=1", "BB=2", "Aa=3", "BB=4", "Aa=5", "BB=6"), pairs,
                 "each member must carry its own name and value");
  }

  @Test
  @DisplayName("a name colliding with one already stored under a different parent still resolves")
  void collisionAcrossNestingLevels() {
    // Aa at the top level, BB nested one level deeper: the two path nodes have different parents,
    // so only the NAME dictionary can conflate them.
    shred("{\"Aa\":{\"BB\":7},\"BB\":8}");

    try (final var database = Databases.openJsonDatabase(databasePath);
         final JsonResourceSession session = database.beginResourceSession(RESOURCE);
         final PathSummaryReader summary = session.openPathSummary()) {
      final BitSet bb = summary.match(new QNm("BB"), 0);
      assertEquals(2, bb.cardinality(), "BB occurs on two distinct paths");

      for (int node = bb.nextSetBit(0); node >= 0; node = bb.nextSetBit(node + 1)) {
        summary.moveTo(node);
        assertEquals("BB", summary.getName().getLocalName(), "both BB path nodes must report BB");
      }

      final BitSet aa = summary.match(new QNm("Aa"), 0);
      assertEquals(1, aa.cardinality(), "Aa occurs on one path");
      assertTrue(aa.stream().noneMatch(bb::get), "the Aa and BB path nodes must be disjoint");
    }
  }
}
