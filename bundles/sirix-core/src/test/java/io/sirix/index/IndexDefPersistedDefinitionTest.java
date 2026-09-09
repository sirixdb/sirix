package io.sirix.index;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.jdm.node.Node;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link IndexDef#hasSameDefinition(IndexDef)} is the guard that refuses to re-bind a catalogue
 * slot to a definition with a different meaning. The catalogue persists every path as
 * {@link Path#toString()} and parses it back on load, so the guard MUST accept a definition's own
 * persisted copy — for every spelling the parser accepts, not only the ones whose internal step
 * representation happens to survive {@code parse(toString())}. A relative JSON name such as
 * {@code foo} parses with a CHILD step, prints as {@code ./foo} and re-parses as
 * CHILD_OBJECT_FIELD: {@link Path#equals(Object)} calls those different, the persisted form is
 * identical.
 */
final class IndexDefPersistedDefinitionTest {

  private static Path<QNm> json(final String path) {
    return Path.parse(path, PathParser.Type.JSON);
  }

  private static Path<QNm> xml(final String path) {
    return Path.parse(path, PathParser.Type.XML);
  }

  /** Persist through the catalogue exactly as a resource does and read the definition back. */
  private static IndexDef roundTrip(final IndexDef definition) {
    final Indexes indexes = new Indexes();
    indexes.add(definition);
    final Node<?> persisted = indexes.materialize();
    final Indexes reloaded = new Indexes();
    reloaded.init(persisted);
    final IndexDef reread = reloaded.getIndexDef(definition.getID(), definition.getType());
    assertNotNull(reread, "the persisted catalogue lost definition " + definition.getID());
    return reread;
  }

  @Test
  @DisplayName("a relative JSON path index equals its own persisted copy")
  void relativeJsonPathSurvivesPersistence() {
    final IndexDef definition = IndexDefs.createPathIdxDef(Set.of(json("foo")), 0, IndexDef.DbType.JSON);
    final IndexDef reread = roundTrip(definition);
    // Non-vacuity: this is exactly the spelling whose Path.equals does NOT survive the round trip.
    assertFalse(definition.getPaths().equals(reread.getPaths()),
        "Path.equals now survives parse(toString()) for 'foo' — this test no longer exercises the persisted-form"
            + " comparison and needs a spelling that does");
    assertTrue(definition.hasSameDefinition(reread), "a definition must accept its own persisted copy");
    assertTrue(reread.hasSameDefinition(definition), "persisted-definition equality must be symmetric");
  }

  @Test
  @DisplayName("absolute JSON and XML paths survive persistence for PATH and CAS definitions")
  void absolutePathsSurvivePersistence() {
    final IndexDef jsonPath = IndexDefs.createPathIdxDef(Set.of(json("/a/[]/b"), json("//c")), 1, IndexDef.DbType.JSON);
    assertTrue(jsonPath.hasSameDefinition(roundTrip(jsonPath)));
    final IndexDef xmlCas =
        IndexDefs.createCASIdxDef(false, Type.STR, Set.of(xml("/a/b"), xml("//c/@d")), 2, IndexDef.DbType.XML);
    assertTrue(xmlCas.hasSameDefinition(roundTrip(xmlCas)));
    final IndexDef xmlPath = IndexDefs.createPathIdxDef(Set.of(xml("/a/b")), 3, IndexDef.DbType.XML);
    assertTrue(xmlPath.hasSameDefinition(roundTrip(xmlPath)));
  }

  @Test
  @DisplayName("projection field paths are compared in persisted form too")
  void projectionFieldsSurvivePersistence() {
    final IndexDef projection = IndexDefs.createProjectionIdxDef(json("/[]"), List.of(json("/[]/id"), json("name")),
        List.of(Type.LON, Type.STR), 4, IndexDef.DbType.JSON);
    final IndexDef reread = roundTrip(projection);
    assertFalse(projection.getProjectionFields().equals(reread.getProjectionFields()),
        "the relative field spelling no longer differs after the round trip — pick one that does");
    assertTrue(projection.hasSameDefinition(reread));
  }

  @Test
  @DisplayName("genuinely different definitions are still rejected")
  void differentDefinitionsAreStillDifferent() {
    final IndexDef foo = IndexDefs.createPathIdxDef(Set.of(json("/foo")), 0, IndexDef.DbType.JSON);
    final IndexDef bar = IndexDefs.createPathIdxDef(Set.of(json("/bar")), 0, IndexDef.DbType.JSON);
    final IndexDef fooAndBar = IndexDefs.createPathIdxDef(Set.of(json("/foo"), json("/bar")), 0, IndexDef.DbType.JSON);
    assertFalse(foo.hasSameDefinition(bar));
    assertFalse(foo.hasSameDefinition(fooAndBar));
    assertFalse(fooAndBar.hasSameDefinition(foo));
    final IndexDef fields = IndexDefs.createProjectionIdxDef(json("/[]"), List.of(json("/[]/id")), List.of(Type.LON), 4,
        IndexDef.DbType.JSON);
    final IndexDef otherFields = IndexDefs.createProjectionIdxDef(json("/[]"), List.of(json("/[]/other")),
        List.of(Type.LON), 4, IndexDef.DbType.JSON);
    assertFalse(fields.hasSameDefinition(otherFields));
    // The persisted spelling is the identity: two spellings that print identically ARE the same
    // definition.
    final IndexDef relative = IndexDefs.createPathIdxDef(Set.of(json("foo")), 5, IndexDef.DbType.JSON);
    final IndexDef dotted = IndexDefs.createPathIdxDef(Set.of(json("./foo")), 5, IndexDef.DbType.JSON);
    assertTrue(relative.hasSameDefinition(dotted));
    assertTrue(new Str(relative.getPaths().iterator().next().toString()).stringValue().equals("./foo"));
  }
}
