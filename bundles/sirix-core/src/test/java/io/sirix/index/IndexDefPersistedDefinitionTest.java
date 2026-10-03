package io.sirix.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.jdm.DocumentException;
import io.brackit.query.jdm.node.Node;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.trx.node.IndexController;
import io.sirix.access.trx.node.xml.XmlIndexController;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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

  @Test
  void validTimeCatalogRequiresTheMetadataFormat() {
    final IndexDef definition =
        IndexDefs.createValidTimeIdxDef(Set.of(json("/[]/vf"), json("/[]/vt")), 0, IndexDef.DbType.JSON);
    assertTrue(definition.hasSameDefinition(roundTrip(definition)));
    final Node<?> persisted = definition.materialize();
    final QNm format = new QNm("validTimeFormat");
    assertTrue(persisted.deleteAttribute(format));
    assertThrows(DocumentException.class, () -> new IndexDef(IndexDef.DbType.JSON).init(persisted));
    persisted.setAttribute(format, new Str("1"));
    assertThrows(DocumentException.class, () -> new IndexDef(IndexDef.DbType.JSON).init(persisted));
  }

  private static Path<QNm> json(final String path) {
    return Path.parse(path, PathParser.Type.JSON);
  }

  private static Path<QNm> xml(final String path) {
    return Path.parse(path, PathParser.Type.XML);
  }

  /** Persist through the catalogue exactly as a resource does and read the definition back. */
  private static IndexDef roundTrip(final IndexDef definition) {
    final XmlIndexController controller = new XmlIndexController();
    controller.getIndexes().add(definition);
    final ByteArrayOutputStream serialized = new ByteArrayOutputStream();
    controller.serialize(serialized);
    final Indexes reloaded = new Indexes();
    reloaded.init(IndexController.deserialize(new ByteArrayInputStream(serialized.toByteArray())).getFirstChild());
    final IndexDef reread = reloaded.getIndexDef(definition.getID(), definition.getType());
    assertNotNull(reread, "the persisted catalogue lost definition " + definition.getID());
    return reread;
  }

  @Test
  void nameFiltersPreserveXmlSensitiveComponentsInSerializedCatalogue() {
    final String sensitive = "\"&<>\t\n\r,\u0000chîld\uD83D\uDE80";
    final QNm xmlName = new QNm("urn:" + sensitive, sensitive, sensitive);
    final QNm jsonName = new QNm(sensitive);
    for (final IndexDef definition : List.of(
        IndexDefs.createSelectiveNameIdxDef(Set.of(xmlName), 0, IndexDef.DbType.XML),
        IndexDefs.createFilteredNameIdxDef(Set.of(xmlName), 1, IndexDef.DbType.XML),
        IndexDefs.createSelectiveNameIdxDef(Set.of(jsonName), 0, IndexDef.DbType.JSON),
        IndexDefs.createFilteredNameIdxDef(Set.of(jsonName), 1, IndexDef.DbType.JSON))) {
      final IndexDef reloaded = roundTrip(definition);
      assertTrue(definition.hasSameDefinition(reloaded));
      assertNameComponents(definition.getIncluded(), reloaded.getIncluded());
      assertNameComponents(definition.getExcluded(), reloaded.getExcluded());
    }
  }

  @Test
  void nameFiltersPreserveNamespacePrefixUnicodeAndCommas() {
    final QNm xmlName = new QNm("urn:names,with-comma", "p", "chîld");
    final QNm jsonName = new QNm("field,with-comma");
    for (final IndexDef definition : List.of(
        IndexDefs.createSelectiveNameIdxDef(Set.of(xmlName), 0, IndexDef.DbType.XML),
        IndexDefs.createFilteredNameIdxDef(Set.of(xmlName), 1, IndexDef.DbType.XML),
        IndexDefs.createSelectiveNameIdxDef(Set.of(jsonName), 0, IndexDef.DbType.JSON),
        IndexDefs.createFilteredNameIdxDef(Set.of(jsonName), 1, IndexDef.DbType.JSON))) {
      final IndexDef reloaded = roundTrip(definition);
      assertTrue(definition.hasSameDefinition(reloaded), "NAME definition must accept its persisted copy");
      assertNameComponents(definition.getIncluded(), reloaded.getIncluded());
      assertNameComponents(definition.getExcluded(), reloaded.getExcluded());
    }
  }

  private static void assertNameComponents(final Set<QNm> expected, final Set<QNm> actual) {
    assertEquals(expected, actual);
    for (final QNm name : expected) {
      final QNm reloaded = actual.stream().filter(name::equals).findFirst().orElseThrow();
      assertEquals(name.getNamespaceURI(), reloaded.getNamespaceURI());
      assertEquals(name.getPrefix(), reloaded.getPrefix());
      assertEquals(name.getLocalName(), reloaded.getLocalName());
    }
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
  void sortedProjectionOrderSurvivesCataloguePersistence() {
    final List<Path<QNm>> fields = List.of(json("/[]/kind"), json("/[]/did"), json("/[]/collection"));
    final List<Type> types = List.of(Type.STR, Type.STR, Type.STR);
    final ProjectionSortedSpec sorted = new ProjectionSortedSpec(List.of(0, 2, 1));
    final IndexDef definition =
        IndexDefs.createProjectionIdxDef(json("/[]"), fields, types, 4, IndexDef.DbType.JSON, sorted);
    final IndexDef reread = roundTrip(definition);
    assertTrue(definition.hasSameDefinition(reread));
    assertTrue(reread.hasSameDefinition(definition));
    assertTrue(sorted.equals(reread.getProjectionSortedSpec()));
    final IndexDef differentOrder = IndexDefs.createProjectionIdxDef(json("/[]"), fields, types, 4,
        IndexDef.DbType.JSON, new ProjectionSortedSpec(List.of(2, 0, 1)));
    assertFalse(definition.hasSameDefinition(differentOrder));
    assertFalse(definition.hasSameDefinition(
        IndexDefs.createProjectionIdxDef(json("/[]"), fields, types, 4, IndexDef.DbType.JSON)));
    assertThrows(IllegalArgumentException.class, () -> IndexDefs.createProjectionIdxDef(json("/[]"), fields, types, 4,
        IndexDef.DbType.JSON, new ProjectionSortedSpec(List.of(3))));
    assertThrows(IllegalArgumentException.class, () -> new ProjectionSortedSpec(List.of(1, 1)));
    assertThrows(IllegalArgumentException.class,
        () -> IndexDefs.createProjectionIdxDef(json("/[]"), List.of(json("/[]/kind"), json("/[]/score")),
            List.of(Type.STR, Type.DBL), 4, IndexDef.DbType.JSON, new ProjectionSortedSpec(List.of(1))),
        "a floating column cannot be a sort key");
    assertThrows(IllegalArgumentException.class,
        () -> IndexDefs.createProjectionIdxDef(json("/[]"), List.of(json("/[]/kind"), json("/[]/tags/[]")),
            List.of(Type.STR, Type.STR), 4, IndexDef.DbType.JSON, new ProjectionSortedSpec(List.of(1))),
        "an array-element (set) column cannot be a sort key");
    assertTrue(IndexDefs.createProjectionIdxDef(json("/[]"), List.of(json("/[]/flag"), json("/[]/day")),
        List.of(Type.BOOL, Type.DATE), 4, IndexDef.DbType.JSON, new ProjectionSortedSpec(List.of(1, 0)))
                        .hasSameDefinition(roundTrip(IndexDefs.createProjectionIdxDef(json("/[]"),
                            List.of(json("/[]/flag"), json("/[]/day")), List.of(Type.BOOL, Type.DATE), 4,
                            IndexDef.DbType.JSON, new ProjectionSortedSpec(List.of(1, 0))))));
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
