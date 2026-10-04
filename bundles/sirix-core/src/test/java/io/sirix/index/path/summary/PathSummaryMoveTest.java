package io.sirix.index.path.summary;

import io.brackit.query.atomic.QNm;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.objectvalue.ObjectValue;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeCursor;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.axis.DescendantAxis;
import io.sirix.node.NodeKind;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.service.xml.serialize.XmlSerializer;
import io.sirix.service.xml.shredder.XmlShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Move maintenance must agree with insertion from scratch, including reference counts. */
final class PathSummaryMoveTest {
  @TempDir
  Path directory;

  private enum Position {
    FIRST_CHILD, LEFT_SIBLING, RIGHT_SIBLING
  }

  private enum JsonSubtree {
    NAMED_OBJECT, NAMED_ARRAY, ARRAY, OBJECT
  }

  static Stream<Arguments> jsonMoves() {
    return moves().flatMap(arguments -> Stream.of(JsonSubtree.values()).map(subtree -> {
      final Object[] move = arguments.get();
      return Arguments.of(move[0], move[1], move[2], move[3], subtree);
    }));
  }

  static Stream<Arguments> moves() {
    final List<Arguments> cases = new ArrayList<>();
    for (final VersioningType versioning : VersioningType.values()) {
      for (final Position position : Position.values()) {
        for (final boolean acrossParents : new boolean[] {false, true}) {
          for (final boolean shared : new boolean[] {false, true}) {
            cases.add(Arguments.of(versioning, position, acrossParents, shared));
          }
        }
      }
    }
    return cases.stream();
  }

  static Stream<Arguments> xmlRenameMoves() {
    return renameMoves(NodeKind.ELEMENT, NodeKind.ATTRIBUTE, NodeKind.NAMESPACE, NodeKind.PROCESSING_INSTRUCTION);
  }

  static Stream<Arguments> jsonRenameMoves() {
    return renameMoves(NodeKind.OBJECT_NAMED_OBJECT, NodeKind.OBJECT_NAMED_ARRAY, NodeKind.OBJECT_NAMED_STRING,
        NodeKind.OBJECT_NAMED_NUMBER, NodeKind.OBJECT_NAMED_BOOLEAN, NodeKind.OBJECT_NAMED_NULL);
  }

  private static Stream<Arguments> renameMoves(final NodeKind... kinds) {
    final List<Arguments> cases = new ArrayList<>();
    for (final VersioningType versioning : VersioningType.values()) {
      for (final Position position : Position.values()) {
        for (final NodeKind kind : kinds) {
          cases.add(Arguments.of(versioning, position, kind));
        }
      }
    }
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("moves")
  void xmlMovesMatchFreshSummary(final VersioningType versioning, final Position position, final boolean acrossParents,
      final boolean shared) throws Exception {
    final Path path = directory.resolve("xml");
    Databases.createXmlDatabase(new DatabaseConfiguration(path));
    try (final var database = Databases.openXmlDatabase(path)) {
      database.createResource(configuration("data", versioning));
      try (final var session = database.beginResourceSession("data"); final XmlNodeTrx trx = session.beginNodeTrx()) {
        final String secondParent = shared
            ? "a"
            : "b";
        final String namespaces = " xmlns:p=\"urn:p\" xmlns:q=\"urn:p\"";
        final String extra = shared
            ? "<child" + namespaces + "><leaf/></child>"
            : "";
        final String xml = "<root><a><target/><child" + namespaces
            + " id=\"1\"><branch><leaf/><leaf/></branch><leaf/><leaf/></child>" + extra + "<last/></a><" + secondParent
            + "><child" + namespaces + "><other/></child><target/><last/></" + secondParent + "></root>";
        trx.insertSubtreeAsFirstChild(XmlShredder.createStringReader(xml), XmlNodeTrx.Commit.No);
        trx.commit();
        final long sourceParent = namedKeys(trx, "a").getFirst();
        final long destinationParent = acrossParents
            ? namedKeys(trx, secondParent).getLast()
            : sourceParent;
        final long source = child(trx, sourceParent, "child");
        final long anchor = position == Position.FIRST_CHILD
            ? destinationParent
            : child(trx, destinationParent, position == Position.LEFT_SIBLING
                ? "target"
                : "last");
        assertTrue(trx.moveTo(anchor));
        switch (position) {
          case FIRST_CHILD -> trx.moveSubtreeToFirstChild(source);
          case LEFT_SIBLING -> trx.moveSubtreeToLeftSibling(source);
          case RIGHT_SIBLING -> trx.moveSubtreeToRightSibling(source);
        }
        assertTrue(trx.moveTo(source));
        assertEquals(destinationParent, trx.getParentKey());
        final Map<String, Entry> moved = assertXmlMatchesFresh(database, session, trx, versioning);
        // Move back into the original ancestry, reusing classes that may have just been deleted.
        assertTrue(trx.moveTo(sourceParent));
        if (trx.getLastChildKey() == source) {
          trx.moveSubtreeToFirstChild(source);
        } else {
          assertTrue(trx.moveToLastChild());
          trx.moveSubtreeToRightSibling(source);
        }
        assertXmlMatchesFresh(database, session, trx, versioning);
        // A later insert must use the new parent/path and preserve the sibling chain.
        assertTrue(trx.moveTo(destinationParent));
        assertTrue(trx.moveToLastChild());
        final long last = trx.getNodeKey();
        trx.insertElementAsRightSibling(new QNm("appended"));
        assertEquals(last, trx.getLeftSiblingKey());
        assertEquals(destinationParent, trx.getParentKey());
        assertXmlMatchesFresh(database, session, trx, versioning);
        try (final var historical = session.openPathSummary(2)) {
          assertEquals(moved, snapshot(historical), "later insert must preserve the move revision");
        }
      }
    } finally {
      Databases.removeDatabase(path);
    }
  }

  @ParameterizedTest
  @MethodSource("jsonMoves")
  void jsonMovesMatchFreshSummary(final VersioningType versioning, final Position position, final boolean acrossParents,
      final boolean shared, final JsonSubtree subtree) throws Exception {
    final Path path = directory.resolve("json");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (final var database = Databases.openJsonDatabase(path)) {
      database.createResource(configuration("data", versioning));
      try (final var session = database.beginResourceSession("data"); final JsonNodeTrx trx = session.beginNodeTrx()) {
        final String secondParent = shared
            ? "a"
            : "b";
        final boolean anonymous = subtree == JsonSubtree.ARRAY || subtree == JsonSubtree.OBJECT;
        final String branch =
            "{\"branch\":{\"leaf\":1},\"leaf\":2,\"items\":[{\"nested\":[{\"value\":4},{\"value\":5}]},{\"nested\":[{\"value\":6}]}]}";
        final String sourceValue = subtree == JsonSubtree.NAMED_ARRAY || subtree == JsonSubtree.ARRAY
            ? "[" + branch + "]"
            : branch;
        final String targetValue = subtree == JsonSubtree.NAMED_ARRAY || subtree == JsonSubtree.ARRAY
            ? "[{\"other\":3}]"
            : "{\"other\":3}";
        final String sourceContainer = anonymous
            ? "[0," + sourceValue + ",9]"
            : "{\"target\":{},\"child\":" + sourceValue + ",\"last\":{}}";
        final String targetContainer = anonymous
            ? "[0," + targetValue + ",9]"
            : "{\"target\":{},\"child\":" + targetValue + ",\"last\":{}}";
        final String json = "[{\"a\":" + sourceContainer + "},{\"" + secondParent + "\":" + targetContainer + "}]";
        trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
        trx.commit();
        final long sourceParent = namedKeys(trx, "a").getFirst();
        final long destinationParent = acrossParents
            ? namedKeys(trx, secondParent).getLast()
            : sourceParent;
        final long source;
        final long anchor;
        if (anonymous) {
          assertTrue(trx.moveTo(sourceParent));
          assertTrue(trx.moveToFirstChild());
          assertTrue(trx.moveToRightSibling());
          source = trx.getNodeKey();
          assertTrue(trx.moveTo(destinationParent));
          if (position == Position.LEFT_SIBLING) {
            assertTrue(trx.moveToFirstChild());
          } else if (position == Position.RIGHT_SIBLING) {
            assertTrue(trx.moveToLastChild());
          }
          anchor = trx.getNodeKey();
        } else {
          source = child(trx, sourceParent, "child");
          anchor = position == Position.FIRST_CHILD
              ? destinationParent
              : child(trx, destinationParent, position == Position.LEFT_SIBLING
                  ? "target"
                  : "last");
        }
        assertTrue(trx.moveTo(anchor));
        switch (position) {
          case FIRST_CHILD -> trx.moveSubtreeToFirstChild(source);
          case LEFT_SIBLING -> trx.moveSubtreeToLeftSibling(source);
          case RIGHT_SIBLING -> trx.moveSubtreeToRightSibling(source);
        }
        assertTrue(trx.moveTo(source));
        assertEquals(destinationParent, trx.getParentKey());
        final Map<String, Entry> moved = assertJsonMatchesFresh(database, session, trx, versioning);
        // Move back into the original ancestry, reusing classes that may have just been deleted.
        assertTrue(trx.moveTo(sourceParent));
        if (trx.getLastChildKey() == source) {
          trx.moveSubtreeToFirstChild(source);
        } else {
          assertTrue(trx.moveToLastChild());
          trx.moveSubtreeToRightSibling(source);
        }
        assertJsonMatchesFresh(database, session, trx, versioning);
        assertTrue(trx.moveTo(destinationParent));
        final long last = trx.getLastChildKey();
        if (anonymous) {
          trx.insertNullValueAsLastChild();
        } else {
          trx.insertObjectRecordAsLastChild("appended", new ObjectValue());
        }
        assertEquals(last, trx.getLeftSiblingKey());
        assertEquals(destinationParent, trx.getParentKey());
        assertJsonMatchesFresh(database, session, trx, versioning);
        try (final var historical = session.openPathSummary(2)) {
          assertEquals(moved, snapshot(historical), "later insert must preserve the move revision");
        }
      }
    } finally {
      Databases.removeDatabase(path);
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void namespaceBearingAdjacentLeftSiblingMove(final VersioningType versioning) throws Exception {
    final Path path = directory.resolve("adjacent-xml");
    Databases.createXmlDatabase(new DatabaseConfiguration(path));
    try (final var database = Databases.openXmlDatabase(path)) {
      database.createResource(configuration("data", versioning));
      try (final var session = database.beginResourceSession("data"); final XmlNodeTrx trx = session.beginNodeTrx()) {
        trx.insertSubtreeAsFirstChild(
            XmlShredder.createStringReader("<root xmlns:p=\"urn:p\"><child xmlns:p=\"urn:p\"/><target/></root>"),
            XmlNodeTrx.Commit.No);
        trx.commit();
        final long childKey = namedKeys(trx, "child").getFirst();
        final long targetKey = namedKeys(trx, "target").getFirst();
        assertTrue(trx.moveTo(targetKey));
        trx.moveSubtreeToLeftSibling(childKey);
        assertEquals(targetKey, trx.getNodeKey());
        assertXmlMatchesFresh(database, session, trx, versioning);
        assertTrue(trx.moveTo(targetKey));
        trx.moveSubtreeToFirstChild(childKey);
        assertXmlMatchesFresh(database, session, trx, versioning);
        final long root = namedKeys(trx, "root").getFirst();
        assertTrue(trx.moveTo(root));
        trx.moveSubtreeToFirstChild(childKey);
        assertXmlMatchesFresh(database, session, trx, versioning);
      }
    } finally {
      Databases.removeDatabase(path);
    }
  }

  @ParameterizedTest
  @MethodSource("xmlRenameMoves")
  void xmlRenameThenMoveMatchesFreshSummary(final VersioningType versioning, final Position position,
      final NodeKind kind) throws Exception {
    final Path path = directory.resolve("xml-rename-move");
    Databases.createXmlDatabase(new DatabaseConfiguration(path));
    try (final var database = Databases.openXmlDatabase(path)) {
      database.createResource(configuration("data", versioning));
      try (final var session = database.beginResourceSession("data"); final XmlNodeTrx trx = session.beginNodeTrx()) {
        final String subtree = switch (kind) {
          case ELEMENT -> "<x><leaf/></x>";
          case ATTRIBUTE -> "<child x=\"value\"><leaf/></child>";
          case NAMESPACE -> "<child xmlns:p=\"urn:x\"><leaf/></child>";
          case PROCESSING_INSTRUCTION -> "<child><?x value?><leaf/></child>";
          default -> throw new AssertionError(kind);
        };
        trx.insertSubtreeAsFirstChild(XmlShredder.createStringReader(
            "<root><a>" + subtree + "</a><b>" + subtree + "<target/><last/></b></root>"), XmlNodeTrx.Commit.No);
        trx.commit();
        snapshot(trx.getPathSummary());
        final long sourceParent = namedKeys(trx, "a").getFirst();
        final long destinationParent = namedKeys(trx, "b").getFirst();
        final String rootName = kind == NodeKind.ELEMENT
            ? "x"
            : "child";
        final long source = child(trx, sourceParent, rootName);
        child(trx, destinationParent, rootName);
        switch (kind) {
          case ATTRIBUTE -> assertTrue(trx.moveToAttribute(0));
          case NAMESPACE -> assertTrue(trx.moveToNamespace(0));
          case PROCESSING_INSTRUCTION -> assertTrue(trx.moveToFirstChild());
          default -> {
          }
        }
        assertEquals(kind, trx.getKind());
        final QNm oldName = trx.getName();
        final QNm newName = kind == NodeKind.NAMESPACE
            ? new QNm("urn:y", "q", "")
            : new QNm("y");
        final long renamedPath = trx.getPathNodeKey();
        trx.setName(newName);
        assertRenamedPath(trx.getPathSummary(), renamedPath, oldName, newName, kind);
        final long anchor = position == Position.FIRST_CHILD
            ? destinationParent
            : child(trx, destinationParent, position == Position.LEFT_SIBLING
                ? "target"
                : "last");
        assertTrue(trx.moveTo(anchor));
        switch (position) {
          case FIRST_CHILD -> trx.moveSubtreeToFirstChild(source);
          case LEFT_SIBLING -> trx.moveSubtreeToLeftSibling(source);
          case RIGHT_SIBLING -> trx.moveSubtreeToRightSibling(source);
        }
        assertTrue(trx.moveTo(source));
        assertEquals(destinationParent, trx.getParentKey());
        assertXmlMatchesFresh(database, session, trx, versioning);
      }
    } finally {
      Databases.removeDatabase(path);
    }
  }

  @ParameterizedTest
  @MethodSource("jsonRenameMoves")
  void jsonRenameThenMoveMatchesFreshSummary(final VersioningType versioning, final Position position,
      final NodeKind kind) throws Exception {
    final Path path = directory.resolve("json-rename-move");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (final var database = Databases.openJsonDatabase(path)) {
      database.createResource(configuration("data", versioning));
      try (final var session = database.beginResourceSession("data"); final JsonNodeTrx trx = session.beginNodeTrx()) {
        final String value = switch (kind) {
          case OBJECT_NAMED_OBJECT -> "{\"leaf\":1}";
          case OBJECT_NAMED_ARRAY -> "[{\"leaf\":1}]";
          case OBJECT_NAMED_STRING -> "\"value\"";
          case OBJECT_NAMED_NUMBER -> "1";
          case OBJECT_NAMED_BOOLEAN -> "true";
          case OBJECT_NAMED_NULL -> "null";
          default -> throw new AssertionError(kind);
        };
        trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
            "[{\"a\":{\"x\":" + value + "}},{\"b\":{\"x\":" + value + ",\"target\":{},\"last\":{}}}]"),
            JsonNodeTrx.Commit.NO);
        trx.commit();
        snapshot(trx.getPathSummary());
        final long sourceParent = namedKeys(trx, "a").getFirst();
        final long destinationParent = namedKeys(trx, "b").getFirst();
        final long source = child(trx, sourceParent, "x");
        child(trx, destinationParent, "x");
        assertEquals(kind, trx.getKind());
        final PathSummaryReader summary = trx.getPathSummary();
        assertTrue(summary.moveTo(trx.getPathNodeKey()));
        final long renamedPath = kind == NodeKind.OBJECT_NAMED_ARRAY
            ? summary.getParentKey()
            : summary.getNodeKey();
        trx.setObjectKeyName("y");
        assertRenamedPath(summary, renamedPath, new QNm("x"), new QNm("y"), NodeKind.OBJECT_NAMED_OBJECT);
        final long anchor = position == Position.FIRST_CHILD
            ? destinationParent
            : child(trx, destinationParent, position == Position.LEFT_SIBLING
                ? "target"
                : "last");
        assertTrue(trx.moveTo(anchor));
        switch (position) {
          case FIRST_CHILD -> trx.moveSubtreeToFirstChild(source);
          case LEFT_SIBLING -> trx.moveSubtreeToLeftSibling(source);
          case RIGHT_SIBLING -> trx.moveSubtreeToRightSibling(source);
        }
        assertTrue(trx.moveTo(source));
        assertEquals(destinationParent, trx.getParentKey());
        assertJsonMatchesFresh(database, session, trx, versioning);
      }
    } finally {
      Databases.removeDatabase(path);
    }
  }

  private static void assertRenamedPath(final PathSummaryReader summary, final long pathNodeKey, final QNm oldName,
      final QNm newName, final NodeKind kind) {
    assertTrue(summary.moveTo(pathNodeKey));
    final long parentPath = summary.getParentKey();
    assertEquals(newName, summary.getName());
    assertEquals(-1L, summary.findChild(parentPath, oldName, kind));
    assertEquals(pathNodeKey, summary.findChild(parentPath, newName, kind));
    assertFalse(summary.match(oldName, 0, kind).get((int) pathNodeKey));
    assertTrue(summary.match(newName, 0, kind).get((int) pathNodeKey));
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void renamedJsonFieldMovesBetweenAnonymousObjects(final VersioningType versioning) throws Exception {
    final Path path = directory.resolve("renamed-json");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (final var database = Databases.openJsonDatabase(path)) {
      database.createResource(configuration("data", versioning));
      try (final var session = database.beginResourceSession("data"); final JsonNodeTrx trx = session.beginNodeTrx()) {
        trx.insertSubtreeAsFirstChild(
            JsonShredder.createStringReader("[{\"item\":{},\"p:item\":{},\"{urn:a}item\":{}},{}]"),
            JsonNodeTrx.Commit.NO);
        trx.commit();
        final long field = namedKeys(trx, "item").getFirst();
        assertTrue(trx.moveTo(field));
        trx.setObjectKeyName("renamed");
        trx.commit();
        final long prefixed = namedKeys(trx, "p:item").getFirst();
        final long uri = namedKeys(trx, "{urn:a}item").getFirst();
        assertTrue(trx.moveTo(uri));
        trx.moveSubtreeToLeftSibling(prefixed);
        assertEquals(uri, trx.getNodeKey());
        assertJsonMatchesFresh(database, session, trx, versioning);
        trx.moveToDocumentRoot();
        assertTrue(trx.moveToFirstChild());
        assertTrue(trx.moveToLastChild());
        final long target = trx.getNodeKey();
        trx.moveSubtreeToFirstChild(field);
        assertTrue(trx.moveTo(field));
        assertEquals(target, trx.getParentKey());
        assertJsonMatchesFresh(database, session, trx, versioning);
      }
    } finally {
      Databases.removeDatabase(path);
    }
  }

  private static Map<String, Entry> assertXmlMatchesFresh(final Database<XmlResourceSession> database,
      final XmlResourceSession session, final XmlNodeTrx trx, final VersioningType versioning) throws Exception {
    final Map<String, Entry> live = snapshot(trx.getPathSummary());
    final List<String> paths = assignments(trx, trx.getPathSummary());
    trx.commit();
    final String resource = "fresh-" + trx.getRevisionNumber();
    database.createResource(configuration(resource, versioning));
    try (final var freshSession = database.beginResourceSession(resource);
        final XmlNodeTrx fresh = freshSession.beginNodeTrx()) {
      final ByteArrayOutputStream output = new ByteArrayOutputStream();
      XmlSerializer.newBuilder(session, output).build().call();
      fresh.insertSubtreeAsFirstChild(XmlShredder.createStringReader(output.toString(StandardCharsets.UTF_8)),
          XmlNodeTrx.Commit.No);
      assertEquals(snapshot(fresh.getPathSummary()), live);
      assertEquals(assignments(fresh, fresh.getPathSummary()), paths);
      fresh.commit();
      try (final var summary = session.openPathSummary(); final var reader = session.beginNodeReadOnlyTrx()) {
        assertEquals(live, snapshot(summary));
        assertEquals(paths, assignments(reader, summary));
      }
    }
    return live;
  }

  private static Map<String, Entry> assertJsonMatchesFresh(final Database<JsonResourceSession> database,
      final JsonResourceSession session, final JsonNodeTrx trx, final VersioningType versioning) throws Exception {
    final Map<String, Entry> live = snapshot(trx.getPathSummary());
    final List<String> paths = assignments(trx, trx.getPathSummary());
    trx.commit();
    final String resource = "fresh-" + trx.getRevisionNumber();
    database.createResource(configuration(resource, versioning));
    try (final var freshSession = database.beginResourceSession(resource);
        final JsonNodeTrx fresh = freshSession.beginNodeTrx()) {
      final StringWriter output = new StringWriter();
      JsonSerializer.newBuilder(session, output).build().call();
      fresh.insertSubtreeAsFirstChild(JsonShredder.createStringReader(output.toString()), JsonNodeTrx.Commit.NO);
      assertEquals(snapshot(fresh.getPathSummary()), live);
      assertEquals(assignments(fresh, fresh.getPathSummary()), paths);
      fresh.commit();
      try (final var summary = session.openPathSummary(); final var reader = session.beginNodeReadOnlyTrx()) {
        assertEquals(live, snapshot(summary));
        assertEquals(paths, assignments(reader, summary));
      }
    }
    return live;
  }

  private static ResourceConfiguration configuration(final String resource, final VersioningType versioning) {
    return ResourceConfiguration.newBuilder(resource).versioningApproach(versioning).buildPathSummary(true).build();
  }

  private static <T extends NodeReadOnlyTrx & NodeCursor> List<Long> namedKeys(final T trx, final String name) {
    trx.moveToDocumentRoot();
    final List<Long> keys = new ArrayList<>();
    final DescendantAxis axis = new DescendantAxis(trx);
    while (axis.hasNext()) {
      final long key = axis.nextLong();
      if (trx.getName() != null && name.equals(trx.getName().getLocalName())) {
        keys.add(key);
      }
    }
    return keys;
  }

  private static <T extends NodeReadOnlyTrx & NodeCursor> long child(final T trx, final long parent,
      final String name) {
    assertTrue(trx.moveTo(parent));
    assertTrue(trx.moveToFirstChild());
    do {
      if (trx.getName() != null && name.equals(trx.getName().getLocalName())) {
        return trx.getNodeKey();
      }
    } while (trx.moveToRightSibling());
    throw new AssertionError("Missing child " + name);
  }

  private static <T extends NodeReadOnlyTrx & NodeCursor> List<String> assignments(final T trx,
      final PathSummaryReader summary) {
    trx.moveToDocumentRoot();
    final List<String> paths = new ArrayList<>();
    final DescendantAxis axis = new DescendantAxis(trx);
    while (axis.hasNext()) {
      axis.nextLong();
      if (trx.getName() != null || trx.getKind() == NodeKind.ARRAY) {
        assertTrue(summary.moveTo(trx.getPathNodeKey()));
        paths.add(trx.getKind() + ":" + summary.getPath());
      }
      if (trx instanceof XmlNodeReadOnlyTrx xml && trx.getKind() == NodeKind.ELEMENT) {
        final long key = trx.getNodeKey();
        final int namespaces = xml.getNamespaceCount();
        final int attributes = xml.getAttributeCount();
        for (int i = 0; i < namespaces; i++) {
          xml.moveToNamespace(i);
          assertTrue(summary.moveTo(xml.getPathNodeKey()));
          paths.add("namespace:" + summary.getPath());
          xml.moveTo(key);
        }
        for (int i = 0; i < attributes; i++) {
          xml.moveToAttribute(i);
          assertTrue(summary.moveTo(xml.getPathNodeKey()));
          paths.add("attribute:" + summary.getPath());
          xml.moveTo(key);
        }
      }
    }
    return paths;
  }

  private record Entry(NodeKind kind, QNm name, int level, int references, long children) {
  }

  private static Map<String, Entry> snapshot(final PathSummaryReader summary) {
    summary.moveToDocumentRoot();
    final Map<String, Entry> paths = new HashMap<>();
    final DescendantAxis axis = new DescendantAxis(summary);
    while (axis.hasNext()) {
      axis.nextLong();
      final String path = summary.getPath().toString();
      assertNotNull(summary.getPathNode());
      assertTrue(paths.put(path, new Entry(summary.getPathKind(), summary.getName(), summary.getLevel(),
          summary.getReferences(), summary.getChildCount())) == null, "duplicate path: " + path);
    }
    return paths;
  }
}
