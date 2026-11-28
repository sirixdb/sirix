/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.scan;

import io.brackit.query.compiler.optimizer.PredicateNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * QRY-102: the nested member path's representation,
 * {@code PredicateNode.ArrayContainsPath(List<String>
 * path, String value)} (decision D1), as sirix consumes it from the brackit snapshot. The brackit
 * repository's planned counterpart belongs to step 2; this one pins the contract the sirix route
 * depends on. The other half of QRY-102's evidence, that sirix compiles against both the current
 * and the new brackit, is the compile itself (step 1) and has no test.
 */
@QryPlanned(step = QrySteps.BRACKIT_DETECTION, value = "QRY-102")
final class PredicateNodeMemberPathTest {

  private static final List<String> USER_TAGS = List.of("user", "tags");

  @Test
  @DisplayName("QRY-102: the leaf keeps its ordered path and literal")
  void keepsItsPathAndLiteral() {
    final PredicateNode leaf = QrySeam.arrayContainsPath(USER_TAGS, "admin");
    assertTrue(leaf instanceof PredicateNode, "ArrayContainsPath is a PredicateNode");
    assertEquals(USER_TAGS, QrySeam.pathOf(leaf));
    assertEquals("admin", QrySeam.valueOf(leaf));
  }

  @Test
  @DisplayName("QRY-102: the path is immutable and copied from the caller's list")
  void pathIsImmutableAndCopied() {
    final List<String> mutable = new ArrayList<>(USER_TAGS);
    final PredicateNode leaf = QrySeam.arrayContainsPath(mutable, "admin");
    mutable.set(1, "groups");
    assertEquals(USER_TAGS, QrySeam.pathOf(leaf), "a later change to the caller's list changed the leaf");
    assertThrows(UnsupportedOperationException.class, () -> QrySeam.pathOf(leaf).add("x"));
  }

  @Test
  @DisplayName("QRY-102: a path has at least one step")
  void emptyPathIsRejected() {
    QrySeam.arrayContainsPath(List.of("tags"), "admin"); // the leaf exists: a missing seam is not the rejection
    assertThrows(RuntimeException.class, () -> QrySeam.arrayContainsPath(List.of(), "admin"));
  }

  @Test
  @DisplayName("QRY-102: collectFields keeps the path's first step, never a joined name")
  void collectFieldsKeepsThePath() {
    final Set<String> fields = new HashSet<>();
    QrySeam.arrayContainsPath(USER_TAGS, "admin").collectFields(fields);
    assertTrue(fields.contains("user"), "the record field the leaf reads is its first step: " + fields);
    for (final String field : fields) {
      assertFalse(field.contains(".") || field.contains("/"), "a path was joined into one name: " + field);
    }
  }

  @Test
  @DisplayName("QRY-102: the leaf excludes records missing p1, and a later step alone does not anchor it")
  void anchorsOnTheFirstStepOnly() {
    final PredicateNode leaf = QrySeam.arrayContainsPath(USER_TAGS, "admin");
    assertTrue(leaf.excludesRecordsMissingField("user"), "a membership leaf is false on every record lacking p1");
    assertFalse(leaf.satisfiedWhenFieldMissing("user"));
    assertEquals("user", leaf.findSoundAnchorField(), "the anchor field of a member path is p1, never pj with j > 1");
    final PredicateNode deep = QrySeam.arrayContainsPath(List.of("user", "profile", "tags"), "admin");
    assertEquals("user", deep.findSoundAnchorField(), "an inner step made the leaf anchorable on its own");
  }

  @Test
  @DisplayName("QRY-102: equality and toString distinguish [user, tags] from [user.tags]")
  void equalityIsStepByStep() {
    final PredicateNode nested = QrySeam.arrayContainsPath(USER_TAGS, "admin");
    final PredicateNode dotted = QrySeam.arrayContainsPath(List.of("user.tags"), "admin");
    final PredicateNode slashed = QrySeam.arrayContainsPath(List.of("user/tags"), "admin");
    assertNotEquals(nested, dotted);
    assertNotEquals(nested.toString(), dotted.toString());
    assertNotEquals(dotted, slashed);
    assertNotEquals(nested, QrySeam.arrayContainsPath(List.of("tags", "user"), "admin"), "steps are ordered");
    assertNotEquals(nested, QrySeam.arrayContainsPath(USER_TAGS, "guest"), "literals differ");
    final PredicateNode same = QrySeam.arrayContainsPath(new ArrayList<>(USER_TAGS), "admin");
    assertEquals(nested, same);
    assertEquals(nested.hashCode(), same.hashCode());
  }

  @Test
  @DisplayName("QRY-102: a depth-1 path means exactly what ArrayContains means")
  void depthOneMeansArrayContains() {
    final PredicateNode path = QrySeam.arrayContainsPath(List.of("tags"), "admin");
    final PredicateNode contains = new PredicateNode.ArrayContains("tags", "admin");
    final Set<String> pathFields = new HashSet<>();
    final Set<String> containsFields = new HashSet<>();
    path.collectFields(pathFields);
    contains.collectFields(containsFields);
    assertEquals(containsFields, pathFields);
    for (final String field : new String[] {"tags", "user", "other"}) {
      assertEquals(contains.excludesRecordsMissingField(field), path.excludesRecordsMissingField(field), field);
      assertEquals(contains.satisfiedWhenFieldMissing(field), path.satisfiedWhenFieldMissing(field), field);
    }
    assertEquals(contains.findSoundAnchorField(), path.findSoundAnchorField());
    assertEquals(contains.isDecomposablyAnchorable(), path.isDecomposablyAnchorable());
  }

  @Test
  @DisplayName("QRY-102: ArrayContains stays unchanged")
  void arrayContainsStaysUnchanged() {
    final PredicateNode.ArrayContains contains = new PredicateNode.ArrayContains("tags", "admin");
    assertEquals("tags", contains.field());
    assertEquals("admin", contains.value());
    assertEquals("tags", contains.findSoundAnchorField());
  }
}
