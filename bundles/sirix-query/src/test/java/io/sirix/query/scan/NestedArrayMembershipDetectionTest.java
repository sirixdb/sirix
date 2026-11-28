/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.optimizer.PredicateNode;
import io.brackit.query.compiler.optimizer.VectorizedScanAnnotation;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * QRY-101 and QRY-103: which {@code where} clauses the detector hands to the executor, read off the
 * optimized AST of sirix's own compile chain (the chain the route runs under; brackit is consumed
 * as a moving snapshot, so this is the detection sirix actually gets). QRY-101 also plans a
 * brackit-local test for the detector change of step 2; this one checks the consumed snapshot
 * through sirix.
 *
 * <p>
 * A claimed form must carry the expected annotations: {@code VECTORIZED_COUNT} for
 * {@code count(… return $r)}, {@code VECTORIZED_AGGREGATE} with {@code AGGREGATE_FIELD} for
 * {@code count}/{@code sum} of {@code $r.N}, and a {@code PREDICATE_TREE} whose conjuncts equal the
 * expected leaves. Conjuncts are compared as a multiset, because QRY-101 claims them in any order
 * and with any parenthesization; a depth-1 {@code ArrayContainsPath} and {@code ArrayContains} are
 * the same leaf (QRY-102).
 *
 * <p>
 * QRY-103's negative table is <em>not</em> planned: none of its forms may ever be claimed, so it
 * runs from today and guards step 2's brackit change the day sirix consumes it. Its disjunction row
 * uses a nested member path on purpose: sirix's executor declares decomposable support for the
 * depth-1 {@code ArrayContains} leaf today, and the detector already claims
 * {@code $r.active or (some $g in $r.tags[] satisfies $g eq "x")} as an {@code Or}; QRY-103 forbids
 * the disjunction only for a leaf kind no executor supports, which is {@code ArrayContainsPath}.
 * Brackit's grammar has no descendant deref ({@code $r..tags}) in this position, so that bullet has
 * no row: it does not parse.
 */
final class NestedArrayMembershipDetectionTest {

  private static final String DB = "qry-detection-db";
  private static final String RES = "r";
  private static final String SRC = "jn:doc('" + DB + "','" + RES + "')[]";

  private static Path dir;

  @BeforeAll
  static void storeDocument() throws Exception {
    dir = Files.createTempDirectory("sirix-qry-detection-");
    new QryFixture(dir, DB).store(RES,
        "[{\"id\":1,\"active\":true,\"verified\":true,"
            + "\"user\":{\"name\":\"a\",\"tags\":[\"admin\"],\"groups\":[\"ops\"],\"profile\":{\"tags\":[\"admin\"]}},"
            + "\"tags\":[\"admin\"],\"score\":3}]");
  }

  @AfterAll
  static void dropDocument() {
    if (dir != null) {
      new QryFixture(dir, DB).drop();
    }
  }

  // -----------------------------------------------------------------------------------------------
  // QRY-101: claimed shapes
  // -----------------------------------------------------------------------------------------------

  /** How the surrounding expression must be annotated. */
  enum Claim {
    FILTERED_COUNT, AGGREGATE
  }

  static Stream<Arguments> claimedForms() {
    final List<Arguments> rows = new ArrayList<>();
    final List<String> userTags = List.of("user", "tags");
    // Membership test forms (QRY-101 item 2), each with either operand order.
    for (final String where : new String[] {"$r.user.tags[] = \"admin\"", "\"admin\" = $r.user.tags[]",
        "some $g in $r.user.tags[] satisfies $g eq \"admin\"", "some $g in $r.user.tags[] satisfies \"admin\" eq $g",
        "some $g in $r.user.tags[] satisfies $g = \"admin\"", "some $g in $r.user.tags[] satisfies \"admin\" = $g"}) {
      rows.add(row(count(where), Claim.FILTERED_COUNT, null, List.of(member(userTags, "admin"))));
    }
    // Member paths of depth 1 and 3.
    rows.add(
        row(count("$r.tags[] = \"admin\""), Claim.FILTERED_COUNT, null, List.of(member(List.of("tags"), "admin"))));
    rows.add(row(count("$r.user.profile.tags[] = \"admin\""), Claim.FILTERED_COUNT, null,
        List.of(member(List.of("user", "profile", "tags"), "admin"))));
    // Conjunctions: any order, parentheses, several where clauses, several leaves of each kind.
    final List<Leaf> flagAndMember = List.of(flag("active"), member(userTags, "admin"));
    rows.add(row(count("$r.active and $r.user.tags[] = \"admin\""), Claim.FILTERED_COUNT, null, flagAndMember));
    rows.add(row(count("($r.user.tags[] = \"admin\") and $r.active"), Claim.FILTERED_COUNT, null, flagAndMember));
    rows.add(row("count(for $r in " + SRC + " where $r.active where $r.user.tags[] = \"admin\" return $r)",
        Claim.FILTERED_COUNT, null, flagAndMember));
    rows.add(row(count("$r.active and ($r.user.tags[] = \"admin\" and $r.verified) and $r.user.groups[] = \"ops\""),
        Claim.FILTERED_COUNT, null, List.of(flag("active"), member(userTags, "admin"), flag("verified"),
            member(List.of("user", "groups"), "ops"))));
    // The predicated aggregates: count and sum of a direct field N.
    rows.add(row("sum(for $r in " + SRC + " where $r.active and $r.user.tags[] = \"admin\" return $r.score)",
        Claim.AGGREGATE, "score", flagAndMember));
    rows.add(row("count(for $r in " + SRC + " where $r.active and $r.user.tags[] = \"admin\" return $r.score)",
        Claim.AGGREGATE, "score", flagAndMember));
    rows.add(row("sum(for $r in " + SRC + " where $r.user.tags[] = \"admin\" return $r.score)", Claim.AGGREGATE,
        "score", List.of(member(userTags, "admin"))));
    rows.add(row("sum(for $r in " + SRC + " where (some $g in $r.user.tags[] satisfies $g eq \"admin\") and $r.active "
        + "return $r.score)", Claim.AGGREGATE, "score", flagAndMember));
    return rows.stream();
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @MethodSource("claimedForms")
  @QryPlanned(step = QrySteps.BRACKIT_DETECTION, value = "QRY-101")
  @DisplayName("QRY-101: each claimed form is annotated with its predicate tree and aggregate")
  void claimedFormIsAnnotated(final String query, final Claim claim, final String field, final List<Leaf> expected)
      throws Exception {
    final AST claimed = annotatedNode(compile(query));
    assertNotNull(claimed, "not claimed: " + query);
    final Object tree = claimed.getProperty(VectorizedScanAnnotation.PREDICATE_TREE);
    assertTrue(tree instanceof PredicateNode, "no predicate tree on the claimed node: " + query);
    final PredicateNode predicate = (PredicateNode) tree;
    assertEquals(sorted(expected), sorted(leavesOf(predicate)), "conjuncts of " + query);
    switch (claim) {
      case FILTERED_COUNT -> {
        assertEquals(Boolean.TRUE, claimed.getProperty(VectorizedScanAnnotation.VECTORIZED_COUNT),
            "count(… return $r) must be claimed as the filtered count");
        assertNull(claimed.getProperty(VectorizedScanAnnotation.VECTORIZED_AGGREGATE));
      }
      case AGGREGATE -> {
        assertEquals(Boolean.TRUE, claimed.getProperty(VectorizedScanAnnotation.VECTORIZED_AGGREGATE),
            "count/sum(… return $r.N) must be claimed as the predicated aggregate");
        assertEquals(field, claimed.getProperty(VectorizedScanAnnotation.AGGREGATE_FIELD));
        // The strict-anchor rule: a predicated aggregate is claimed only with a sound anchor.
        assertNotNull(predicate.findSoundAnchorField(), "claimed without a sound anchor: " + query);
      }
      default -> fail("unknown claim " + claim);
    }
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @MethodSource("membershipOnlyAggregates")
  @QryPlanned(step = QrySteps.BRACKIT_DETECTION, value = "QRY-101")
  @DisplayName("QRY-101: a membership leaf alone anchors the aggregate on its first step")
  void membershipLeafAnchorsOnItsFirstStep(final String query) throws Exception {
    final AST claimed = annotatedNode(compile(query));
    assertNotNull(claimed, "not claimed: " + query);
    final PredicateNode predicate = (PredicateNode) claimed.getProperty(VectorizedScanAnnotation.PREDICATE_TREE);
    assertEquals("user", predicate.findSoundAnchorField(), "a membership leaf is false on every record lacking p1");
  }

  static Stream<String> membershipOnlyAggregates() {
    return Stream.of("sum(for $r in " + SRC + " where $r.user.tags[] = \"admin\" return $r.score)",
        "count(for $r in " + SRC + " where $r.user.profile.tags[] = \"admin\" return $r.score)");
  }

  // -----------------------------------------------------------------------------------------------
  // QRY-103: shapes that stay unclaimed (runs today)
  // -----------------------------------------------------------------------------------------------

  static Stream<Arguments> unclaimedForms() {
    final List<Arguments> rows = new ArrayList<>();
    // Each bullet, alone and as a conjunct beside a claimable flag (so a weaker predicate that drops
    // the leaf would be caught too).
    final String[][] bullets = {
        // value comparison over the unboxed array: a singleton comparison
        {"value comparison", "$r.user.tags[] eq \"admin\""}, {"value comparison, depth 1", "$r.tags[] eq \"admin\""},
        // != , ordering operators, every
        {"!=", "$r.user.tags[] != \"admin\""}, {"ordering", "$r.user.tags[] > \"admin\""},
        {"ordering, ge", "$r.user.tags[] ge \"admin\""},
        {"every", "every $g in $r.user.tags[] satisfies $g eq \"admin\""},
        // an index or a slice instead of []
        {"index", "$r.user.tags[[1]] = \"admin\""}, {"position", "$r.user.tags[0] = \"admin\""},
        {"slice", "$r.user.tags[1:2] = \"admin\""},
        // a non-string literal, a variable, a computed right-hand side
        {"numeric literal", "$r.user.tags[] = 1"}, {"boolean literal", "$r.user.tags[] = true()"},
        {"variable", "$r.user.tags[] = $lit"}, {"computed", "$r.user.tags[] = concat(\"ad\", \"min\")"},
        {"computed in some", "some $g in $r.user.tags[] satisfies $g eq concat(\"ad\", \"min\")"},
        // an array step or a function call in the path
        {"array step", "$r.user[].tags[] = \"admin\""}, {"function step", "exactly-one($r.user).tags[] = \"admin\""},
        // a disjunction containing a membership leaf
        {"disjunction", "$r.active or $r.user.tags[] = \"admin\""},
        {"disjunction, some", "$r.verified or (some $g in $r.user.tags[] satisfies $g eq \"admin\")"}};
    for (final String[] bullet : bullets) {
      rows.add(Arguments.of(bullet[0], count(bullet[1])));
      rows.add(Arguments.of(bullet[0] + ", conjunct", count("$r.active and (" + bullet[1] + ")")));
      rows.add(Arguments.of(bullet[0] + ", sum",
          "sum(for $r in " + SRC + " where $r.active and (" + bullet[1] + ") return $r.score)"));
    }
    return rows.stream();
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @MethodSource("unclaimedForms")
  @DisplayName("QRY-103: no membership leaf is claimed for a form with different semantics")
  void unclaimedFormIsNotClaimed(final String bullet, final String query) throws Exception {
    final AST ast = compile("declare variable $lit := \"admin\"; " + query);
    final List<AST> claims = new ArrayList<>();
    collectClaims(ast, claims);
    for (final AST claim : claims) {
      final Object tree = claim.getProperty(VectorizedScanAnnotation.PREDICATE_TREE);
      fail(bullet + ": claimed (" + (tree == null
          ? "no tree"
          : tree) + ") for " + query);
    }
  }

  // -----------------------------------------------------------------------------------------------
  // helpers
  // -----------------------------------------------------------------------------------------------

  /**
   * An expected conjunct, comparable across {@code ArrayContains} and depth-1
   * {@code ArrayContainsPath}.
   */
  record Leaf(String kind, List<String> path, String value) implements Comparable<Leaf> {
    @Override
    public int compareTo(final Leaf o) {
      return toString().compareTo(o.toString());
    }
  }

  private static Leaf flag(final String field) {
    return new Leaf("flag", List.of(field), null);
  }

  private static Leaf member(final List<String> path, final String value) {
    return new Leaf("member", path, value);
  }

  private static Arguments row(final String query, final Claim claim, final String field, final List<Leaf> leaves) {
    return Arguments.of(query, claim, field, leaves);
  }

  private static String count(final String where) {
    return "count(for $r in " + SRC + " where " + where + " return $r)";
  }

  private static List<Leaf> sorted(final List<Leaf> leaves) {
    final List<Leaf> copy = new ArrayList<>(leaves);
    copy.sort(null);
    return copy;
  }

  /** The conjuncts of {@code node}: nested {@code And}s flattened, each leaf converted. */
  private static List<Leaf> leavesOf(final PredicateNode node) {
    final List<Leaf> leaves = new ArrayList<>();
    collectLeaves(node, leaves);
    return leaves;
  }

  private static void collectLeaves(final PredicateNode node, final List<Leaf> leaves) {
    if (node instanceof PredicateNode.And and) {
      for (final PredicateNode child : and.children()) {
        collectLeaves(child, leaves);
      }
    } else if (node instanceof PredicateNode.BoolRef bool) {
      leaves.add(flag(bool.field()));
    } else if (node instanceof PredicateNode.ArrayContains contains) {
      leaves.add(member(List.of(contains.field()), contains.value()));
    } else if (QrySeam.isArrayContainsPath(node)) {
      leaves.add(member(List.copyOf(QrySeam.pathOf(node)), QrySeam.valueOf(node)));
    } else {
      leaves.add(new Leaf("other", List.of(), String.valueOf(node)));
    }
  }

  /** The first node carrying a count or aggregate claim, or {@code null}. */
  private static AST annotatedNode(final AST ast) {
    final List<AST> claims = new ArrayList<>();
    collectClaims(ast, claims);
    assertFalse(claims.size() > 1, "more than one claim in one query: " + claims.size());
    return claims.isEmpty()
        ? null
        : claims.getFirst();
  }

  private static void collectClaims(final AST node, final List<AST> claims) {
    if (node == null) {
      return;
    }
    if (node.getProperty(VectorizedScanAnnotation.VECTORIZED_COUNT) != null
        || node.getProperty(VectorizedScanAnnotation.VECTORIZED_AGGREGATE) != null
        || node.getProperty(VectorizedScanAnnotation.PREDICATE_TREE) != null) {
      claims.add(node);
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      collectClaims(node.getChild(i), claims);
    }
  }

  private static AST compile(final String query) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dir).buildPathSummary(true).build();
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, query);
      return chain.getOptimizedAST();
    }
  }
}
