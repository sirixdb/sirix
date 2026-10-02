package io.sirix.query.compiler.optimizer;

import io.brackit.query.Query;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Hand-computed answers for repeated array iteration and correlated nested FLWORs. The nested joins
 * exercise Brackit's TableJoin with a wider right input than left input: changing the driving side
 * must not change the join key, tuple layout, or result.
 */
final class LetBoundSequenceTest {

  @TempDir
  Path directory;

  private static final String CONTRACTS =
      "[{\"id\":1,\"pid\":10,\"cost\":5,\"vf\":\"2024-01-01T00:00:00Z\",\"vt\":\"2024-07-01T00:00:00Z\"},"
          + "{\"id\":2,\"pid\":20,\"cost\":7,\"vf\":\"2024-03-01T00:00:00Z\",\"vt\":\"2024-09-01T00:00:00Z\"},"
          + "{\"id\":3,\"pid\":10,\"cost\":9,\"vf\":\"2024-06-01T00:00:00Z\",\"vt\":\"2025-01-01T00:00:00Z\"},"
          + "{\"id\":4,\"pid\":30,\"cost\":2,\"vf\":\"2023-01-01T00:00:00Z\",\"vt\":\"2024-02-01T00:00:00Z\"}]";
  private static final String PRODUCTS =
      "[{\"id\":10,\"category\":\"a\",\"retail\":20,\"vf\":\"2024-01-01T00:00:00Z\",\"vt\":\"2024-08-01T00:00:00Z\"},"
          + "{\"id\":20,\"category\":\"b\",\"retail\":30,\"vf\":\"2024-04-01T00:00:00Z\",\"vt\":\"2025-01-01T00:00:00Z\"},"
          + "{\"id\":10,\"category\":\"c\",\"retail\":25,\"vf\":\"2024-08-01T00:00:00Z\",\"vt\":\"2025-01-01T00:00:00Z\"}]";

  @Test
  void inlineUnboxedSequenceCanBeReiterated() {
    try (final BasicJsonDBStore store = newStore()) {
      assertAnswer(store, "10 20 10 20 10 20",
          "let $products := [10,20][] for $c in 1 to 3 for $p in $products return $p");
    }
  }

  @Test
  void inlineUnboxingWithoutLetCanBeReiterated() {
    try (final BasicJsonDBStore store = newStore()) {
      assertAnswer(store, "10 20 10 20 10 20", "for $c in 1 to 3 for $p in [10,20][] return $p");
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"jn:doc('products','data')", "jn:open('products','data',current-dateTime())"})
  void storedUnboxedSequenceCanBeReiterated(final String source) {
    try (final BasicJsonDBStore store = newStore()) {
      store.create("products", "data", "[10,20]");
      assertAnswer(store, "10 20 10 20 10 20",
          "let $products := " + source + "[] for $c in 1 to 3 for $p in $products return $p");
    }
  }

  @ParameterizedTest(name = "stored={0}")
  @ValueSource(booleans = {false, true})
  void letBoundNestedFlworCountsOnlyMatchingProducts(final boolean stored) {
    try (final BasicJsonDBStore store = newStore()) {
      // c1: p10a; c2: p20; c3: p10a and p10c; c4: no product 30.
      final String expected =
          "{\"c\":1,\"matches\":1} {\"c\":2,\"matches\":1}" + " {\"c\":3,\"matches\":2} {\"c\":4,\"matches\":0}";
      assertAnswer(store, expected, prolog(store, stored) + "for $c in $C[]"
          + " let $m := (for $p in $P[] where xs:dateTime($p.vf) lt xs:dateTime($c.vt) and $p.id eq $c.pid return $p.category)"
          + " order by $c.id return {\"c\":$c.id,\"matches\":count($m)}");
    }
  }

  @ParameterizedTest(name = "stored={0}")
  @ValueSource(booleans = {false, true})
  void antiJoinWithTwoPredicatesExcludesOnlyMatchedContracts(final boolean stored) {
    try (final BasicJsonDBStore store = newStore()) {
      // c1 and c3 have later-starting product 10, c2 has later-starting product 20; c4 alone survives.
      assertAnswer(store, "4", prolog(store, stored) + "for $c in $C[]"
          + " where empty(for $p in $P[] where xs:dateTime($p.vf) gt xs:dateTime($c.vf) and $p.id eq $c.pid return $p)"
          + " order by $c.id return $c.id");
    }
  }

  @ParameterizedTest(name = "stored={0}")
  @ValueSource(booleans = {false, true})
  void hoistedProductsGiveTheSameOverlapJoinAnswer(final boolean stored) {
    try (final BasicJsonDBStore store = newStore()) {
      final String prolog = prolog(store, stored);
      final String suffix = " where $c.pid eq $p.id and xs:dateTime($c.vf) lt $U and $L lt xs:dateTime($c.vt)"
          + " and xs:dateTime($p.vf) lt $U and $L lt xs:dateTime($p.vt)"
          + " and xs:dateTime($c.vf) lt xs:dateTime($p.vt) and xs:dateTime($p.vf) lt xs:dateTime($c.vt)"
          + " let $category := $p.category, $margin := $p.retail - $c.cost"
          + " group by $category let $n := count($margin), $min := min($margin), $max := max($margin)"
          + " order by $category return {\"category\":$category,\"n\":$n,\"min\":$min,\"max\":$max}";
      // a: c1 (20-5) and c3 (20-9); b: c2 (30-7); c: c3 (25-9).
      final String expected =
          "{\"category\":\"a\",\"n\":2,\"min\":11,\"max\":15}" + " {\"category\":\"b\",\"n\":1,\"min\":23,\"max\":23}"
              + " {\"category\":\"c\",\"n\":1,\"min\":16,\"max\":16}";
      assertAnswer(store, expected, prolog + "for $c in $C[] for $p in $P[]" + suffix);
      assertAnswer(store, expected, prolog + "let $products := $P[] for $c in $C[] for $p in $products" + suffix);
    }
  }

  @ParameterizedTest(name = "comparison={0}")
  @CsvSource(value = {"lt|[2,1]", "le|[2,1] [2,2]", "gt|[2,3]", "ge|[2,2] [2,3]", "eq|[2,2]", "<|[2,1]",
      "<=|[2,1] [2,2]", ">|[2,3]", ">=|[2,2] [2,3]", "=|[2,2]"}, delimiter = '|')
  void indexedJoinPreservesComparisonWhenProductsAreHoisted(final String comparison, final String expected) {
    final String products = "[{\"v\":1},{\"v\":2},{\"v\":3}]";
    final String suffix = " where $c.id eq 2 and $p.v " + comparison + " $c.v order by $c.id,$p.v return [$c.id,$p.v]";
    try (final BasicJsonDBStore store = newStore()) {
      assertAnswer(store, expected,
          "let $products := " + products + "[] for $c in [{\"id\":1,\"v\":1},{\"id\":2,\"v\":2},{\"id\":3,\"v\":3}][]"
              + " for $p in $products" + suffix);

      // Enough nodes for the cost model to prefer the CAS index. A tiny stored fixture masks the
      // bug by choosing a scan and never performing the index-driven join-input swap.
      final StringBuilder contracts = new StringBuilder(6_000).append('[');
      for (int id = 1; id <= 300; id++) {
        if (id > 1) {
          contracts.append(',');
        }
        contracts.append("{\"id\":").append(id).append(",\"v\":").append(id).append('}');
      }
      contracts.append(']');
      store.create("contracts", "data", contracts.toString());
      store.create("products", "data", products);
      final String contractsSource = "jn:open('contracts','data',current-dateTime())[]";
      final String productsSource = "jn:open('products','data',current-dateTime())[]";
      final String hoisted =
          "let $products := " + productsSource + " for $c in " + contractsSource + " for $p in $products" + suffix;
      assertAnswer(store, expected, hoisted);

      try (final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
          final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
        new Query(chain,
            "let $doc := jn:doc('contracts','data')"
                + " let $index := jn:create-cas-index($doc,'xs:integer','/[]/v') return sdb:commit($doc)").evaluate(
                    context);
      }
      // The one selected contract has v=2. Products 1, 2 and 3 exercise below, equal and above.
      assertAnswer(store, expected, "for $c in " + contractsSource + " for $p in " + productsSource + suffix);
      assertAnswer(store, expected, hoisted);
    }
  }

  private BasicJsonDBStore newStore() {
    return BasicJsonDBStore.newBuilder().location(directory).build();
  }

  private static String prolog(final BasicJsonDBStore store, final boolean stored) {
    if (stored) {
      store.create("contracts", "data", CONTRACTS);
      store.create("products", "data", PRODUCTS);
    }
    final String contracts = stored
        ? "jn:doc('contracts','data')"
        : CONTRACTS;
    final String products = stored
        ? "jn:open('products','data',current-dateTime())"
        : PRODUCTS;
    return "declare variable $L := xs:dateTime('2024-02-15T00:00:00Z');"
        + "declare variable $U := xs:dateTime('2024-10-15T00:00:00Z');" + "declare variable $C := " + contracts
        + ";declare variable $P := " + products + ";";
  }

  private static void assertAnswer(final BasicJsonDBStore store, final String expected, final String query) {
    final StringWriter out = new StringWriter();
    try (final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store);
        final PrintWriter writer = new PrintWriter(out)) {
      new Query(chain, query).serialize(context, writer);
      writer.flush();
      assertEquals(expected, out.toString().trim(), query);
    }
  }
}
