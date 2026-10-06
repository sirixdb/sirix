package io.sirix.query.function.xml.index;

import io.brackit.query.Query;
import io.brackit.query.compiler.CompileChain;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.node.BasicXmlDBStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class CASStringBoundDifferentialTest {
  private static final String SOURCE = "xn:doc('xml-cas','resource1')";

  @TempDir
  Path directory;

  @BeforeEach
  void loadFixture() {
    run("xn:store('xml-cas',(),<root><title>a</title></root>)", true);
    run("let $doc := " + SOURCE + " let $idx := xn:create-cas-index($doc,'xs:string','/root/title')"
        + " return sdb:commit($doc)", true);
  }

  static Stream<Arguments> orderingBounds() {
    return Stream.of("b", "\uD800")
                 .flatMap(
                     literal -> Stream.of(Arguments.of(literal, "<", "lt", "a"), Arguments.of(literal, "<=", "le", "a"),
                         Arguments.of(literal, ">", "gt", ""), Arguments.of(literal, ">=", "ge", "")));
  }

  @ParameterizedTest
  @MethodSource("orderingBounds")
  void publicSingleComparisonScanMatchesTheInterpreter(final String literal, final String operator,
      final String comparison, final String expected) {
    final String plain =
        "for $n in " + SOURCE + "/root/title where string($n) " + comparison + " '" + literal + "' return string($n)";
    final String interpreted = run(plain, false);
    assertEquals(expected, interpreted);

    final String indexed = "let $doc := " + SOURCE + " return xn:scan-cas-index($doc,"
        + "xn:find-cas-index($doc,'xs:string','/root/title'),'" + literal + "',false(),'" + operator
        + "','/root/title')";
    assertEquals(interpreted, run("for $n in (" + indexed + ") return string($n)", true), indexed);
  }

  private String run(final String expression, final boolean optimized) {
    try (final var store = BasicXmlDBStore.newBuilder().location(directory).build();
        final var context = SirixQueryContext.createWithNodeStore(store);
        final var chain = SirixCompileChain.createWithNodeStore(store)) {
      final StringWriter output = new StringWriter();
      try (final PrintWriter writer = new PrintWriter(output)) {
        new Query(optimized
            ? chain
            : new CompileChain(), expression).serialize(context, writer);
      }
      return output.toString();
    }
  }
}
