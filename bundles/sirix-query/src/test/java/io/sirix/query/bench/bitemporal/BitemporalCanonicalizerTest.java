package io.sirix.query.bench.bitemporal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class BitemporalCanonicalizerTest {

  @TempDir
  private Path temporaryDirectory;

  @Test
  void canonicalizesWithoutSortingOrNumericCoercion() throws Exception {
    final BitemporalQueries.Query query = BitemporalQueries.all().get(0);
    final Path output = temporaryDirectory.resolve("q1.tsv");

    final BitemporalCanonicalizer.Result result =
        BitemporalCanonicalizer.write(query, "{\"id\":1,\"cost\":12345,\"qty\":7,\"grade\":2}", output);

    assertEquals(1, result.rows());
    assertEquals("1\t12345\t7\t2\n", Files.readString(output));

    final Path fractional = temporaryDirectory.resolve("fractional.tsv");
    assertThrows(IllegalArgumentException.class,
        () -> BitemporalCanonicalizer.write(query, "{\"id\":1,\"cost\":1.5,\"qty\":7,\"grade\":2}", fractional));

    final Path overflow = temporaryDirectory.resolve("overflow.tsv");
    assertThrows(IllegalArgumentException.class, () -> BitemporalCanonicalizer.write(query,
        "{\"id\":9223372036854775808,\"cost\":1,\"qty\":7,\"grade\":2}", overflow));
  }

  @Test
  void rejectsDuplicateOrUnorderedKeys() {
    final BitemporalQueries.Query query = BitemporalQueries.all().get(0);
    final String duplicate = """
        {"id":1,"cost":10,"qty":1,"grade":0}
        {"id":1,"cost":20,"qty":2,"grade":1}
        """;
    assertThrows(IllegalArgumentException.class,
        () -> BitemporalCanonicalizer.write(query, duplicate, temporaryDirectory.resolve("duplicate.tsv")));

    final String unordered = """
        {"id":2,"cost":10,"qty":1,"grade":0}
        {"id":1,"cost":20,"qty":2,"grade":1}
        """;
    assertThrows(IllegalArgumentException.class,
        () -> BitemporalCanonicalizer.write(query, unordered, temporaryDirectory.resolve("unordered.tsv")));
  }

  @Test
  void queryCatalogIsCompleteAndDeclaresHalfOpenResiduals() {
    final List<BitemporalQueries.Query> queries = BitemporalQueries.all();
    assertEquals(12, queries.size());
    for (int index = 0; index < queries.size(); index++) {
      final BitemporalQueries.Query query = queries.get(index);
      assertEquals(index + 1, query.index());
      assertTrue(query.text().contains("local:slice"));
    }
    assertEquals(List.of(4, 6, 7, 8, 9, 11, 12),
        queries.stream()
               .filter(BitemporalQueries.Query::strictEndResidual)
               .map(BitemporalQueries.Query::index)
               .toList());
  }
}
