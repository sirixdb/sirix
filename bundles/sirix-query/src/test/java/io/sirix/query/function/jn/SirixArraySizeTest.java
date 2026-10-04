package io.sirix.query.function.jn;

import io.brackit.query.Query;
import io.brackit.query.atomic.IntNumeric;
import io.brackit.query.compiler.CompileChain;
import io.brackit.query.util.ExprUtil;
import io.sirix.access.Databases;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class SirixArraySizeTest {

  private Path location;

  @AfterEach
  void tearDown() {
    SirixArraySize.resetStoredArraySizesServedForTests();
    if (location != null) {
      Databases.removeDatabase(location);
    }
  }

  @Test
  void optimizerRecordsOnlySuccessfulStoredArrayCardinalities() throws Exception {
    location = Files.createTempDirectory("sirix-stored-array-size");
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(location).build()) {
      store.create("size-db", "records", "[1,2,3]");
      final CompileChain genericChain = new CompileChain();
      try (final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
          final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
        SirixArraySize.resetStoredArraySizesServedForTests();

        assertEquals(3L, result(chain, context, "let $hits := jn:doc('size-db','records') return count($hits[])"));
        final long storedServingCount = SirixArraySize.storedArraySizesServedCount();
        assertTrue(storedServingCount > 0L, "the rewritten count must execute the stored-array accessor");

        assertEquals(3L, result(chain, context, "count(for $record in [1,2,3][] return $record)"));
        assertEquals(storedServingCount, SirixArraySize.storedArraySizesServedCount(),
            "an in-memory literal must not masquerade as storage-native serving evidence");

        assertEquals(5L, result(chain, context, "let $arrays := ([1,2], [3,4,5]) return count($arrays[])"),
            "the rewrite must retain general sequence-of-arrays semantics");
        assertEquals(5L, result(genericChain, context, "let $arrays := ([1,2], [3,4,5]) return count($arrays[])"));
        assertEquals(storedServingCount, SirixArraySize.storedArraySizesServedCount(),
            "a sequence of in-memory arrays must not emit storage-native evidence");

        assertEquals(3L, result(chain, context, "let $items := ([1,2], 9, [3]) return count($items[])"),
            "the rewrite must mirror sequence unboxing by skipping non-array members");
        assertEquals(3L, result(genericChain, context, "let $items := ([1,2], 9, [3]) return count($items[])"));
        assertEquals(2L, result(chain, context, "let $arrays := ([], [1,2]) return count($arrays[])"));
        assertEquals(2L, result(genericChain, context, "let $arrays := ([], [1,2]) return count($arrays[])"));
        assertEquals(2L, result(chain, context, "let $arrays := ([1], [], [2]) return count($arrays[])"));
        assertEquals(2L, result(genericChain, context, "let $arrays := ([1], [], [2]) return count($arrays[])"));
        assertEquals(0L, result(chain, context, "count(()[])"));
        assertEquals(storedServingCount, SirixArraySize.storedArraySizesServedCount(),
            "empty and mixed in-memory inputs must not emit storage-native evidence");

        assertEquals(0L, result(chain, context, "count(1[])"), "a lone non-array item has no array members");
        assertEquals(0L, result(genericChain, context, "count(1[])"));
        assertEquals(3L, result(chain, context, "count(([1,2],[3])[])"));
        assertEquals(3L, result(genericChain, context, "count(([1,2],[3])[])"));
        assertEquals(0L, result(chain, context, "count((1,2)[])"));
        assertEquals(0L, result(genericChain, context, "count((1,2)[])"));
        assertEquals(storedServingCount, SirixArraySize.storedArraySizesServedCount(),
            "lenient in-memory unboxing must not emit storage-native evidence");
      }
    }
  }

  private static long result(final CompileChain chain, final SirixQueryContext context, final String query) {
    return ((IntNumeric) ExprUtil.asItem(new Query(chain, query).evaluate(context))).longValue();
  }
}
