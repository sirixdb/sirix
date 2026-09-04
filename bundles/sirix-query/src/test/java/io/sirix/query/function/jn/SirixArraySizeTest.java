package io.sirix.query.function.jn;

import io.brackit.query.ErrorCode;
import io.brackit.query.Query;
import io.brackit.query.QueryException;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
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
        assertEquals(0L, result(chain, context, "let $arrays := ([], [1,2]) return count($arrays[])"));
        assertEquals(0L, result(genericChain, context, "let $arrays := ([], [1,2]) return count($arrays[])"));
        assertEquals(1L, result(chain, context, "let $arrays := ([1], [], [2]) return count($arrays[])"));
        assertEquals(1L, result(genericChain, context, "let $arrays := ([1], [], [2]) return count($arrays[])"));
        assertEquals(0L, result(chain, context, "count(()[])"));
        assertEquals(storedServingCount, SirixArraySize.storedArraySizesServedCount(),
            "empty and mixed in-memory inputs must not emit storage-native evidence");

        final QueryException typeError = assertThrows(QueryException.class, () -> result(chain, context, "count(1[])"));
        assertEquals(ErrorCode.ERR_TYPE_INAPPROPRIATE_TYPE, typeError.getCode(),
            "a lone non-array item must retain ArrayAccessExpr's XPTY0004 behavior");
        assertSameTypeError(chain, genericChain, context, "count(([1,2],[3])[])");
        assertSameTypeError(chain, genericChain, context, "count((1,2)[])");
        assertEquals(storedServingCount, SirixArraySize.storedArraySizesServedCount(),
            "a failed accessor must never emit outcome evidence");
      }
    }
  }

  private static long result(final CompileChain chain, final SirixQueryContext context, final String query) {
    return ((IntNumeric) ExprUtil.asItem(new Query(chain, query).evaluate(context))).longValue();
  }

  private static void assertSameTypeError(final CompileChain optimized, final CompileChain generic,
      final SirixQueryContext context, final String query) {
    final QueryException optimizedError = assertThrows(QueryException.class, () -> result(optimized, context, query));
    final QueryException genericError = assertThrows(QueryException.class, () -> result(generic, context, query));
    assertEquals(genericError.getCode(), optimizedError.getCode());
  }
}
