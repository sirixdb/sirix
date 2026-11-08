package io.sirix.query;

import com.google.gson.JsonParser;
import io.brackit.query.BrackitQueryContext;
import io.brackit.query.ErrorCode;
import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Int32;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.node.parser.DocumentParser;
import io.brackit.query.sequence.AbstractSequence;
import io.brackit.query.sequence.BaseIter;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.node.BasicXmlDBStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 10, threadMode = ThreadMode.SEPARATE_THREAD)
final class JsonDBSerializerTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @ValueSource(strings = {"<index/>", "document { <index/> }", "attribute name {'g'}", "text {'g'}", "comment {'g'}",
      "processing-instruction index {'g'}", "function($x) { $x }"})
  void unsupportedItemsAreRejectedAndTheirIteratorIsClosed(final String query) {
    final Item unsupported = (Item) new Query(query).execute(new BrackitQueryContext());
    final AtomicBoolean closed = new AtomicBoolean();
    final Sequence sequence = new AbstractSequence() {
      @Override
      public Iter iterate() {
        return new BaseIter() {
          private int position;

          @Override
          public Item next() {
            return switch (position++) {
              case 0 -> Int32.ONE;
              case 1 -> unsupported;
              default -> throw new AssertionError("Serialization must stop at the unsupported item");
            };
          }

          @Override
          public void close() {
            closed.set(true);
          }
        };
      }
    };
    final StringBuilder output = new StringBuilder();
    try (final JsonDBSerializer serializer = new JsonDBSerializer(output, false)) {
      final QueryException failure = assertThrows(QueryException.class, () -> serializer.serialize(sequence));
      assertEquals(ErrorCode.ERR_TYPE_INAPPROPRIATE_TYPE, failure.getCode());
      assertTrue(failure.getMessage().contains("Cannot serialize"));
      assertTrue(closed.get());
      assertEquals("{\"rest\":[1,", output.toString());
    }
  }

  @Test
  void storedXmlNodesAreRejectedWithoutCastingTheirTransactionToJson() {
    try (final var store = BasicXmlDBStore.newBuilder().location(directory).build();
        final var context = SirixQueryContext.createWithNodeStore(store);
        final var chain = SirixCompileChain.createWithNodeStore(store)) {
      store.create("xml", new DocumentParser("<root/>"));
      final Sequence result = new Query(chain, "xn:doc('xml','resource1')").execute(context);
      try (final JsonDBSerializer serializer = new JsonDBSerializer(new StringBuilder(), false)) {
        assertThrows(QueryException.class, () -> serializer.serialize(result));
      }
    }
  }

  @ParameterizedTest
  @CsvSource(delimiter = '|',
      value = {"()|[]", "1|[1]", "(1,true(),null,\"text\")|[1,true,null,\"text\"]", "[1,2]|[[1,2]]",
          "{\"g\":[1,true(),null]}|[{\"g\":[1,true,null]}]",
          "({\"g\":1},\"text\",[2],{\"g\":[3]},{\"g\":4})|[{\"g\":1},\"text\",[2],{\"g\":[3]},{\"g\":4}]"})
  void supportedResultsRemainValidJson(final String query, final String expected) {
    final Sequence result = new Query(query).execute(new BrackitQueryContext());
    final StringBuilder output = new StringBuilder();
    try (final JsonDBSerializer serializer = new JsonDBSerializer(output, false)) {
      serializer.serialize(result);
    }
    assertEquals(JsonParser.parseString(expected),
        JsonParser.parseString(output.toString()).getAsJsonObject().getAsJsonArray("rest"));
  }

  @Test
  void storedJsonContainersStillUseJsonSerialization() {
    try (final var store = BasicJsonDBStore.newBuilder().location(directory).build();
        final var context = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {
      store.create("json", "records", "[{\"g\":1}]");
      final StringBuilder output = new StringBuilder();
      try (final JsonDBSerializer serializer = new JsonDBSerializer(output, false)) {
        serializer.serialize(new Query(chain, "jn:doc('json','records')").execute(context));
      }
      assertEquals(1,
          JsonParser.parseString(output.toString())
                    .getAsJsonObject()
                    .getAsJsonArray("rest")
                    .get(0)
                    .getAsJsonObject()
                    .getAsJsonArray("revision")
                    .get(0)
                    .getAsJsonObject()
                    .get("g")
                    .getAsInt());
    }
  }
}
