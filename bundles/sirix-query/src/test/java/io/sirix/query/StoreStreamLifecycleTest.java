package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.DocumentException;
import io.brackit.query.jdm.Stream;
import io.brackit.query.node.parser.DocumentParser;
import io.brackit.query.node.parser.NodeSubtreeParser;
import io.brackit.query.node.stream.ArrayStream;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.node.BasicXmlDBStore;
import io.sirix.settings.VersioningType;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(60)
final class StoreStreamLifecycleTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void xmlImportFailureReachesCreateCallerBeforeImmediateClose(final VersioningType versioning) {
    final DocumentException failure = new DocumentException("controlled import failure");
    final CountDownLatch consumed = new CountDownLatch(1);
    final AtomicBoolean siblingCompleted = new AtomicBoolean();
    final NodeSubtreeParser failing = handler -> {
      await(consumed);
      throw failure;
    };
    final NodeSubtreeParser sibling = handler -> {
      await(consumed);
      new DocumentParser("<root><item>complete</item></root>").parse(handler);
      siblingCompleted.set(true);
    };
    try (final BasicXmlDBStore store = xmlStore(versioning)) {
      final DocumentException thrown =
          assertThrows(DocumentException.class, () -> store.create("failed", gatedStream(consumed, failing, sibling)));
      assertSame(failure, thrown.getCause());
      assertTrue(siblingCompleted.get(), "create must drain other imports even when one fails");
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void xmlStreamCreateThenImmediateClosePersistsEveryDocument(final VersioningType versioning) {
    final CountDownLatch consumed = new CountDownLatch(1);
    final NodeSubtreeParser first = handler -> {
      await(consumed);
      new DocumentParser("<root><item>a</item></root>").parse(handler);
    };
    final NodeSubtreeParser second = handler -> {
      await(consumed);
      new DocumentParser("<root><item>b</item></root>").parse(handler);
    };
    try (final BasicXmlDBStore store = xmlStore(versioning)) {
      final var collection = store.create("sequence", gatedStream(consumed, first, second));
      assertSame(collection, store.lookup("sequence"));
    }
    try (final BasicXmlDBStore store = xmlStore(versioning)) {
      final var collection = store.lookup("sequence");
      assertEquals(2, collection.getDatabase().listResources().size());
      assertEquals("a",
          collection.getDocument("resource1").getFirstChild().getFirstChild().getFirstChild().getValue().stringValue());
      assertEquals("b",
          collection.getDocument("resource2").getFirstChild().getFirstChild().getFirstChild().getValue().stringValue());
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void xmlStoreSequenceThenImmediateContextClosePersistsEveryDocument(final VersioningType versioning) {
    try (final BasicXmlDBStore store = xmlStore(versioning);
        final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store)) {
      new Query(chain,
          "xn:store('sequence',(),(<root><item xmlns='urn:a'>a</item></root>,document { <root/> }))").execute(context);
    }
    try (final BasicXmlDBStore store = xmlStore(versioning)) {
      final var collection = store.lookup("sequence");
      assertEquals(2, collection.getDatabase().listResources().size());
      assertEquals("root", collection.getDocument("resource1").getFirstChild().getName().getLocalName());
      assertEquals("root", collection.getDocument("resource2").getFirstChild().getName().getLocalName());
      assertEquals("a",
          collection.getDocument("resource1").getFirstChild().getFirstChild().getFirstChild().getValue().stringValue());
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void jsonStreamsThenImmediateContextClosePersistEveryDocument(final VersioningType versioning) throws Exception {
    final Path first = Files.writeString(directory.resolve("first.json"), "[1,2,3]");
    final Path second = Files.writeString(directory.resolve("second.json"), "{\"key\":\"value\"}");
    try (final BasicJsonDBStore store = jsonStore(versioning);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      store.createFromPaths("paths", new ArrayStream<>(new Path[] {first, second}));
      store.createFromJsonStrings("strings",
          new ArrayStream<>(new Str[] {new Str("[1,2,3]"), new Str("{\"key\":\"value\"}")}));
    }
    try (final BasicJsonDBStore store = jsonStore(versioning);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      for (final String collection : new String[] {"paths", "strings"}) {
        assertEquals(2, store.lookup(collection).getDatabase().listResources().size());
        final StringWriter output = new StringWriter();
        new Query(chain, "jn:doc('" + collection + "','resource1')").serialize(context, new PrintWriter(output));
        assertEquals("[1,2,3]", output.toString());
        output.getBuffer().setLength(0);
        new Query(chain, "jn:doc('" + collection + "','resource2')").serialize(context, new PrintWriter(output));
        assertEquals("{\"key\":\"value\"}", output.toString());
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void xmlStreamFailureDrainsImportsBeforeClosingTheirSource(final VersioningType versioning) {
    final DocumentException failure = new DocumentException("controlled stream failure");
    final CountDownLatch consumed = new CountDownLatch(1);
    final AtomicBoolean sourceClosed = new AtomicBoolean();
    final AtomicBoolean completed = new AtomicBoolean();
    final NodeSubtreeParser parser = handler -> {
      await(consumed);
      new DocumentParser("<root/>").parse(handler);
      assertFalse(sourceClosed.get(), "parser source must remain open until imports finish");
      completed.set(true);
    };
    final Stream<NodeSubtreeParser> stream = new Stream<>() {
      private boolean first = true;

      @Override
      public NodeSubtreeParser next() {
        if (first) {
          first = false;
          return parser;
        }
        consumed.countDown();
        throw failure;
      }

      @Override
      public void close() {
        sourceClosed.set(true);
        consumed.countDown();
      }
    };
    try (final BasicXmlDBStore store = xmlStore(versioning)) {
      assertSame(failure, assertThrows(DocumentException.class, () -> store.create("failed", stream)));
      assertTrue(completed.get(), "create must drain submitted imports when stream iteration fails");
      assertTrue(sourceClosed.get());
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void jsonParallelImportFailureReachesCallerBeforeImmediateClose(final VersioningType versioning) throws Exception {
    final Path valid = Files.writeString(directory.resolve("valid.json"), "[1,2,3]");
    final Path malformed = Files.writeString(directory.resolve("malformed.json"), "{\"key\":");
    try (final BasicJsonDBStore store = jsonStore(versioning);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      assertThrows(DocumentException.class,
          () -> store.createFromPaths("failed", new ArrayStream<>(new Path[] {valid, malformed})));
    }
    try (final BasicJsonDBStore store = jsonStore(versioning)) {
      assertEquals(0, store.lookup("failed").getDatabase().listResources().size());
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void interruptedXmlCreateStopsAndDrainsWorkersBeforeImmediateClose(final VersioningType versioning) {
    final CountDownLatch entered = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final AtomicBoolean parserInterrupted = new AtomicBoolean();
    final NodeSubtreeParser parser = handler -> {
      entered.countDown();
      try {
        release.await();
      } catch (final InterruptedException e) {
        parserInterrupted.set(true);
        Thread.currentThread().interrupt();
        throw new DocumentException(e);
      }
    };
    final Stream<NodeSubtreeParser> stream = new Stream<>() {
      private boolean first = true;

      @Override
      public @Nullable NodeSubtreeParser next() {
        if (first) {
          first = false;
          return parser;
        }
        // This import cannot complete until interrupted: Future.get deterministically sees the
        // caller's interrupt, independent of worker scheduling or elapsed time.
        await(entered);
        Thread.currentThread().interrupt();
        return null;
      }

      @Override
      public void close() {
        release.countDown();
      }
    };
    try (final BasicXmlDBStore store = xmlStore(versioning)) {
      try {
        final DocumentException thrown = assertThrows(DocumentException.class, () -> store.create("failed", stream));
        assertInstanceOf(InterruptedException.class, thrown.getCause());
        assertTrue(Thread.currentThread().isInterrupted());
        assertTrue(parserInterrupted.get(), "create must stop and drain its import before returning");
      } finally {
        Thread.interrupted();
        release.countDown();
      }
    }
  }

  private BasicXmlDBStore xmlStore(final VersioningType versioning) {
    return BasicXmlDBStore.newBuilder().location(directory).versioningType(versioning).build();
  }

  private BasicJsonDBStore jsonStore(final VersioningType versioning) {
    return BasicJsonDBStore.newBuilder().location(directory).versioningType(versioning).build();
  }

  private static Stream<NodeSubtreeParser> gatedStream(final CountDownLatch consumed,
      final NodeSubtreeParser... parsers) {
    return new Stream<>() {
      private int index;

      @Override
      public @Nullable NodeSubtreeParser next() {
        if (index == parsers.length) {
          consumed.countDown();
          return null;
        }
        return parsers[index++];
      }

      @Override
      public void close() {
        consumed.countDown();
      }
    };
  }

  private static void await(final CountDownLatch latch) {
    try {
      latch.await();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DocumentException(e);
    }
  }
}
