package io.sirix.access.trx.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.brackit.query.BrackitQueryContext;
import io.brackit.query.Query;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.Una;
import io.brackit.query.compiler.CompileChain;
import io.brackit.query.node.parser.DocumentParser;
import io.brackit.query.update.UpdateList;
import io.brackit.query.update.op.OpType;
import io.brackit.query.update.op.ReplaceElementContentOp;
import io.brackit.query.update.op.UpdateOp;
import io.sirix.api.Axis;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.axis.DescendantAxis;
import io.sirix.exception.SirixUsageException;
import io.sirix.io.StorageType;
import io.sirix.node.NodeKind;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.SirixQueryContext.CommitStrategy;
import io.sirix.query.node.BasicXmlDBStore;
import io.sirix.query.node.XmlDBCollection;
import io.sirix.query.node.XmlDBNode;
import io.sirix.settings.VersioningType;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

@Timeout(30)
final class XmlPendingUpdatePublicationTest {
  private static final Scenario[] SCENARIOS =
      {new Scenario("empty", "<r>old</r>", "replace value of node r/text() with ''", "<r/>"),
          new Scenario("first", "<r>old</r>", "insert node 'new' as first into r", "<r>newold</r>"),
          new Scenario("left", "<r>left<a/>right</r>", "insert node 'middle' before r/a", "<r>leftmiddle<a/>right</r>"),
          new Scenario("right", "<r>left<a/>right</r>", "insert node 'middle' after r/a", "<r>left<a/>middleright</r>"),
          new Scenario("remove", "<r>left<a/>right</r>", "delete node r/a", "<r>leftright</r>")};

  @TempDir
  Path directory;

  @AfterEach
  void clearHook() {
    AbstractNodeTrxImpl.asyncCommitTestHook = null;
  }

  @ParameterizedTest
  @MethodSource("countConfigurations")
  void countBoundariesPublishOnlyNormalizedBatches(final VersioningType versioning, final CommitStrategy strategy,
      final AfterCommitState state, final Scenario scenario) {
    checkCountUpdate(versioning, strategy, state, scenario.name(), scenario.xml(), scenario.update(),
        scenario.expected());
  }

  private void checkCountUpdate(final VersioningType versioning, final CommitStrategy strategy,
      final AfterCommitState state, final String name, final String xml, final String update, final String expected) {
    try (final BasicXmlDBStore store = newStore(versioning, name);
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store);
        final SirixQueryContext context = SirixQueryContext.createWithNodeStoreAndCommitStrategy(store, strategy)) {
      final XmlDBCollection collection = store.create("data", new DocumentParser(xml));
      final XmlDBNode original = collection.getDocument(1);
      final long rootKey = original.getFirstChild().getNodeKey();
      final XmlResourceSession session = original.getTrx().getResourceSession();
      final AtomicInteger commits = new AtomicInteger();
      final AtomicInteger invalidCommits = new AtomicInteger();
      final int batchCommits;
      boolean thresholdResumed = true;
      try (final XmlNodeTrx writer = session.beginNodeTrx(1, state)) {
        writer.addPreCommitHook(unused -> {
          commits.incrementAndGet();
          if (!isCanonical(writer)) {
            invalidCommits.incrementAndGet();
          }
        });
        context.setContextItem(original);
        new Query(chain, update).execute(context);
        batchCommits = commits.get();
        assertEquals(xml, serialize(original));
        if (strategy == CommitStrategy.EXPLICIT) {
          assertSame(writer, session.getNodeTrx().orElseThrow());
          final int generation = writer.getStorageEngineWriter().getLog().getCurrentGeneration();
          writer.moveTo(rootKey);
          writer.insertCommentAsFirstChild("outside");
          if (state == AfterCommitState.KEEP_OPEN_ASYNC_FLUSH) {
            thresholdResumed = writer.getStorageEngineWriter().getLog().getCurrentGeneration() > generation;
          } else {
            thresholdResumed = commits.get() == batchCommits + 1;
          }
          writer.remove();
          writer.commit();
        } else {
          assertTrue(writer.isClosed());
        }
      }
      checkHistory(collection, session, rootKey);
      assertEquals(0, invalidCommits.get());
      assertEquals(strategy == CommitStrategy.AUTO
          ? 1
          : 0, batchCommits);
      assertTrue(thresholdResumed, "the original count threshold must resume outside the batch");
      if (name.equals("empty")) {
        final BrackitQueryContext counts = new BrackitQueryContext();
        final Query count = new Query(new CompileChain(), "count(r/text())");
        for (int revision = 2, last = session.getMostRecentRevisionNumber(); revision <= last; revision++) {
          counts.setContextItem(collection.getDocument(revision));
          assertEquals(new Int64(0), count.execute(counts));
        }
      }
      assertEquals(expected, serialize(collection.getDocument(session.getMostRecentRevisionNumber())));
      assertEquals(xml, serialize(collection.getDocument(1)));
    }
  }

  @ParameterizedTest
  @MethodSource("queryConfigurations")
  void scheduledCommitWaitsForNormalization(final VersioningType versioning, final CommitStrategy strategy)
      throws Exception {
    try (final BasicXmlDBStore store = newStore(versioning, "timed");
        final SirixQueryContext context = SirixQueryContext.createWithNodeStoreAndCommitStrategy(store, strategy);
        final ExecutorService executor = Executors.newSingleThreadExecutor()) {
      final XmlDBCollection collection = store.create("data", new DocumentParser("<r>old</r>"));
      final XmlDBNode original = collection.getDocument(1);
      final XmlDBNode text = original.getFirstChild().getFirstChild();
      final long rootKey = text.getParent().getNodeKey();
      final XmlResourceSession session = original.getTrx().getResourceSession();
      final CountDownLatch changed = new CountDownLatch(1);
      final CountDownLatch finish = new CountDownLatch(1);
      final AtomicBoolean invalidCommit = new AtomicBoolean();
      try (final XmlNodeTrx writer = session.beginNodeTrx(0, 1, TimeUnit.MILLISECONDS)) {
        writer.addPreCommitHook(unused -> {
          if (!isCanonical(writer)) {
            invalidCommit.set(true);
          }
        });
        final UpdateOp operation = new PausedEmptyValue(text, changed, finish);
        final Object identity = operation.getTargetIdentity();
        final UpdateList updates = new UpdateList();
        updates.append(operation);
        context.setUpdateList(updates);
        final Future<?> application = executor.submit(context::applyUpdates);
        try {
          assertTrue(changed.await(10, TimeUnit.SECONDS));
          final ReentrantLock lock = (ReentrantLock) ((AbstractNodeTrxImpl<?, ?, ?, ?, ?>) writer).lock;
          while (!lock.hasQueuedThreads() && !invalidCommit.get()) {
            Thread.onSpinWait();
          }
        } finally {
          finish.countDown();
        }
        application.get(10, TimeUnit.SECONDS);
        if (strategy == CommitStrategy.EXPLICIT) {
          assertSame(identity, updates.list().getFirst().getTargetIdentity());
          writer.commit();
        }
      }
      assertFalse(invalidCommit.get());
      checkHistory(collection, session, rootKey);
      assertEquals("<r/>", serialize(collection.getDocument(session.getMostRecentRevisionNumber())));
      assertEquals("<r>old</r>", serialize(original));
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void nestedScopesAndFailuresRestoreCountCommitBehavior(final VersioningType versioning) {
    try (final BasicXmlDBStore store = newStore(versioning, "failure")) {
      final XmlDBCollection collection = store.create("data", new DocumentParser("<r>old</r>"));
      final XmlDBNode original = collection.getDocument(1);
      final XmlResourceSession session = original.getTrx().getResourceSession();
      final long textKey = original.getFirstChild().getFirstChild().getNodeKey();
      try (final XmlNodeTrx writer = session.beginNodeTrx(1)) {
        writer.moveTo(textKey);
        assertThrows(IllegalStateException.class, () -> writer.runAtomically(() -> {
          writer.setValue("failed");
          throw new IllegalStateException("injected batch failure");
        }));
        assertThrows(SirixUsageException.class, writer::commit);
        assertEquals(1, session.getMostRecentRevisionNumber());
        writer.rollback();
        writer.moveTo(textKey);
        writer.runAtomically(() -> {
          writer.setValue("first");
          writer.runAtomically(() -> writer.setValue("second"));
          assertThrows(SirixUsageException.class, writer::commit);
          assertEquals(1, session.getMostRecentRevisionNumber());
        });
        writer.setValue("third");
        assertEquals(2, session.getMostRecentRevisionNumber());
        writer.commit();
      }
      assertEquals("<r>second</r>", serialize(collection.getDocument(2)));
      assertEquals("<r>third</r>", serialize(collection.getDocument(3)));
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void batchWaitsForAnEarlierAsynchronousPublication(final VersioningType versioning) throws Exception {
    final CountDownLatch hardening = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final AtomicBoolean first = new AtomicBoolean(true);
    AbstractNodeTrxImpl.asyncCommitTestHook = stage -> {
      if (stage.equals("before-harden") && first.getAndSet(false)) {
        hardening.countDown();
        try {
          if (!release.await(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("hardening barrier was not released");
          }
        } catch (final InterruptedException failure) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(failure);
        }
      }
    };
    try (final BasicXmlDBStore store = newStore(versioning, "predecessor");
        final SirixQueryContext context =
            SirixQueryContext.createWithNodeStoreAndCommitStrategy(store, CommitStrategy.EXPLICIT);
        final ExecutorService executor = Executors.newSingleThreadExecutor()) {
      final XmlDBCollection collection = store.create("data", new DocumentParser("<r>old</r>"));
      final XmlDBNode original = collection.getDocument(1);
      final XmlDBNode text = original.getFirstChild().getFirstChild();
      final long rootKey = text.getParent().getNodeKey();
      final XmlResourceSession session = original.getTrx().getResourceSession();
      try (final XmlNodeTrx writer = session.beginNodeTrx(1, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT)) {
        writer.moveTo(text.getNodeKey());
        writer.setValue("before");
        writer.setValue("next");
        final CountDownLatch started = new CountDownLatch(1);
        final AtomicReference<Thread> worker = new AtomicReference<>();
        final AtomicBoolean applied = new AtomicBoolean();
        final UpdateList updates = new UpdateList();
        updates.append(new ObservedEmptyValue(text, applied));
        context.setUpdateList(updates);
        final Future<?> application = executor.submit(() -> {
          worker.set(Thread.currentThread());
          started.countDown();
          context.applyUpdates();
        });
        try {
          assertTrue(hardening.await(10, TimeUnit.SECONDS));
          assertTrue(started.await(10, TimeUnit.SECONDS));
          while (worker.get().getState() != Thread.State.WAITING && !application.isDone()) {
            Thread.onSpinWait();
          }
          assertFalse(applied.get(), "the pending predecessor must be published before the batch starts");
        } finally {
          release.countDown();
        }
        application.get(10, TimeUnit.SECONDS);
        writer.commit();
      }
      checkHistory(collection, session, rootKey);
      assertEquals("<r>old</r>", serialize(collection.getDocument(1)));
      assertEquals("<r>before</r>", serialize(collection.getDocument(2)));
      assertEquals("<r/>", serialize(collection.getDocument(session.getMostRecentRevisionNumber())));
    } finally {
      release.countDown();
    }
  }

  @ParameterizedTest
  @MethodSource("queryConfigurations")
  void allSuppliedWritersShareTheBatchBoundary(final VersioningType versioning, final CommitStrategy strategy) {
    try (final BasicXmlDBStore store = newStore(versioning, "writers");
        final SirixQueryContext context = SirixQueryContext.createWithNodeStoreAndCommitStrategy(store, strategy)) {
      final XmlDBCollection first = store.create("first", new DocumentParser("<r><a/><b/></r>"));
      final XmlDBCollection second = store.create("second", new DocumentParser("<r><a/><b/></r>"));
      final XmlDBNode firstDocument = first.getDocument(1);
      final XmlDBNode secondDocument = second.getDocument(1);
      final XmlDBNode firstRoot = firstDocument.getFirstChild();
      final XmlDBNode secondRoot = secondDocument.getFirstChild();
      final XmlResourceSession firstSession = firstDocument.getTrx().getResourceSession();
      final XmlResourceSession secondSession = secondDocument.getTrx().getResourceSession();
      try (final XmlNodeTrx firstWriter = firstSession.beginNodeTrx(1);
          final XmlNodeTrx secondWriter = secondSession.beginNodeTrx(1)) {
        final UpdateList updates = new UpdateList();
        final UpdateOp firstUpdate = new ReplaceElementContentOp(firstRoot, new Una("first"));
        final UpdateOp secondUpdate = new ReplaceElementContentOp(secondRoot, new Una("second"));
        updates.append(firstUpdate);
        updates.append(secondUpdate);
        context.setUpdateList(updates);
        context.applyUpdates();
        if (strategy == CommitStrategy.EXPLICIT) {
          assertSame(firstUpdate.getTargetIdentity(), updates.list().get(0).getTargetIdentity());
          assertSame(secondUpdate.getTargetIdentity(), updates.list().get(1).getTargetIdentity());
          assertEquals(1, firstSession.getMostRecentRevisionNumber());
          assertEquals(1, secondSession.getMostRecentRevisionNumber());
          firstWriter.commit();
          secondWriter.commit();
        } else {
          assertTrue(firstWriter.isClosed());
          assertTrue(secondWriter.isClosed());
        }
      }
      checkHistory(first, firstSession, firstRoot.getNodeKey());
      checkHistory(second, secondSession, secondRoot.getNodeKey());
      assertEquals("<r>first</r>", serialize(first.getDocument(2)));
      assertEquals("<r>second</r>", serialize(second.getDocument(2)));
      assertEquals("<r><a/><b/></r>", serialize(firstDocument));
      assertEquals("<r><a/><b/></r>", serialize(secondDocument));
    }
  }

  @ParameterizedTest
  @MethodSource("queryConfigurations")
  void failedBatchDoesNotDisableScheduledCommitsAfterRollback(final VersioningType versioning,
      final CommitStrategy strategy) throws Exception {
    try (final BasicXmlDBStore store = newStore(versioning, "timed-failure");
        final SirixQueryContext context = SirixQueryContext.createWithNodeStoreAndCommitStrategy(store, strategy)) {
      final XmlDBCollection collection = store.create("data", new DocumentParser("<r>old</r>"));
      final XmlDBNode original = collection.getDocument(1);
      final XmlDBNode text = original.getFirstChild().getFirstChild();
      final XmlResourceSession session = original.getTrx().getResourceSession();
      final CountDownLatch recovered = new CountDownLatch(1);
      final AtomicBoolean recoveredValue = new AtomicBoolean();
      try (final XmlNodeTrx writer = session.beginNodeTrx(0, 1, TimeUnit.MILLISECONDS)) {
        writer.addPreCommitHook(unused -> {
          final long saved = writer.getNodeKey();
          if (writer.moveTo(text.getNodeKey()) && writer.getValue().equals("recovered")) {
            recoveredValue.set(true);
          }
          writer.moveTo(saved);
        });
        writer.addPostCommitHook(unused -> {
          if (recoveredValue.get()) {
            recovered.countDown();
          }
        });
        for (final boolean error : new boolean[] {false, true}) {
          final UpdateList updates = new UpdateList();
          updates.append(new FailingEmptyValue(text, error));
          context.setUpdateList(updates);
          if (error) {
            assertThrows(AssertionError.class, context::applyUpdates);
          } else {
            assertThrows(IllegalStateException.class, context::applyUpdates);
          }
          assertThrows(SirixUsageException.class, writer::commit);
          writer.rollback();
        }
        writer.runLocked(() -> {
          writer.moveTo(text.getNodeKey());
          writer.setValue("recovered");
        });
        assertTrue(recovered.await(10, TimeUnit.SECONDS));
        writer.commit();
      }
      checkHistory(collection, session, original.getFirstChild().getNodeKey());
      assertEquals("<r>old</r>", serialize(original));
    }
  }

  private record Scenario(String name, String xml, String update, String expected) {
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void concurrentQueryKeepsCachedSnapshotWhileUpdateIsPaused(final VersioningType versioning) throws Exception {
    try (final BasicXmlDBStore store = newStore(versioning, "concurrent");
        final SirixQueryContext context = SirixQueryContext.createWithNodeStoreAndCommitStrategy(store,
            CommitStrategy.EXPLICIT);
        final SirixQueryContext readContext = SirixQueryContext.createWithNodeStore(store);
        final ExecutorService executor = Executors.newSingleThreadExecutor()) {
      final XmlDBCollection collection = store.create("data", new DocumentParser("<r xmlns:p='urn:old'>old</r>"));
      final XmlDBNode cached = collection.getDocument(1);
      final XmlNodeReadOnlyTrx originalReader = cached.getTrx();
      final XmlResourceSession session = originalReader.getResourceSession();
      final CountDownLatch changed = new CountDownLatch(1);
      final CountDownLatch finish = new CountDownLatch(1);
      try (final XmlNodeTrx writer = session.beginNodeTrx(1)) {
        context.addPendingUpdate(new UpdateOp() {
          @Override
          public XmlDBNode getTarget() {
            return cached;
          }

          @Override
          public OpType getType() {
            return OpType.REPLACE_ELEMENT_CONTENT;
          }

          @Override
          public void apply() {
            final XmlDBNode root = cached.getFirstChild();
            new ReplaceElementContentOp(root, new Una("uncommitted")).apply();
            root.getScope().addPrefix("q", "urn:uncommitted");
            changed.countDown();
            try {
              if (!finish.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Reader did not release update");
              }
            } catch (final InterruptedException failure) {
              Thread.currentThread().interrupt();
              throw new IllegalStateException(failure);
            }
          }
        });
        final Future<?> application = executor.submit(context::applyUpdates);
        final XmlDBNode retained;
        try {
          assertTrue(changed.await(10, TimeUnit.SECONDS));
          assertSame(cached, collection.getDocument(1));
          readContext.setContextItem(collection.getDocument(1));
          retained = (XmlDBNode) new Query(new CompileChain(), "$$").execute(readContext);
          assertSame(originalReader, retained.getTrx());
          assertEquals("old", new Query(new CompileChain(), "string($$)").execute(readContext).toString());
          assertEquals("urn:old", retained.getFirstChild().getScope().resolvePrefix("p"));
          assertEquals(null, retained.getFirstChild().getScope().resolvePrefix("q"));
          assertEquals(1, session.getMostRecentRevisionNumber());
        } finally {
          finish.countDown();
        }
        application.get(10, TimeUnit.SECONDS);
        assertSame(originalReader, retained.getTrx());
        assertEquals("old", retained.getFirstChild().getValue().stringValue());
        writer.commit();
        assertEquals("old", retained.getFirstChild().getValue().stringValue());
        assertEquals("uncommitted", collection.getDocument(2).getFirstChild().getValue().stringValue());
      }
    }
  }

  private record ObservedEmptyValue(XmlDBNode target, AtomicBoolean applied) implements UpdateOp {
    @Override
    public XmlDBNode getTarget() {
      return target;
    }

    @Override
    public OpType getType() {
      return OpType.REPLACE_VALUE;
    }

    @Override
    public void apply() {
      applied.set(true);
      target.setValue(new Una(""));
    }
  }

  private record FailingEmptyValue(XmlDBNode target, boolean error) implements UpdateOp {
    @Override
    public XmlDBNode getTarget() {
      return target;
    }

    @Override
    public OpType getType() {
      return OpType.REPLACE_VALUE;
    }

    @Override
    public void apply() {
      target.setValue(new Una(""));
      if (error) {
        throw new AssertionError("injected update failure");
      }
      throw new IllegalStateException("injected update failure");
    }
  }

  private record PausedEmptyValue(XmlDBNode target, CountDownLatch changed, CountDownLatch finish) implements UpdateOp {
    @Override
    public XmlDBNode getTarget() {
      return target;
    }

    @Override
    public OpType getType() {
      return OpType.REPLACE_VALUE;
    }

    @Override
    public void apply() {
      target.setValue(new Una(""));
      changed.countDown();
      try {
        if (!finish.await(10, TimeUnit.SECONDS)) {
          throw new IllegalStateException("batch barrier was not released");
        }
      } catch (final InterruptedException failure) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(failure);
      }
    }
  }

  private static boolean isCanonical(final XmlNodeReadOnlyTrx reader) {
    final long saved = reader.getNodeKey();
    try {
      reader.moveToDocumentRoot();
      for (final Axis axis = new DescendantAxis(reader); axis.hasNext();) {
        final long key = axis.nextLong();
        if (reader.getKind() == NodeKind.TEXT) {
          if (reader.getValue().isEmpty()) {
            return false;
          }
          if (reader.moveToLeftSibling() && reader.getKind() == NodeKind.TEXT) {
            return false;
          }
          reader.moveTo(key);
        }
      }
      return true;
    } finally {
      reader.moveTo(saved);
    }
  }

  private static void checkHistory(final XmlDBCollection collection, final XmlResourceSession session,
      final long rootKey) {
    final List<Integer> invalid = new ArrayList<>();
    for (int revision = 1, last = session.getMostRecentRevisionNumber(); revision <= last; revision++) {
      try (final XmlNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(revision)) {
        if (!isCanonical(reader)) {
          invalid.add(revision);
        }
      }
      assertEquals(rootKey, collection.getDocument(revision).getFirstChild().getNodeKey());
    }
    assertEquals(List.of(), invalid, "historical revisions containing invalid text nodes");
  }

  private BasicXmlDBStore newStore(final VersioningType versioning, final String name) {
    return BasicXmlDBStore.newBuilder()
                          .location(directory.resolve(name))
                          .versioningType(versioning)
                          .storageType(StorageType.FILE_CHANNEL)
                          .build();
  }

  private static String serialize(final XmlDBNode document) {
    final BrackitQueryContext context = new BrackitQueryContext();
    context.setContextItem(document);
    final StringWriter output = new StringWriter();
    new Query(new CompileChain(), "$$").serialize(context, new PrintWriter(output));
    return output.toString();
  }

  private static List<Arguments> queryConfigurations() {
    final List<Arguments> configurations = new ArrayList<>(8);
    for (final VersioningType versioning : VersioningType.values()) {
      for (final CommitStrategy strategy : CommitStrategy.values()) {
        configurations.add(Arguments.of(versioning, strategy));
      }
    }
    return configurations;
  }

  private static List<Arguments> countConfigurations() {
    final List<Arguments> configurations = new ArrayList<>(120);
    for (final VersioningType versioning : VersioningType.values()) {
      for (final CommitStrategy strategy : CommitStrategy.values()) {
        for (final AfterCommitState state : new AfterCommitState[] {AfterCommitState.KEEP_OPEN,
            AfterCommitState.KEEP_OPEN_ASYNC_COMMIT, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH}) {
          for (final Scenario scenario : SCENARIOS) {
            configurations.add(Arguments.of(versioning, strategy, state, scenario));
          }
        }
      }
    }
    return configurations;
  }
}
