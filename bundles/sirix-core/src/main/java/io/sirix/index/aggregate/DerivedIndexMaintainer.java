package io.sirix.index.aggregate;

import io.sirix.access.trx.node.InternalResourceSession;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.Indexes;
import io.sirix.cache.PageContainer;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.index.changelog.ChangeLogSlab;
import io.sirix.index.changelog.ChangeLogStorage;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntConsumer;

/** One database-owned worker. Source publication does nothing but unpark this thread. */
public final class DerivedIndexMaintainer implements AutoCloseable {
  public static final String SUFFIX = "$derived";
  private static final Logger LOGGER = LoggerFactory.getLogger(DerivedIndexMaintainer.class);
  private static final DerivedIndex AGGREGATE = new TemporalAggregateDerivedIndex();
  private final ConcurrentHashMap<Path, Resource> resources = new ConcurrentHashMap<>();
  private final Thread worker;
  private volatile boolean stopped;
  private volatile boolean paused;

  public DerivedIndexMaintainer(final String databaseName) {
    worker = Thread.ofPlatform().daemon().name("sirix-derived-" + databaseName).unstarted(this::run);
    worker.start();
  }

  public static boolean isDerivedResourceName(final String name) {
    return name.regionMatches(true, name.length() - SUFFIX.length(), SUFFIX, 0, SUFFIX.length());
  }

  public static boolean enabled(final ResourceConfiguration configuration) {
    return !configuration.aggregateKernel();
  }

  /** Validate the restart marker before a database installs an existing derived resource. */
  public static void validateSnapshot(final JsonResourceSession derived) {
    try (StorageEngineReader reader = derived.createStorageEngineReader()) {
      final int frontier = DerivedRevisionMapping.frontier(reader);
      if (frontier > 0 && DerivedRevisionMapping.read(reader, frontier) == null) {
        throw new IllegalStateException("Derived frontier has no durable mapping");
      }
    }
  }

  public synchronized Resource register(final JsonResourceSession source, final JsonResourceSession derived) {
    Objects.requireNonNull(source);
    Objects.requireNonNull(derived);
    if (stopped) {
      throw new IllegalStateException("Derived maintainer is closed");
    }
    try {
      if (!Files.isSameFile(source.getResourcePath().resolveSibling(source.getResourcePath().getFileName() + SUFFIX),
          derived.getResourcePath())) {
        throw new IllegalArgumentException("Derived resource must be the source's hidden sibling");
      }
    } catch (final IOException exception) {
      throw new IllegalArgumentException("Could not resolve derived resource identity", exception);
    }
    final Resource resource = new Resource(source, derived);
    if (resources.putIfAbsent(source.getResourcePath(), resource) != null) {
      throw new IllegalStateException("Source is already registered with the derived maintainer");
    }
    ((InternalResourceSession<?, ?>) source).addRevisionListener(resource);
    LockSupport.unpark(worker);
    return resource;
  }

  public void remove(final Path source) {
    final Resource resource = resources.remove(source);
    if (resource != null) {
      resource.close();
    }
  }

  /** Deterministic control for embedded operators and tests; publication continues while paused. */
  public void pause() {
    paused = true;
    for (final Resource resource : resources.values()) {
      resource.historyLock.lock();
      try {
        // Acquiring the lock waits for maintenance already in progress to finish.
      } finally {
        resource.historyLock.unlock();
      }
    }
  }

  public void resume() {
    paused = false;
    LockSupport.unpark(worker);
  }

  private void run() {
    while (!stopped) {
      boolean progressed = false;
      if (!paused) {
        for (final Resource resource : resources.values()) {
          if (stopped || paused) {
            break;
          }
          progressed |= resource.advance(false);
        }
      }
      if (!progressed) {
        LockSupport.parkNanos(this, Duration.ofSeconds(1).toNanos());
      }
    }
  }

  @Override
  public synchronized void close() {
    stopped = true;
    LockSupport.unpark(worker);
    try {
      worker.join(Duration.ofSeconds(30));
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted stopping derived maintainer", e);
    }
    if (worker.isAlive()) {
      throw new IllegalStateException("Derived maintainer did not stop; sessions remain open");
    }
    for (final Resource resource : resources.values()) {
      resource.close();
    }
    resources.clear();
  }

  /** Source-specific availability and failure state, shared by all source user sessions. */
  public final class Resource implements AutoCloseable, IntConsumer {
    private final JsonResourceSession source;
    private final JsonResourceSession derived;
    private final ConcurrentSkipListMap<Integer, DerivedRevisionMapping> mappings = new ConcurrentSkipListMap<>();
    private final ReentrantLock historyLock = new ReentrantLock();
    private @Nullable JsonNodeTrx writer;
    private volatile int maintained;
    private volatile int poisonedRevision = -1;
    private volatile @Nullable Throwable failure;
    private volatile boolean closed;
    private boolean initialized;
    private volatile long documentPagesWritten;

    public long documentPagesWritten() {
      return documentPagesWritten;
    }

    private int writerSourceRevision;
    private int processingRevision = 1;
    private int lastFoldRevision;
    private boolean truncatePending;
    private boolean foldRequired;
    private final Int2ObjectOpenHashMap<TreeSet<byte[]>> pendingFolds = new Int2ObjectOpenHashMap<>();

    private Resource(final JsonResourceSession source, final JsonResourceSession derived) {
      this.source = Objects.requireNonNull(source);
      this.derived = Objects.requireNonNull(derived);
    }

    @Override
    public void accept(final int revision) {
      LockSupport.unpark(worker);
    }

    public int maintainedRevision() {
      return maintained;
    }

    public int lag() {
      return Math.max(0, source.getMostRecentRevisionNumber() - maintained);
    }

    public int poisonedRevision() {
      return poisonedRevision;
    }

    public @Nullable Throwable failure() {
      return failure;
    }

    public void pause() {
      DerivedIndexMaintainer.this.pause();
    }

    public void resume() {
      DerivedIndexMaintainer.this.resume();
    }

    /** Used only by truncation, never by normal source commits. */
    public void lockHistory() {
      historyLock.lock();
    }

    public void unlockHistory() {
      historyLock.unlock();
    }

    public void truncate(final int revision) {
      if (!historyLock.isHeldByCurrentThread()) {
        throw new IllegalStateException("Truncation must hold the derived history lock");
      }
      mappings.tailMap(revision, false).clear();
      maintained = Math.min(maintained, revision);
      truncatePending = true;
      failure = null;
      poisonedRevision = -1;
    }

    public @Nullable StorageEngineReader openReader(final StorageEngineReader sourceReader) {
      if (closed || poisonedRevision >= 0 || sourceReader.getRevisionNumber() < 1) {
        return null;
      }
      try {
        DerivedRevisionMapping mapping = mappings.get(sourceReader.getRevisionNumber());
        if (mapping != null) {
          if (mapping.projection() < 0 || !mapping.matches(sourceReader)) {
            return null;
          }
        } else {
          final boolean locked = historyLock.tryLock();
          try (StorageEngineReader latest = derived.createStorageEngineReader()) {
            final List<DerivedRevisionMapping> window =
                DerivedRevisionMapping.readWindow(latest, sourceReader.getRevisionNumber());
            for (final DerivedRevisionMapping adjacent : window) {
              if (adjacent.sourceRevision() == sourceReader.getRevisionNumber()) {
                mapping = adjacent;
                break;
              }
            }
            if (mapping == null || mapping.projection() < 0 || !mapping.matches(sourceReader)) {
              return null;
            }
            if (locked && initialized && !truncatePending) {
              try (StorageEngineReader current = source.createStorageEngineReader()) {
                for (final DerivedRevisionMapping adjacent : window) {
                  if (adjacent.sourceRevision() <= maintained && adjacent.matchesIdentity(current)) {
                    mappings.putIfAbsent(adjacent.sourceRevision(), adjacent);
                  }
                }
              }
              while (mappings.size() > DerivedRevisionMapping.CACHE_CAPACITY) {
                mappings.pollFirstEntry();
              }
            }
          } finally {
            if (locked) {
              historyLock.unlock();
            }
          }
        }
        return derived.createStorageEngineReader(mapping.indexRevision());
      } catch (final RuntimeException unavailable) {
        LOGGER.warn("Derived revision unavailable for {} at {}", source.getResourcePath(),
            sourceReader.getRevisionNumber(), unavailable);
        return null;
      }
    }

    /** Drain a paused resource on the calling thread, for deterministic embedded maintenance. */
    public void drain() {
      if (!paused) {
        throw new IllegalStateException("Pause the maintainer before draining a resource");
      }
      while (maintainNextBatch()) {
        // Each bounded batch releases its reader before taking the next published range.
      }
      if (failure != null) {
        throw new IllegalStateException("Derived maintenance failed", failure);
      }
    }

    /** Executes one bounded catch-up batch, or one idle fold, while the daemon is paused. */
    public boolean maintainNextBatch() {
      if (!paused) {
        throw new IllegalStateException("Pause the maintainer before explicit maintenance");
      }
      return advance(true);
    }

    @SuppressWarnings("NonAtomicVolatileUpdate") // All frontier writers hold historyLock.
    private boolean advance(final boolean manual) {
      historyLock.lock();
      try {
        if (closed || (paused && !manual) || stopped || poisonedRevision >= 0) {
          return false;
        }
        if (!initialized) {
          final int latest = source.getMostRecentRevisionNumber();
          try (StorageEngineReader reader = derived.createStorageEngineReader()) {
            int candidate = Math.min(DerivedRevisionMapping.frontier(reader), latest);
            while (candidate > 0) {
              final DerivedRevisionMapping mapping = DerivedRevisionMapping.read(reader, candidate);
              if (mapping != null) {
                try (StorageEngineReader sourceReader = source.createStorageEngineReader(candidate)) {
                  if (mapping.matches(sourceReader)) {
                    mappings.put(candidate, mapping);
                    maintained = candidate;
                    break;
                  }
                }
              }
              candidate--;
            }
          }
          writerSourceRevision = -1;
          truncatePending = true;
          initialized = true;
        }
        if (truncatePending) {
          DerivedRevisionMapping frontier = mapping(maintained);
          while (maintained > 0 && frontier == null) {
            frontier = mapping(--maintained);
          }
          mappings.tailMap(maintained, false).clear();
          final int indexRevision = frontier == null
              ? 0
              : frontier.indexRevision();
          if (derived.getMostRecentRevisionNumber() > indexRevision) {
            if (writer == null) {
              writer = derived.beginNodeTrx();
            }
            writer.truncateTo(indexRevision);
            writer.close();
            writer = null;
          }
          pendingFolds.clear();
          foldRequired = false;
          writerSourceRevision = maintained;
          truncatePending = false;
        }
        if (maintained >= source.getMostRecentRevisionNumber()) {
          return fold();
        }
        // Preserve the existing bounded point representation even under continuous publications.
        // A fold is its own commit, never part of a catch-up transaction.
        if (!pendingFolds.isEmpty() && (foldRequired || maintained - lastFoldRevision >= 64)) {
          fold();
        }
        if (writer == null) {
          writer = derived.beginNodeTrx();
        }
        final int first = maintained + 1;
        final int latest = source.getMostRecentRevisionNumber();
        int entries = 0;
        int revision = first;
        int projection = -1;
        for (int count = 0; count < 8 && entries < 100_000 && !stopped && (!paused || manual)
            && revision <= latest; count++, revision++) {
          final AppliedRevision applied = applyRevision(revision);
          entries += applied.entries();
          projection = applied.projection();
          if (applied.catalogueChanged()) {
            revision++;
            break;
          }
        }
        if (revision == first) {
          return false;
        }
        final int last = revision - 1;
        final JsonNodeTrx transaction = Objects.requireNonNull(writer);
        try (JsonNodeReadOnlyTrx reader = source.beginNodeReadOnlyTrx(last)) {
          final DerivedRevisionMapping mapping =
              DerivedRevisionMapping.of(reader.getStorageEngineReader(), transaction.getRevisionNumber(), projection);
          mapping.write(transaction.getStorageEngineWriter());
          recordDocumentPages(transaction.getStorageEngineWriter());
          transaction.commit(first == last
              ? "src=" + last
              : "src=" + first + ".." + last, reader.getRevisionTimestamp());
          mappings.put(last, mapping);
          while (mappings.size() > DerivedRevisionMapping.CACHE_CAPACITY) {
            mappings.pollFirstEntry();
          }
          maintained = last;
        }
        // Reserve room for an interior-base replay and another batch after a crash. This is
        // a separate durable transaction, after the catch-up commit has completed.
        if (foldRequired) {
          fold();
        }
        return true;
      } catch (final RuntimeException | Error exception) {
        poisonedRevision = processingRevision;
        failure = exception;
        if (writer != null) {
          try {
            writer.rollback();
            writer.close();
          } catch (final RuntimeException cleanup) {
            exception.addSuppressed(cleanup);
          }
          writer = null;
        }
        LOGGER.error("Derived maintenance poisoned for {} at source revision {}", source.getResourcePath(),
            poisonedRevision, exception);
        return false;
      } finally {
        historyLock.unlock();
      }
    }

    private @Nullable DerivedRevisionMapping mapping(final int revision) {
      final DerivedRevisionMapping cached = mappings.get(revision);
      if (cached != null || revision == 0) {
        return cached;
      }
      try (StorageEngineReader reader = derived.createStorageEngineReader()) {
        final DerivedRevisionMapping mapping = DerivedRevisionMapping.read(reader, revision);
        if (mapping == null) {
          return null;
        }
        try (StorageEngineReader current = source.createStorageEngineReader(revision)) {
          return mapping.matches(current)
              ? mapping
              : null;
        }
      }
    }

    private record AppliedRevision(int entries, int projection, boolean catalogueChanged) {
    }

    private AppliedRevision applyRevision(final int revision) {
      processingRevision = revision;
      final JsonNodeTrx transaction = Objects.requireNonNull(writer);
      try (JsonNodeReadOnlyTrx reader = source.beginNodeReadOnlyTrx(revision)) {
        final Indexes sourceIndexes = source.getRtxIndexController(revision).getIndexes();
        final List<IndexDef> definitions = new ArrayList<>();
        final Int2ObjectOpenHashMap<ChangeLogSlab> slabs = new Int2ObjectOpenHashMap<>();
        int base = revision - 1;
        int entries = 0;
        int identityProjection = -1;
        for (final IndexDef definition : sourceIndexes.getIndexDefs()) {
          if (definition.isTemporalAggregateIndex()) {
            definitions.add(definition);
            final int projection = definition.getTemporalAggregateSpec().projectionId();
            if (!slabs.containsKey(projection)) {
              final ChangeLogSlab slab = ChangeLogStorage.read(reader.getStorageEngineReader(), projection, revision);
              if (!slabs.isEmpty() && base != slab.baseRevision()) {
                throw new IllegalStateException("Projection logs disagree about source base revision");
              }
              base = slab.baseRevision();
              entries += slab.entries().size();
              slabs.put(projection, slab);
            }
            identityProjection = identityProjection < 0
                ? projection
                : Math.min(identityProjection, projection);
          }
        }
        if (writerSourceRevision != base) {
          pendingFolds.clear();
          foldRequired = false;
          lastFoldRevision = base;
          int mappedBase = base;
          DerivedRevisionMapping mapping = mapping(mappedBase);
          while (mappedBase > 0 && mapping == null) {
            mapping = mapping(--mappedBase);
          }
          transaction.revertTo(mapping == null
              ? 0
              : mapping.indexRevision());
          writerSourceRevision = mappedBase;
          // Interior revisions of a batch have no standalone snapshot. Replay their durable
          // slabs only on this rare revert path; query-time tail catch-up is separate work.
          for (int replay = mappedBase + 1; replay <= base; replay++) {
            applyRevision(replay);
          }
          // Reverting a snapshot also reverts its mapping tree. Preserve published branches.
          try (StorageEngineReader latest = derived.createStorageEngineReader()) {
            for (final DerivedRevisionMapping existing : DerivedRevisionMapping.readAll(latest)) {
              existing.write(transaction.getStorageEngineWriter());
            }
          }
        }
        processingRevision = revision;
        final StorageEngineWriter destination = transaction.getStorageEngineWriter();
        final Indexes target = derived.getWtxIndexController(transaction.getRevisionNumber()).getIndexes();
        boolean catalogueChanged = false;
        for (final IndexDef previous : target.getIndexDefs()) {
          if (definitions.stream().noneMatch(previous::hasSameDefinition)) {
            target.removeIndex(previous);
            pendingFolds.remove(previous.getID());
            if (pendingFolds.isEmpty()) {
              foldRequired = false;
            }
            catalogueChanged = true;
          }
        }
        for (final IndexDef definition : definitions) {
          final IndexDef projection = Objects.requireNonNull(
              sourceIndexes.getIndexDef(definition.getTemporalAggregateSpec().projectionId(), IndexType.PROJECTION));
          final IndexDef previous = target.getIndexDef(definition.getID(), IndexType.TEMPORAL_AGGREGATE);
          if (previous == null) {
            catalogueChanged = true;
            AGGREGATE.build(destination, reader, definition, sourceIndexes);
            target.add(IndexDefs.createTemporalAggregateIdxDef(projection.getProjectionRootPath(),
                definition.getTemporalAggregateSpec(), definition.getID()));
          } else {
            AGGREGATE.applySlab(destination, definition, sourceIndexes, source.getResourceConfig(),
                slabs.get(projection.getID()), (key, partCount) -> {
                  pendingFolds.computeIfAbsent(definition.getID(), ignored -> new TreeSet<>(Arrays::compareUnsigned))
                              .add(key);
                  foldRequired |= partCount > TemporalAggregateParts.MAX_PARTS - 24;
                });
          }
        }
        writerSourceRevision = revision;
        return new AppliedRevision(entries, identityProjection, catalogueChanged);
      }
    }

    @SuppressWarnings("NonAtomicVolatileUpdate") // Called only by maintenance holding historyLock.
    private void recordDocumentPages(final StorageEngineWriter destination) {
      if (TemporalAggregateStorage.workDiagnosticsEnabled()) {
        long pages = 0;
        for (final PageContainer container : destination.getLog().getList()) {
          if (container.getModified() instanceof KeyValueLeafPage page && page.getIndexType() == IndexType.DOCUMENT) {
            pages++;
          }
        }
        documentPagesWritten += pages;
      }
    }

    private boolean fold() {
      if (pendingFolds.isEmpty() || writer == null) {
        return false;
      }
      processingRevision = maintained;
      final JsonNodeTrx transaction = writer;
      final Indexes indexes = derived.getWtxIndexController(transaction.getRevisionNumber()).getIndexes();
      for (final var entry : pendingFolds.int2ObjectEntrySet()) {
        final IndexDef definition = indexes.getIndexDef(entry.getIntKey(), IndexType.TEMPORAL_AGGREGATE);
        if (definition != null) {
          AGGREGATE.fold(transaction.getStorageEngineWriter(), definition, entry.getValue());
        }
      }
      try (JsonNodeReadOnlyTrx sourceReader = source.beginNodeReadOnlyTrx(maintained)) {
        final DerivedRevisionMapping previous = Objects.requireNonNull(mapping(maintained));
        final DerivedRevisionMapping mapping = DerivedRevisionMapping.of(sourceReader.getStorageEngineReader(),
            transaction.getRevisionNumber(), previous.projection());
        mapping.write(transaction.getStorageEngineWriter());
        recordDocumentPages(transaction.getStorageEngineWriter());
        transaction.commit("fold src=" + maintained, sourceReader.getRevisionTimestamp());
        mappings.put(maintained, mapping);
        while (mappings.size() > DerivedRevisionMapping.CACHE_CAPACITY) {
          mappings.pollFirstEntry();
        }
      }
      pendingFolds.clear();
      foldRequired = false;
      lastFoldRevision = maintained;
      return true;
    }

    @Override
    public void close() {
      historyLock.lock();
      try {
        closed = true;
        ((InternalResourceSession<?, ?>) source).removeRevisionListener(this);
        if (writer != null) {
          writer.rollback();
          writer.close();
          writer = null;
        }
      } finally {
        historyLock.unlock();
      }
    }
  }
}
