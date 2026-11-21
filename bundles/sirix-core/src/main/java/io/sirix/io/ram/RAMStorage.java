package io.sirix.io.ram;

import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.ScratchLease;
import io.sirix.cache.ScratchUse;
import io.sirix.exception.SirixIOException;
import io.sirix.io.AbstractReader;
import io.sirix.io.IOStorage;
import io.sirix.io.PageHasher;
import io.sirix.io.Reader;
import io.sirix.io.RevisionFileData;
import io.sirix.io.RevisionIndex;
import io.sirix.io.RevisionIndexHolder;
import io.sirix.io.Writer;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.node.BytesOut;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PagePersister;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.SerializationType;
import io.sirix.page.UberPage;
import io.sirix.page.interfaces.Page;
import org.jspecify.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Objects.requireNonNull;

/**
 * In-memory storage: the committed pages of a resource kept in the JVM heap instead of a file.
 *
 * <p>
 * Contract, identical to the file-backed storages except for durability across processes:
 * </p>
 * <ul>
 * <li>A page is <em>serialized</em> when it is written: the stored bytes are exactly the payload a
 * file-backed writer would append (page encoding, byte-handler pipeline, page hash on the
 * reference). The writer keeps its page instance and may close or reuse it afterwards; nothing it
 * does to that instance reaches the store.</li>
 * <li>Every read decodes a fresh page that the caller owns, so {@link Reader#returnsSharedPages()}
 * is {@code false}, as for every other backend. A reader therefore never observes a page that a
 * writer or another reader closed, and a later commit can never change what an earlier revision
 * reads.</li>
 * <li>The committed state belongs to the resource inside its open database instance, not to one
 * storage instance: every {@link RAMStorage} opened on the same resource (same path and resource
 * UUID) shares it, so every later writer, reader and resource session of that instance sees every
 * committed revision. The database's final close and the resource's removal drop it through
 * {@link #discardUnder(Path)} (TXN-615).</li>
 * <li>Rolling back to an earlier revision is not supported ({@link Writer#supportsTruncateTo()} is
 * {@code false}).</li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 */
public final class RAMStorage implements IOStorage {

  /**
   * The committed state of every in-memory resource of this process, by canonical resource path.
   */
  private static final ConcurrentMap<Path, Store> STORES = new ConcurrentHashMap<>();

  /** The committed state of this storage's resource. */
  private final Store store;

  /** The byte handler pipeline pages are encoded with. */
  private final ByteHandlerPipeline byteHandler;

  /**
   * Constructor.
   *
   * @param resourceConfig {@link ResourceConfiguration} reference
   */
  public RAMStorage(final ResourceConfiguration resourceConfig) {
    byteHandler = requireNonNull(resourceConfig).byteHandlePipeline;
    store = storeOf(resourceConfig);
  }

  /**
   * The shared state of a resource. A store left behind by a resource that was deleted without
   * {@link #discardUnder(Path)} is replaced once a resource with a different identity opens at its
   * path, so a recreated resource never sees the revisions of its predecessor.
   */
  private static Store storeOf(final ResourceConfiguration resourceConfig) {
    final Path path = canonical(requireNonNull(resourceConfig.getResource(), "resource path"));
    final UUID resourceUuid = resourceConfig.resourceUuid;
    return STORES.compute(path, (_, existing) -> existing != null && Objects.equals(existing.resourceUuid, resourceUuid)
        ? existing
        : new Store(resourceUuid));
  }

  /**
   * Drop the committed state of every in-memory resource at or below {@code directory}: called when a
   * resource or a whole database is removed.
   *
   * @param directory a resource or database directory
   */
  public static void discardUnder(final Path directory) {
    final Path canonical = canonical(requireNonNull(directory));
    for (final var entry : STORES.entrySet()) {
      final Store store = entry.getValue();
      if (entry.getKey().startsWith(canonical) && STORES.remove(entry.getKey(), store)) {
        store.clear();
      }
    }
  }

  private static Path canonical(final Path path) {
    return path.toAbsolutePath().normalize();
  }

  @Override
  public Writer createWriter() {
    return new RAMAccess(store, byteHandler);
  }

  @Override
  public Reader createReader() {
    return new RAMAccess(store, byteHandler);
  }

  @Override
  public void close() {
    // The committed state belongs to the open database instance and outlives this storage instance.
  }

  @Override
  public ByteHandlerPipeline getByteHandler() {
    return byteHandler;
  }

  @Override
  public RevisionIndexHolder getRevisionIndexHolder() {
    return store.revisionIndexHolder;
  }

  @Override
  public boolean exists() {
    return store.uberBeacon != null;
  }

  /**
   * One committed revision's root page: where its payload is stored, when it was committed and the
   * hash of its payload.
   */
  private record RevisionRecord(long key, long timestampMillis, long pageHash) {
  }

  /**
   * The committed state of one resource.
   *
   * <p>
   * Page keys are dense and increase monotonically, so payloads sit in a growable array indexed by
   * slot (key minus {@link IOStorage#DATA_REGION_START}). There is a single writer per resource (the
   * resource's write lock); readers run concurrently. A slot is written once with release semantics
   * before the key can be published to any reader (the commit publishes the uber page only
   * afterwards), and the arrays are replaced, never shrunk in place, so a reader holding an older
   * array still finds every slot it may ask for.
   * </p>
   */
  private static final class Store {
    private static final VarHandle PAYLOADS = MethodHandles.arrayElementVarHandle(byte[][].class);

    private static final VarHandle RECORDS = MethodHandles.arrayElementVarHandle(RevisionRecord[].class);

    /** Initial number of page slots. */
    private static final int INITIAL_PAGES = 64;

    /** Initial number of revision slots. */
    private static final int INITIAL_REVISIONS = 16;

    final @Nullable UUID resourceUuid;

    final RevisionIndexHolder revisionIndexHolder = new RevisionIndexHolder();

    /**
     * Page payloads by slot. A page's key is {@link IOStorage#DATA_REGION_START} plus its slot, as a
     * file offset would be: keys never collide with the uber-page beacon offsets, which the storage
     * engine reads through {@link Reader#read} like any other page.
     */
    private volatile byte[][] payloads = new byte[INITIAL_PAGES][];

    /** The slot of the next page; guarded by {@code this}. */
    private int nextSlot;

    /** Revision root records by revision number. */
    private volatile RevisionRecord[] revisions = new RevisionRecord[INITIAL_REVISIONS];

    /**
     * The uber page in the file storages' beacon framing ({@code [u32 len][payload][u64 hash]}), or
     * {@code null} before the first commit.
     */
    volatile byte @Nullable [] uberBeacon;

    Store(final @Nullable UUID resourceUuid) {
      this.resourceUuid = resourceUuid;
    }

    synchronized long append(final byte[] payload) {
      final int slot = nextSlot;
      if (slot == Integer.MAX_VALUE) {
        throw new SirixIOException("In-memory storage is full: " + slot + " pages");
      }
      byte[][] current = payloads;
      if (slot == current.length) {
        current = Arrays.copyOf(current, (int) Math.min(Integer.MAX_VALUE, (long) current.length << 1));
        payloads = current;
      }
      PAYLOADS.setRelease(current, slot, payload);
      nextSlot = slot + 1;
      return IOStorage.DATA_REGION_START + slot;
    }

    byte[] payload(final long key) {
      final byte[][] current = payloads;
      final long slot = key - IOStorage.DATA_REGION_START;
      final byte[] payload = slot >= 0 && slot < current.length
          ? (byte[]) PAYLOADS.getAcquire(current, (int) slot)
          : null;
      if (payload == null) {
        throw new SirixIOException("In-memory storage holds no page at key " + key);
      }
      return payload;
    }

    synchronized void recordRevision(final int revision, final RevisionRecord revisionRecord) {
      RevisionRecord[] current = revisions;
      if (revision >= current.length) {
        current = Arrays.copyOf(current, Math.max(revision + 1, current.length << 1));
        revisions = current;
      }
      RECORDS.setRelease(current, revision, revisionRecord);
    }

    RevisionRecord revision(final int revision) {
      final RevisionRecord[] current = revisions;
      final RevisionRecord revisionRecord = revision >= 0 && revision < current.length
          ? (RevisionRecord) RECORDS.getAcquire(current, revision)
          : null;
      if (revisionRecord == null) {
        throw new SirixIOException("In-memory storage holds no revision " + revision);
      }
      return revisionRecord;
    }

    synchronized void clear() {
      payloads = new byte[INITIAL_PAGES][];
      revisions = new RevisionRecord[INITIAL_REVISIONS];
      nextSlot = 0;
      uberBeacon = null;
      revisionIndexHolder.update(RevisionIndex.EMPTY);
    }
  }

  /**
   * Reads and writes one resource's store. Decoding is the shared {@link AbstractReader} path, so a
   * page read here is checked and decoded exactly as a page read from a file.
   */
  private static final class RAMAccess extends AbstractReader implements Writer {

    private final Store store;

    private final ByteHandlerPipeline pipeline;

    private final PagePersister persister;

    /** Serialization scratch, created on the first write: readers never need it. */
    private @Nullable MemorySegmentBytesOut scratch;

    RAMAccess(final Store store, final ByteHandlerPipeline pipeline) {
      this(store, pipeline, new PagePersister());
    }

    private RAMAccess(final Store store, final ByteHandlerPipeline pipeline, final PagePersister persister) {
      super(pipeline, persister, SerializationType.DATA);
      this.store = store;
      this.pipeline = pipeline;
      this.persister = persister;
    }

    @Override
    public Page read(final PageReference reference, final @Nullable ResourceConfiguration resourceConfiguration) {
      final long key = reference.getKey();
      final byte[] payload = key == IOStorage.PRIMARY_BEACON_OFFSET || key == IOStorage.SECONDARY_BEACON_OFFSET
          ? uberPayload()
          : store.payload(key);
      verifyChecksumIfNeeded(payload, reference, resourceConfiguration);
      try {
        if (pipeline.supportsMemorySegments()) {
          try (final ScratchLease lease = ScratchLease.of(payload.length, ScratchUse.READ)) {
            final MemorySegment segment = lease.segment().asSlice(0, payload.length);
            MemorySegment.copy(payload, 0, segment, ValueLayout.JAVA_BYTE, 0, payload.length);
            return deserializeFromSegment(resourceConfiguration, segment, reference);
          }
        }
        return deserialize(resourceConfiguration, payload, reference);
      } catch (final IOException e) {
        throw new SirixIOException(e);
      }
    }

    @Override
    protected ByteBuffer readBeaconSlot(final long offset) {
      return ByteBuffer.wrap(uberBeacon());
    }

    /**
     * The uber page's payload, for a read of a beacon offset as a page (see {@link Store#payloads}).
     */
    private byte[] uberPayload() {
      final byte[] beacon = uberBeacon();
      final int length = ByteBuffer.wrap(beacon).order(ByteOrder.LITTLE_ENDIAN).getInt(0);
      return Arrays.copyOfRange(beacon, Integer.BYTES, Integer.BYTES + length);
    }

    private byte[] uberBeacon() {
      final byte[] beacon = store.uberBeacon;
      if (beacon == null) {
        throw new SirixIOException("In-memory storage holds no committed uber page");
      }
      return beacon;
    }

    @Override
    public Writer write(final ResourceConfiguration resourceConfiguration, final PageReference pageReference,
        final Page page, final BytesOut<?> bufferedBytes) {
      final byte[] payload = serialize(resourceConfiguration, page);
      final long pageHash = PageHasher.computeLong(payload);
      final long key = store.append(payload);
      pageReference.setKey(key);
      pageReference.setHash(pageHash);
      if (page instanceof RevisionRootPage revisionRootPage) {
        final long timestamp = revisionRootPage.getRevisionTimestamp();
        store.recordRevision(revisionRootPage.getRevision(), new RevisionRecord(key, timestamp, pageHash));
        store.revisionIndexHolder.addRevision(key, timestamp);
      }
      return this;
    }

    @Override
    public Writer writeUberPageReference(final ResourceConfiguration resourceConfiguration,
        final PageReference pageReference, final Page page, final BytesOut<?> bufferedBytes) {
      if (!(page instanceof UberPage)) {
        throw new IllegalArgumentException("Not an uber page: " + page);
      }
      final byte[] payload = serialize(resourceConfiguration, page);
      final long pageHash = PageHasher.computeLong(payload);
      // The beacon framing AbstractReader verifies: little-endian length, payload, the payload's hash
      // in its canonical big-endian byte order.
      final ByteBuffer beacon =
          ByteBuffer.allocate(Integer.BYTES + payload.length + Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      beacon.putInt(payload.length).put(payload).putLong(Long.reverseBytes(pageHash));
      store.uberBeacon = beacon.array();
      // As a file writer leaves it after writing both beacon copies: keyed at the last one written.
      pageReference.setKey(IOStorage.SECONDARY_BEACON_OFFSET);
      pageReference.setHash(pageHash);
      return this;
    }

    /**
     * The payload a file-backed writer would append for {@code page}: a key-value leaf page's cached
     * encoding when it has one, otherwise the page encoding run through the byte-handler pipeline.
     * Always a copy the store owns.
     */
    private byte[] serialize(final ResourceConfiguration resourceConfiguration, final Page page) {
      try {
        if (page instanceof KeyValueLeafPage keyValueLeafPage) {
          // close() holds the same monitor: the page can neither close nor hand its frame back while
          // its encoding is copied.
          synchronized (keyValueLeafPage) {
            if (keyValueLeafPage.isClosed()) {
              throw new IllegalStateException("cannot write a closed key-value leaf page");
            }
            return serializeLeaf(resourceConfiguration, keyValueLeafPage);
          }
        }
        return encode(resourceConfiguration, page);
      } catch (final IOException e) {
        throw new SirixIOException(e);
      } finally {
        final MemorySegmentBytesOut current = scratch;
        if (current != null) {
          current.clear();
        }
      }
    }

    private byte[] serializeLeaf(final ResourceConfiguration resourceConfiguration, final KeyValueLeafPage page)
        throws IOException {
      final MemorySegment cached = page.getCompressedSegment();
      if (cached != null) {
        return cached.toArray(ValueLayout.JAVA_BYTE);
      }
      final MemorySegmentBytesOut bytes = scratch();
      persister.serializePage(resourceConfiguration, bytes, page, SerializationType.DATA);
      final MemorySegment compressed = page.getCompressedSegment();
      if (compressed != null) {
        return compressed.toArray(ValueLayout.JAVA_BYTE);
      }
      final BytesOut<?> legacy = page.getBytes();
      if (legacy != null) {
        return legacy.toByteArray();
      }
      return compress(bytes);
    }

    private byte[] encode(final ResourceConfiguration resourceConfiguration, final Page page) throws IOException {
      final MemorySegmentBytesOut bytes = scratch();
      persister.serializePage(resourceConfiguration, bytes, page, SerializationType.DATA);
      return compress(bytes);
    }

    /** Run the encoded page through the byte-handler pipeline, as the file writers do. */
    private byte[] compress(final MemorySegmentBytesOut bytes) throws IOException {
      if (pipeline.isEmpty()) {
        return bytes.toByteArray();
      }
      if (pipeline.supportsMemorySegments()) {
        return pipeline.compress(bytes.getDestination()).toArray(ValueLayout.JAVA_BYTE);
      }
      final byte[] encoded = bytes.toByteArray();
      final ByteArrayOutputStream output = new ByteArrayOutputStream(encoded.length);
      try (final DataOutputStream dataOutput = new DataOutputStream(pipeline.serialize(output))) {
        dataOutput.write(encoded);
      }
      return output.toByteArray();
    }

    private MemorySegmentBytesOut scratch() {
      MemorySegmentBytesOut current = scratch;
      if (current == null) {
        current = MemorySegmentBytesOut.synchronousScratch(Writer.FLUSH_SIZE);
        scratch = current;
      }
      return current;
    }

    @Override
    public RevisionRootPage readRevisionRootPage(final int revision,
        final @Nullable ResourceConfiguration resourceConfiguration) {
      final RevisionRecord revisionRecord = store.revision(revision);
      final PageReference reference = new PageReference();
      reference.setKey(revisionRecord.key());
      reference.setHash(revisionRecord.pageHash());
      return (RevisionRootPage) read(reference, resourceConfiguration);
    }

    @Override
    public Instant readRevisionRootPageCommitTimestamp(final int revision) {
      return Instant.ofEpochMilli(store.revision(revision).timestampMillis());
    }

    @Override
    public RevisionFileData getRevisionFileData(final int revision) {
      final RevisionRecord revisionRecord = store.revision(revision);
      return new RevisionFileData(revisionRecord.key(), Instant.ofEpochMilli(revisionRecord.timestampMillis()),
          revisionRecord.pageHash());
    }

    @Override
    public Writer truncate() {
      store.clear();
      return this;
    }

    @Override
    public boolean supportsTruncateTo() {
      return false;
    }

    @Override
    public Writer truncateTo(final int revision) {
      // Callers of truncateTo — crash recovery in AbstractResourceSession and explicit rollback via
      // NodeStorageEngineWriter — treat a normal return as "the resource is now at `revision`", and
      // go on to commit on top of that. This storage keeps only the newest uber page, so it cannot
      // restore an earlier one. Fail loudly instead of reporting a rollback that never happened.
      throw new UnsupportedOperationException("RAMStorage cannot truncate to revision " + revision
          + ": only the newest uber page is retained, " + "so a rollback cannot be honoured. "
          + "Use a persistent StorageType for resources that need rollback or crash recovery.");
    }

    @Override
    public void forceAll() {
      // Nothing to make durable.
    }

    @Override
    public void close() {
      // The store belongs to the resource; the scratch is heap memory the collector reclaims.
    }
  }
}
