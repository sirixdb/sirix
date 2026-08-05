package io.sirix.page.pax;

import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.page.SirixLZ77Codec;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.LongAdder;

/**
 * Length-prefixed list of PAX regions appended to a {@link io.sirix.page.KeyValueLeafPage}.
 *
 * <p>Wire format:
 * <pre>
 * int regionCount
 * regionCount × { byte kind, int size, byte[size] payload }
 * </pre>
 *
 * <p>Each region holds payload-type-segregated data (numeric values, string dictionary
 * entries, struct pointers, DeweyIDs) so scan operators can read a contiguous buffer
 * without per-slot varint decode. The table itself is deliberately simple — per-region
 * encoding lives inside the payload bytes, so adding a new encoding doesn't require a
 * further format bump.
 *
 * <h2>HFT-grade access</h2>
 * Regions are kept in a fixed-size {@code byte[KIND_COUNT][]} slotted by kind ordinal.
 * {@link #payload(byte)} is a single array read with no branching, no linear scan, no
 * boxing, no per-call allocation. The on-read allocation at {@link #read(BytesIn)} is
 * bounded by region count (≤ {@link #KIND_COUNT}) and is only paid once per page load.
 *
 * <p>This class is the Phase-1 scaffold: the table round-trips cleanly but is empty
 * on writes produced by the current codebase. Later tasks populate it with number,
 * string, struct, and DeweyID regions.
 */
public final class RegionTable {

  public static final byte KIND_NUMBER = 0;
  public static final byte KIND_STRING = 1;
  public static final byte KIND_STRUCT = 2;
  public static final byte KIND_DEWEYID = 3;
  public static final byte KIND_OBJECT_KEY_NAMEKEY = 4;
  /**
   * BooleanRegion — packed-bit column for OBJECT_BOOLEAN_VALUE slots, tag-grouped
   * by parent OBJECT_KEY nameKey / pathNodeKey. Enables the columnar filter path
   * for predicates like {@code $u.active} that currently drop back to OBJECT_KEY
   * slot traversal (see task #48 ColumnarScanExecutor).
   */
  public static final byte KIND_BOOLEAN = 5;

  /**
   * HashRegion — per-page column of record hash values, sparse bitmap-indexed.
   * Replaces the per-record 8-byte hash field in the slotted-page heap. For
   * resources configured {@link io.sirix.access.trx.node.HashType#NONE} the
   * region is simply absent and every record's hash resolves to 0 via the
   * sentinel offset-table entry written by the node writers. Saves ~2.4 GB /
   * 100M-record shred on the bench workload.
   */
  public static final byte KIND_HASH = 6;

  /**
   * StructuralPointersRegion — per-page FOR+RLE+delta-of-delta columns for
   * {@code parentKey}, {@code leftSiblingKey}, {@code rightSiblingKey}, and
   * {@code firstChildKey}. In preorder-shred workloads these columns degenerate
   * to monotonic sequences (+1 for right-sibling runs, -N for parent within a
   * subtree), compressing to near-zero bits per record. Replaces the four
   * per-record delta-varint fields in the heap.
   */
  public static final byte KIND_STRUCT_POINTERS = 7;

  /**
   * {@link StringDictSketch} — Bloom filter over the page's string dictionary entries. Lets an
   * equality predicate rule a page out without decompressing {@link #KIND_STRING}, which is the
   * single most expensive thing a string scan does.
   */
  public static final byte KIND_STRING_DICT_SKETCH = 8;

  /**
   * {@link NumberZoneMapRegion} — the per-tag min/max of {@link #KIND_NUMBER}, stored raw and on
   * its own. Lets a range predicate rule a page out without decompressing the number column, which
   * is where the zone maps used to live and therefore could not save the decompression they were
   * meant to make unnecessary.
   */
  public static final byte KIND_NUMBER_ZONEMAP = 9;

  /**
   * {@link RecordOrdinalRegion} — which record each OBJECT_KEY slot belongs to. The one piece of
   * structure the per-field columns cannot supply, and what a predicate spanning two fields needs
   * before it can intersect them. Absent on pages whose records are not wholly page-local; every
   * reader falls back to the record path when it is.
   */
  public static final byte KIND_RECORD_ORDINAL = 10;

  /** Size of the fixed-slot storage. Bump when a new region kind is introduced. */
  public static final int KIND_COUNT = 11;

  /** Sentinel empty payload, so an absent-but-present region needs no special case. */
  private static final MemorySegment EMPTY = MemorySegment.ofArray(new byte[0]);

  /**
   * Region payload slotted by kind ordinal. Index = {@link #KIND_NUMBER},
   * {@link #KIND_STRING}, etc. {@code null} when the region is absent.
   *
   * <h2>Why these are off-heap</h2>
   *
   * <p>Payloads used to be heap {@code byte[]}, which cost twice. Vector loads against a heap
   * segment do not intrinsify — 16 GB/s against 128 for a native one, measured, and unchanged on
   * JDK 26 — so every column kernel that wrapped one was loading at a fraction of the rate its
   * arithmetic could sustain. And a decompressing read could not write into its destination: the
   * LZ77 decoder needs tail slack past the decoded bytes, so it decoded into an oversized
   * thread-local scratch and then allocated an exact-size array and copied. Allocating the payload
   * natively with that slack built in removes the copy, removes the per-region garbage a scan
   * across thousands of pages would otherwise make, and hands the kernels the load they want.
   *
   * <p>See {@link ColumnLoad} for the measurements this rests on.
   */
  private final MemorySegment[] payloads = new MemorySegment[KIND_COUNT];

  /**
   * Arena backing this table's native payloads, created on first allocation.
   *
   * <p>{@link Arena#ofAuto()} rather than a closeable arena: a table's payloads are reachable from
   * a page that a cache may hand to any thread and evict at any time, so there is no point at which
   * an explicit close is provably safe. An automatic arena frees the memory once the arena and
   * every segment from it are unreachable, which makes a stale payload reference impossible by
   * construction rather than by discipline. Per table rather than per payload so a page's regions
   * are reclaimed as one unit.
   */
  private volatile Arena arena;

  /** Live count — number of region slots whose payload is non-empty. */
  private int liveCount;

  public RegionTable() {
    // payloads start as null[]; keep null semantics to distinguish "absent" from "empty bytes".
  }

  /**
   * Wire bytes of payloads whose decompression was deferred, or {@code null} when nothing on this
   * table was deferred (the overwhelmingly common case, and the one {@link #payload} is tuned for).
   */
  private byte[][] deferredWire;

  /** Uncompressed length of each deferred payload, parallel to {@link #deferredWire}. */
  private int[] deferredRawLen;

  /**
   * Slot accessor with release/acquire ordering.
   *
   * <p>Needed only because of lazy materialization. A table built by {@link #read} is fully
   * written before anything else can see it, and whatever publishes it — a
   * {@code ConcurrentHashMap} put, a {@code Future}, a constructor — supplies the edge. A DEFERRED
   * table is different: it is published first and completed later, so the write in
   * {@link #materializeDeferred} races every subsequent reader. A plain array write there is the
   * textbook unsafe publication: a reader can see the non-null reference without the bytes behind
   * it and silently count against a zero-filled region. On x86 the acquire load compiles to an
   * ordinary {@code mov}, so the fast path pays nothing for it.
   */
  private static final VarHandle PAYLOAD_SLOT =
      MethodHandles.arrayElementVarHandle(MemorySegment[].class);

  /**
   * Returns the payload bytes for {@code kind}, or {@code null} when absent. O(1) — one array read
   * and one null check — unless the payload was read in deferred form, in which case the first
   * call decompresses it and every later call is O(1) again.
   */
  public MemorySegment payload(final byte kind) {
    final MemorySegment p = (MemorySegment) PAYLOAD_SLOT.getAcquire(payloads, (int) kind);
    if (p != null) {
      return p;
    }
    // Absent — or deferred and not yet materialized. Deliberately does NOT peek at
    // deferredWire[kind] to decide: a reader could observe that slot already nulled by a
    // materializing thread while the payload it installed is not yet visible here, and would then
    // report an existing region as absent. The check under the lock is the only reliable one; the
    // field read below just keeps non-deferred tables (all but the column-scan path) on the fast
    // path, since for those deferredWire is null for the object's whole life.
    return deferredWire == null ? null : materializeDeferred(kind);
  }

  /**
   * Decompress a deferred payload on first use.
   *
   * <p>Synchronized because a deferred table is cheap to share by accident and a torn
   * double-materialize would hand two callers different arrays; the lock is taken at most once per
   * kind per page and never on the fast path above.
   */
  private synchronized MemorySegment materializeDeferred(final byte kind) {
    final MemorySegment existing = payloads[kind];
    if (existing != null) {
      return existing;
    }
    final byte[] wire = deferredWire[kind];
    if (wire == null) {
      return null;
    }
    final int rawLen = deferredRawLen[kind];
    final int wireLen = deferredWireLen[kind];
    // Decompress BEFORE touching the bookkeeping: decompressInto throws on a length mismatch, and
    // decrementing first left the counter one low with the wire bytes still present — enough for
    // write()'s "refusing to serialize a deferred table" guard to pass and drop the region.
    final MemorySegment payload = rawLen == wireLen
        ? copyIn(wire, 0, rawLen)
        : decompressInto(wire, wireLen, rawLen, kind);
    // Install BEFORE clearing the wire slot: both are seen by readers that re-enter this method,
    // and the reverse order leaves a window where neither slot holds the region.
    setSegment(kind, payload);
    clearDeferred(kind);
    if (READ_DIAG) {
      MATERIALIZED_BYTES[kind].add(rawLen);
    }
    return payload;
  }

  /**
   * Allocate a native payload of {@code rawLen} bytes.
   *
   * <p>Over-allocated by {@link #DECODE_TAIL_SLACK} and sliced back, because the native LZ77
   * decoder's wildcopy hot path writes past the decoded length and is documented against exactly
   * that much room. Sizing the visible payload to {@code rawLen} keeps {@code byteSize()} honest
   * for every reader, while the slack the decoder needs sits beyond it in the same allocation. A
   * payload sized exactly would still decode — but only via the Java decoder, silently taking the
   * whole engine off the fast path.
   */
  private MemorySegment allocate(final int rawLen) {
    Arena local = arena;
    if (local == null) {
      // Double-checked under the monitor: allocate() is reached both from the synchronized
      // materializeDeferred and from the unsynchronized set(), so two threads could otherwise mint
      // two arenas for one table and split its payloads across two independently reclaimed
      // lifetimes — breaking the "reclaimed as one unit" invariant this field exists to provide.
      synchronized (this) {
        local = arena;
        if (local == null) {
          local = Arena.ofAuto();
          arena = local;
        }
      }
    }
    return local.allocate(rawLen + DECODE_TAIL_SLACK, Long.BYTES).asSlice(0, rawLen);
  }

  /** Copy {@code len} bytes of an array into a fresh native payload. */
  private MemorySegment copyIn(final byte[] src, final int off, final int len) {
    final MemorySegment target = allocate(len);
    MemorySegment.copy(src, off, target, ValueLayout.JAVA_BYTE, 0L, len);
    return target;
  }

  /**
   * Decompress {@code encodedLen} wire bytes straight into a fresh native payload.
   *
   * <p>The whole reason payloads are native: the decoder writes its output where the readers will
   * read it, with no scratch buffer in between and no copy afterwards.
   */
  private MemorySegment decompressInto(final byte[] wire, final int encodedLen, final int rawLen,
      final byte kind) {
    final MemorySegment target = allocate(rawLen);
    // The slack lives past the slice, so hand the decoder the full allocation to write into.
    final MemorySegment writable = target.reinterpret(rawLen + DECODE_TAIL_SLACK);
    final int decoded = SirixLZ77Codec.decode(wire, 0, encodedLen, writable, 0L);
    if (decoded != rawLen) {
      throw new IllegalStateException("region kind " + kind + " decompressed to " + decoded
          + " bytes, expected " + rawLen);
    }
    return target;
  }

  /**
   * Forget the deferred wire bytes for {@code kind}, and drop the whole deferred view once the last
   * one is gone.
   *
   * <p>Nulling the array is what lets {@link #payload} return to its lock-free fast path: while
   * {@code deferredWire} is non-null, a lookup of an ABSENT kind falls into the synchronized
   * materializer just to discover there is nothing there — once per page, on shared tables, on the
   * hot scan loop.
   */
  private void clearDeferred(final byte kind) {
    if (deferredWire == null || deferredWire[kind] == null) {
      return;
    }
    deferredWire[kind] = null;
    deferredCount--;
    if (deferredCount == 0) {
      deferredWire = null;
      deferredRawLen = null;
      deferredWireLen = null;
    }
  }

  /**
   * Installs a payload for the given region kind, copying it off-heap. Pass {@code null} to clear.
   *
   * <p>The copy is what keeps every reader on the intrinsified load path: encoders build their
   * output in an on-heap {@code byte[]}, and a page that was just written is read back by the same
   * kernels that read one loaded from disk.
   */
  public void set(final byte kind, final byte[] payload) {
    setSegment(kind, payload == null ? null : copyIn(payload, 0, payload.length));
  }

  /**
   * Whether a region is present, WITHOUT materializing it.
   *
   * <p>Deliberately not expressed as {@code payload(kind) != null}: that accessor is the one that
   * decompresses a deferred region, so asking a deferred table what it holds would decode the very
   * payload the deferral exists to postpone — at a call site (invalidation on the mutation path)
   * that must stay cheap.
   */
  public boolean hasRegion(final byte kind) {
    if (payloads[kind] != null) {
      return true;
    }
    final byte[][] deferred = deferredWire;
    return deferred != null && deferred[kind] != null;
  }

  /** Installs an already-native payload. Pass {@code null} to clear. */
  public synchronized void setSegment(final byte kind, final MemorySegment payload) {
    if (payload == null) {
      // Clearing has to reach the deferred form too. hasRegion() reports a deferred region as
      // present, so an invalidation that consulted it and then called this would otherwise believe
      // it had dropped the region while the next payload() quietly re-materialized the stale one.
      clearDeferred(kind);
    }
    final MemorySegment prev = payloads[kind];
    if (prev == null && payload != null) {
      liveCount++;
    } else if (prev != null && payload == null) {
      liveCount--;
    }
    // Release: the payload's contents must be visible to anyone who observes the reference. See
    // PAYLOAD_SLOT — only lazy materialization actually needs this, but a store-release costs
    // nothing on x86 and one accessor is easier to keep correct than two.
    PAYLOAD_SLOT.setRelease(payloads, (int) kind, payload);
  }

  /**
   * Bytes of payload this table retains, counting a deferred region by its COMPRESSED wire length.
   *
   * <p>Exists because the obvious formulation — loop over {@link #payload(byte)} and sum the
   * lengths — is precisely wrong for a deferred table: that accessor is the one that decompresses,
   * so asking such a table how much memory it holds would materialize everything it was built to
   * defer, at the call site that most wants to be cheap (a cache admitting a page it may not even
   * keep). Sizing is an accounting question and must not decode.
   */
  public int retainedBytes() {
    int total = 0;
    for (int kind = 0; kind < KIND_COUNT; kind++) {
      final MemorySegment p = payloads[kind];
      if (p != null) {
        total += (int) p.byteSize();
      } else if (deferredWire != null && deferredWire[kind] != null) {
        // The encoded length, not the buffer's — the buffer carries decoder tail slack past it.
        total += deferredWireLen[kind];
      }
    }
    return total;
  }

  /** Payloads read in deferred form and not yet materialized. */
  private int deferredCount;

  /** Record a payload's wire bytes for later decompression. See {@link #read(BytesIn, int, int)}. */
  private void defer(final byte kind, final byte[] wire, final int rawLen, final int wireLen) {
    if (deferredWire == null) {
      deferredWire = new byte[KIND_COUNT][];
      deferredRawLen = new int[KIND_COUNT];
      deferredWireLen = new int[KIND_COUNT];
    }
    deferredWire[kind] = wire;
    deferredRawLen[kind] = rawLen;
    // Held separately because the buffer is deliberately longer than the frame; see the read path.
    deferredWireLen[kind] = wireLen;
    deferredCount++;
  }

  /** Encoded length of each deferred payload, which is shorter than its buffer. */
  private int[] deferredWireLen;

  public boolean isEmpty() {
    return liveCount + deferredCount == 0;
  }

  /** Regions this table holds, materialized or deferred — consistent with {@link #isEmpty()}. */
  public int size() {
    return liveCount + deferredCount;
  }

  /** Wire codec ids for region payloads. Matches the heap body's id for LZ77 (3). */
  private static final byte PAYLOAD_RAW = 0;
  private static final byte PAYLOAD_LZ77 = 3;

  /**
   * Payloads below this size skip the compression attempt: the codec's own framing plus the
   * extra length int eat any conceivable saving, and the attempt itself is not free.
   */
  private static final int MIN_COMPRESS_BYTES = 64;

  /**
   * Per-thread scratch for the encode attempt, sized to the LZ77 worst case and grown on demand.
   */
  private static final ThreadLocal<byte[]> ENCODE_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[64 * 1024]);

  /**
   * Tail slack every payload allocation carries past its visible length. Sized for the NATIVE
   * decoder's requirement rather than the Java one: less slack still decodes correctly, but only
   * via the Java decoder, which would silently switch the whole engine onto the slow path.
   */
  private static final int DECODE_TAIL_SLACK = SirixLZ77Codec.NATIVE_OUTPUT_TAIL_SLACK;

  /** Matching slack on the buffer the compressed bytes are read into. */
  private static final int ENCODE_TAIL_SLACK = SirixLZ77Codec.NATIVE_INPUT_TAIL_SLACK;

  /**
   * Write the table, compressing each payload that pays for it.
   *
   * <p>The regions are the single largest section of a JSON database — with strings stored once
   * (heap value elision), the string region alone was 77% of all page bytes, written raw. Text
   * compresses well, and the scans that make the regions worth having never see these wire
   * bytes: {@link #read} decompresses into the in-memory payload arrays exactly once per page
   * load, so the SIMD paths keep scanning raw bytes. Each payload elects its own codec — raw
   * when compression does not strictly win — so incompressible regions cost one length int and
   * nothing else.
   *
   * @param sink the sink
   * @param compress whether payloads may elect compression at all — the resource's
   *        {@code RegionCompressionType} speed/size dial. The format stays self-describing per
   *        payload either way, so readers never consult the setting and databases written under
   *        one setting remain readable under the other.
   */
  /**
   * Order regions are written in — deliberately NOT kind order. A column scan reads the wire
   * forwards and stops once it has what it asked for, so the small, frequently-scanned regions come
   * first and {@link #KIND_STRING} — routinely the majority of a page's bytes — comes last. That is
   * what lets a reader fetch a few kilobytes from the front of the table instead of the whole page.
   *
   * <p>Readers dispatch on the kind byte, so any permutation is readable; this one is chosen for
   * where the bytes land, not for what they mean.
   */
  private static final byte[] WRITE_ORDER = {
      KIND_NUMBER_ZONEMAP, KIND_NUMBER, KIND_OBJECT_KEY_NAMEKEY, KIND_RECORD_ORDINAL,
      KIND_STRING_DICT_SKETCH, KIND_BOOLEAN, KIND_STRUCT, KIND_STRUCT_POINTERS, KIND_HASH,
      KIND_DEWEYID, KIND_STRING
  };

  static {
    // write() emits liveCount as the entry count and then iterates WRITE_ORDER. A kind installed
    // in payloads[] but missing here would make the declared count exceed the entries written, and
    // read() would consume the next page section's bytes as a region header — misparsing
    // everything after the table. Cheaper to fail at class load than to debug on the wire.
    if (WRITE_ORDER.length != KIND_COUNT) {
      throw new AssertionError(
          "WRITE_ORDER covers " + WRITE_ORDER.length + " kinds but KIND_COUNT is " + KIND_COUNT);
    }
    final boolean[] seen = new boolean[KIND_COUNT];
    for (final byte kind : WRITE_ORDER) {
      if (kind < 0 || kind >= KIND_COUNT || seen[kind]) {
        throw new AssertionError("WRITE_ORDER is not a permutation of the region kinds: " + kind);
      }
      seen[kind] = true;
    }
  }

  /**
   * Region kinds that must never be compressed, however large they get.
   *
   * <p>These are the summaries a scan reads in order to decide whether it needs the region they
   * summarise. Compressing one would put a decode between the reader and the decision, which is
   * precisely the cost it exists to avoid — a zone map that must be decompressed before it can
   * rule a page out has given the saving back before collecting it. They are small by
   * construction, so the bytes given up are few.
   */
  private static boolean neverCompress(final byte kind) {
    return kind == KIND_NUMBER_ZONEMAP;
  }

  /**
   * Whether a region is a pure accelerator, i.e. one every reader already falls back from.
   *
   * <p>Load-bearing for cache reuse: a resident table is only reused when it holds every kind the
   * caller asked for, and an accelerator fails that test on every page written before it existed
   * and on every page whose encoding does not produce one. Treating its absence as a miss would
   * quietly send an existing database back to a full page read per page per query — correct
   * answers, permanently slower than before the accelerator was added.
   */
  public static boolean isOptionalAccelerator(final byte kind) {
    // The dictionary sketch has the same shape and could join this, but it predates the rule and
    // its absence is already handled by fail-open in mayContain; left alone deliberately.
    //
    // KIND_RECORD_ORDINAL belongs here for the same reason as the zone map, plus one of its own: it
    // is absent by design on any page holding a record that spans pages, so treating its absence as
    // a cache miss would re-read such pages on every query forever.
    return kind == KIND_NUMBER_ZONEMAP || kind == KIND_RECORD_ORDINAL;
  }

  public void write(final BytesOut<?> sink, final boolean compress) {
    // A deferred region lives in deferredWire, not payloads, and the loop below reads only
    // payloads — so serializing such a table would drop those regions with no error at all, and
    // the page would come back missing columns. Nothing constructs this situation today (deferred
    // tables are produced by the read-only column path and never written back), which is exactly
    // why it needs to fail loudly the day something does.
    if (deferredCount != 0) {
      throw new IllegalStateException("refusing to serialize a region table with " + deferredCount
          + " deferred region(s): they are not in payloads[] and would be silently dropped");
    }
    sink.writeInt(liveCount);
    if (liveCount == 0) {
      return;
    }
    for (final byte kind : WRITE_ORDER) {
      final MemorySegment p = payloads[kind];
      if (p == null) {
        continue;
      }
      final int len = (int) p.byteSize();
      sink.writeByte(kind);
      if (compress && len >= MIN_COMPRESS_BYTES && !neverCompress(kind)) {
        final int bound = SirixLZ77Codec.maxEncodedSize(len);
        byte[] out = ENCODE_SCRATCH.get();
        if (out.length < bound) {
          out = new byte[Math.max(bound, out.length * 2)];
          ENCODE_SCRATCH.set(out);
        }
        // The encoder already reads from a segment, so a native payload needs no staging at all.
        final int encodedLen = SirixLZ77Codec.encode(p, 0L, len, out, 0);
        if (encodedLen > 0 && encodedLen < len) {
          sink.writeByte(PAYLOAD_LZ77);
          sink.writeInt(len);
          sink.writeInt(encodedLen);
          sink.write(out, 0, encodedLen);
          continue;
        }
      }
      sink.writeByte(PAYLOAD_RAW);
      sink.writeInt(len);
      if (len > 0) {
        // The sink takes bytes, so an uncompressed payload stages through the same per-thread
        // buffer the compression attempt uses rather than allocating per region.
        byte[] out = ENCODE_SCRATCH.get();
        if (out.length < len) {
          out = new byte[Math.max(len, out.length * 2)];
          ENCODE_SCRATCH.set(out);
        }
        MemorySegment.copy(p, ValueLayout.JAVA_BYTE, 0L, out, 0, len);
        sink.write(out, 0, len);
      }
    }
  }

  public static RegionTable read(final BytesIn<?> source) {
    return read(source, ALL_KINDS);
  }

  /** {@link #read(BytesIn, int)} mask selecting every region kind. */
  public static final int ALL_KINDS = (1 << KIND_COUNT) - 1;

  /** @return the {@link #read(BytesIn, int)} mask that selects exactly {@code kind}. */
  public static int maskOf(final byte kind) {
    return 1 << kind;
  }

  /**
   * Read the table, materializing only the payloads whose kind bit is set in {@code kindMask}.
   *
   * <p>Every other region is skipped over by its length prefix — never copied, never
   * decompressed. That is the whole point on the column-scan path: a page's STRING region is
   * routinely the majority of its bytes, and decompressing it to answer {@code count(year > N)}
   * is pure waste. The skipped kinds are simply absent from the returned table, so a caller that
   * asks for a kind it did not request gets {@code null} — the same answer it gets for a region
   * the writer never emitted, which every consumer already handles by falling back.
   *
   * @param source the wire bytes, positioned at the region-count int
   * @param kindMask bitmask of {@link #KIND_NUMBER} .. kinds to materialize; see {@link #maskOf}
   * @return the table holding only the requested regions
   */
  /**
   * Per-kind accounting of what a read actually materializes, off unless
   * {@code -Dsirix.page.regionReadDiag=true}. A column scan's cost is dominated by the payloads it
   * decompresses, and "which kind, how many bytes" is not derivable from the wall clock.
   */
  private static final boolean READ_DIAG = Boolean.getBoolean("sirix.page.regionReadDiag");

  private static final LongAdder[] MATERIALIZED_BYTES =
      new LongAdder[KIND_COUNT];
  private static final LongAdder[] SKIPPED_BYTES =
      new LongAdder[KIND_COUNT];

  static {
    for (int i = 0; i < KIND_COUNT; i++) {
      MATERIALIZED_BYTES[i] = new LongAdder();
      SKIPPED_BYTES[i] = new LongAdder();
    }
  }

  /** Uncompressed bytes materialized for {@code kind} since the last reset. */
  public static long materializedBytes(final byte kind) {
    return MATERIALIZED_BYTES[kind].sum();
  }

  /** On-wire bytes stepped over for {@code kind} since the last reset. */
  public static long skippedBytes(final byte kind) {
    return SKIPPED_BYTES[kind].sum();
  }

  public static void resetReadDiag() {
    for (int i = 0; i < KIND_COUNT; i++) {
      MATERIALIZED_BYTES[i].reset();
      SKIPPED_BYTES[i].reset();
    }
  }

  public static RegionTable read(final BytesIn<?> source, final int kindMask) {
    return read(source, kindMask, 0);
  }

  /**
   * Read the table, materializing the payloads in {@code kindMask} but leaving those additionally
   * named in {@code deferMask} compressed until someone asks for them.
   *
   * <p>Deferral exists for one shape of query: a predicate that can often prove it does not need a
   * region <em>after</em> the table has been read, from a cheaper region read in the same pass. A
   * string equality is exactly that — {@link StringDictSketch} rules the page out and the
   * dictionary is never decompressed. The wire bytes are copied out (a few KB) so the caller's
   * page buffer can be released immediately; only the decompression, which is the expensive part,
   * waits.
   *
   * <p>A deferred table is thread-confined by convention: it is produced only by the column-only
   * read path, whose pages are decoded and consumed by one worker.
   *
   * @param source the wire bytes, positioned at the region-count int
   * @param kindMask kinds to read at all
   * @param deferMask subset of {@code kindMask} to leave compressed until first use
   */
  public static RegionTable read(final BytesIn<?> source, final int kindMask, final int deferMask) {
    return read(source, kindMask, deferMask, false);
  }

  /**
   * Like {@link #read(BytesIn, int, int)} but stops the moment every requested kind has been seen.
   *
   * <p>Separate entry point on purpose: stopping early leaves {@code source} positioned <em>inside
   * the table</em>, so anything the caller expects to find after it — the page's overlong-entry
   * section and FSST reference — is no longer reachable. Only a caller that wants the regions and
   * nothing else may use this, which is exactly the bounded-chunk page read.
   */
  public static RegionTable readStoppingWhenSatisfied(final BytesIn<?> source, final int kindMask,
      final int deferMask) {
    return read(source, kindMask, deferMask, true);
  }

  private static RegionTable read(final BytesIn<?> source, final int kindMask, final int deferMask,
      final boolean stopWhenSatisfied) {
    final int count = source.readInt();
    final RegionTable t = new RegionTable();
    if (count == 0) {
      return t;
    }
    // Bits still outstanding. When this hits zero the rest of the table is of no interest and the
    // read returns without touching it — which is what makes a bounded, partial page read possible.
    int remaining = kindMask;
    for (int i = 0; i < count; i++) {
      if (stopWhenSatisfied && remaining == 0) {
        break;
      }
      final byte kind = source.readByte();
      final byte codec = source.readByte();
      final int rawLen = source.readInt();
      // Unwanted kind: step over its bytes without touching them. RAW is the raw length;
      // LZ77 stores the encoded length in the int that follows.
      if (kind < 0 || kind >= KIND_COUNT || (kindMask & (1 << kind)) == 0) {
        if (READ_DIAG && kind >= 0 && kind < KIND_COUNT) {
          SKIPPED_BYTES[kind].add(rawLen);
        }
        if (codec == PAYLOAD_RAW) {
          if (rawLen > 0) {
            source.skip(rawLen);
          }
        } else if (codec == PAYLOAD_LZ77) {
          final int encodedLen = source.readInt();
          if (encodedLen <= 0 || rawLen <= 0) {
            throw new IllegalStateException("region kind " + kind + " declares a compressed payload"
                + " with rawLen=" + rawLen + " encodedLen=" + encodedLen);
          }
          source.skip(encodedLen);
        } else {
          throw new IllegalStateException("region kind " + kind + " has unknown codec " + codec);
        }
        continue;
      }
      if ((deferMask & (1 << kind)) != 0) {
        // Copy the wire bytes verbatim and stop: decompression waits for payload(kind).
        // Validate the codec first — treating an unrecognised byte as LZ77 would read a length int
        // that is not there and misparse the rest of the table, where the skip branch above throws.
        if (codec != PAYLOAD_RAW && codec != PAYLOAD_LZ77) {
          throw new IllegalStateException("region kind " + kind + " has unknown codec " + codec);
        }
        final int wireLen = codec == PAYLOAD_RAW ? rawLen : source.readInt();
        if (codec == PAYLOAD_LZ77 && (wireLen <= 0 || rawLen <= 0)) {
          throw new IllegalStateException("region kind " + kind + " declares a compressed payload"
              + " with rawLen=" + rawLen + " encodedLen=" + wireLen);
        }
        // Over-sized by the native decoder's input tail slack. Sized exactly, the deferred payload
        // fails SirixLZ77Codec's `off + len + NATIVE_INPUT_TAIL_SLACK <= input.length` precondition
        // and every lazily materialized region silently takes the Java decoder — and deferral is
        // now the default for numeric scans, so that would be most of them.
        final byte[] wire = new byte[wireLen + ENCODE_TAIL_SLACK];
        if (wireLen > 0) {
          source.read(wire, 0, wireLen);
        }
        t.defer(kind, wire, rawLen, wireLen);
        remaining &= ~(1 << kind);
        continue;
      }
      final MemorySegment payload;
      if (codec == PAYLOAD_RAW) {
        if (rawLen == 0) {
          payload = EMPTY;
        } else {
          byte[] in = ENCODE_SCRATCH.get();
          if (in.length < rawLen) {
            in = new byte[Math.max(rawLen, in.length * 2)];
            ENCODE_SCRATCH.set(in);
          }
          source.read(in, 0, rawLen);
          payload = t.copyIn(in, 0, rawLen);
        }
      } else if (codec == PAYLOAD_LZ77) {
        final int encodedLen = source.readInt();
        if (encodedLen <= 0 || rawLen <= 0) {
          throw new IllegalStateException("region kind " + kind + " declares a compressed payload"
              + " with rawLen=" + rawLen + " encodedLen=" + encodedLen);
        }
        byte[] in = ENCODE_SCRATCH.get();
        if (in.length < encodedLen + ENCODE_TAIL_SLACK) {
          in = new byte[Math.max(encodedLen + ENCODE_TAIL_SLACK, in.length * 2)];
          ENCODE_SCRATCH.set(in);
        }
        source.read(in, 0, encodedLen);
        // Straight into the payload — no intermediate scratch, no copy afterwards.
        payload = t.decompressInto(in, encodedLen, rawLen, kind);
      } else {
        throw new IllegalStateException("region kind " + kind + " has unknown codec " + codec);
      }
      if (kind >= 0 && kind < KIND_COUNT) {
        t.setSegment(kind, payload);
        remaining &= ~(1 << kind);
        if (READ_DIAG) {
          MATERIALIZED_BYTES[kind].add(rawLen);
        }
      }
      // Unknown region kinds are silently skipped (forward-compat).
    }
    return t;
  }
}
