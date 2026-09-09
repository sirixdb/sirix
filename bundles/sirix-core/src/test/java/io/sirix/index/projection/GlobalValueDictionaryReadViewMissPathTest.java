package io.sirix.index.projection;

import com.sun.management.ThreadMXBean;
import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.access.DatabaseType;
import io.sirix.page.NamePage;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The {@link GlobalValueDictionary.ReadView} MISS path — the case its own javadoc used to gloss.
 *
 * <p>
 * A hit on the fixed 256-slot cache was always allocation-free. A miss was not: it walked
 * {@code entryResult}, which allocates a three-element {@code int[]} radix path, a
 * {@code LeafResult} and an {@code EntryResult} per call — all three purely to report a probe-unit
 * count the read view discards. On a high-cardinality column, where the working set cannot fit 256
 * slots, that is per-row garbage on a scan path.
 *
 * <p>
 * These tests pin both halves: the miss path still answers exactly what it used to, and it no
 * longer allocates those three objects. What is NOT claimed is that a miss is allocation-free in
 * general — materialising a record the reader's own record cache has evicted still decodes, which
 * is inherent to reading a page-resident record. The measurement below warms that cache first so it
 * isolates the traversal.
 */
final class GlobalValueDictionaryReadViewMissPathTest {

  private static final String RESOURCE_NAME = "readViewMissPathResource";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  /**
   * Must exceed BOTH view caches, or the measurement flatters itself.
   *
   * <p>
   * 256 entry slots is the obvious bound, but the 16-slot reverse-BUCKET cache is the binding one:
   * each bucket covers 256 ids, so anything under 4096 ids keeps every bucket resident after warm-up
   * and the sweep never pays a radix walk at all. 12 000 ids span ~47 buckets against 16 slots, so
   * buckets are evicted and re-walked continuously — the honest steady state for a high-cardinality
   * column.
   */
  private static final int ENTRIES = 12_000;

  /**
   * Set from the honest measurement, not from zero, and charged for the MISS alone.
   *
   * <p>
   * Measured 21.8 B/probe: with 16-slot bucket and block caches against ~47 buckets the decode of an
   * evicted bucket or sub-block is amortised over the ids it covers, but it is real and it is not
   * zero. The per-ID compare path itself allocates nothing — the materialising control on the same
   * meter sits three orders of magnitude above this, so a returning per-id copy or wrapper lands far
   * above this bound.
   * </p>
   *
   * <p>
   * Charged against the FULLY HOT window rather than against zero. Both windows drive the identical
   * {@code compareIds} call in the identical loop, so whatever a comparison costs before it resolves
   * anything is common to the two and cancels in the difference; what is left is the resolution, and
   * that is what this bounds. See {@link #GUARD_PROBES} for the term that makes the subtraction
   * necessary.
   * </p>
   */
  private static final double SLICE_PATH_MAX_BYTES_PER_PROBE = 40.0;

  /**
   * Revision guards one {@code compareIds} performs: one per operand.
   *
   * <p>
   * {@code compareIds} resolves a slice per id, and every resolution re-checks that the reader still
   * serves the revision the view was built for — {@code ReadView.ensureRevision} calling
   * {@link StorageEngineReader#getRevisionNumber()}. In production that guard is two field reads and
   * allocates nothing, which is why the hot bound below is effectively zero.
   * </p>
   */
  private static final int GUARDS_PER_COMPARE = 2;

  /**
   * Iterations of the revision-guard floor probe.
   *
   * <p>
   * <b>Why a floor exists at all.</b> This module selects Mockito's {@code mock-maker-inline} (see
   * {@code src/test/resources/mockito-extensions/org.mockito.plugins.MockMaker}), which mocks final
   * types by RETRANSFORMING the real class rather than by subclassing it. Several suite-mates call
   * {@code mock(RevisionRootPage.class)}, and from the moment the first of them runs, every real
   * {@code RevisionRootPage} in the JVM carries Mockito's interception prologue — including
   * {@code getRevision()}, which is where the revision guard above ends up. That prologue allocates
   * per call on any JIT that cannot scalar-replace it: 48 B per guard and so 96 B per comparison
   * under Temurin 25, byte-identical at 7 103 760 B over 72 000 probes on Linux, macOS and Windows; 0
   * B under GraalVM, whose partial escape analysis removes it; and 0 B under either when this class
   * runs on its own, because then nothing has retransformed anything.
   * </p>
   *
   * <p>
   * That is the harness rewriting bytecode underneath the meter, not the compare path allocating, and
   * an ABSOLUTE per-probe bound cannot tell the two apart — which is exactly how a bound calibrated
   * by running this class alone came to be red in the suite on every CI lane. So the floor is
   * MEASURED, on the same meter and through the same reader, and the two claims below are stated
   * relative to it. It is zero whenever the harness has instrumented nothing, so the bounds stay as
   * tight as they ever were.
   * </p>
   *
   * <p>
   * The probe count only has to make the per-call figure exact — the quantity is a fixed-size object
   * per call, not a distribution — and it must run AFTER the windows it calibrates: a hot loop over
   * the guard ahead of them re-shapes the JIT's inlining of that very guard, and then the floor being
   * measured is no longer the floor those windows paid.
   * </p>
   */
  private static final int GUARD_PROBES = 200_000;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());
    }
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  private static String valueOf(final int id) {
    return "value-" + id;
  }

  private static long build(final JsonResourceSession session) {
    try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"k\":\"v\"}"), JsonNodeTrx.Commit.NO);
      final GlobalValueDictionaryWriter dictionary = new GlobalValueDictionaryWriter();
      for (int i = 1; i <= ENTRIES; i++) {
        final byte[] utf8 = valueOf(i).getBytes(StandardCharsets.UTF_8);
        dictionary.intern(utf8, 0, utf8.length);
      }
      // Ordering adversaries in the same dictionary: U+10400's high surrogate sorts BEFORE U+FF01
      // under UTF-16 while unsigned UTF-8 order puts it after, and an over-long integer must decline
      // rather than wrap.
      for (final String extra : new String[] {"！", "𐐀", "92233720368547758070"}) {
        final byte[] utf8 = extra.getBytes(StandardCharsets.UTF_8);
        dictionary.intern(utf8, 0, utf8.length);
      }
      final var writer = wtx.getStorageEngineWriter();
      final NamePage namePage = writer.getNamePage(writer.getActualRevisionRootPage());
      final long headerKey = dictionary.flush(namePage, DatabaseType.JSON, writer, writer.getLog());
      wtx.commit();
      return headerKey;
    }
  }

  @Test
  @DisplayName("every id resolves across many reverse buckets, with ordering and overflow intact")
  void missPathResolvesEveryIdExactly() {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final long headerKey = build(session);
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final var reader = rtx.getStorageEngineReader();
        final GlobalValueDictionary.ReadView view = GlobalValueDictionary.readView(headerKey, reader);
        assertNotNull(view);
        assertEquals(ENTRIES + 3, view.entryCount());

        // Sweep every id: ENTRIES/256 reverse buckets, so this walks the radix repeatedly and
        // evicts the view cache continuously.
        for (int id = 1; id <= ENTRIES; id++) {
          assertEquals(valueOf(id), GlobalValueDictionary.value(headerKey, id, reader), "id " + id);
          final int neighbour = id == ENTRIES
              ? 1
              : id + 1;
          assertEquals(Integer.signum(valueOf(id).compareTo(valueOf(neighbour))),
              Integer.signum(view.compareIds(id, neighbour)), "ordering of " + id + " vs " + neighbour);
        }
        // Ordering follows UTF-16, not first-seen and not raw UTF-8.
        final int fullwidth = ENTRIES + 1;
        final int supplementary = ENTRIES + 2;
        assertEquals(Integer.signum("𐐀".compareTo("！")), Integer.signum(view.compareIds(supplementary, fullwidth)),
            "supplementary-plane ordering must follow UTF-16");
        // Overflow declines rather than wrapping.
        assertEquals(Long.MIN_VALUE, view.xsIntegerOfSubstring(ENTRIES + 3, 1, 20));
        // Range refusals, at both ends.
        assertThrows(IllegalStateException.class, () -> view.compareIds(0, 1));
        assertThrows(IllegalStateException.class, () -> view.compareIds(view.entryCount() + 1, 1));
      }
    }
  }

  @Test
  @DisplayName("a cold reopen resolves the same ids identically")
  void coldReopenResolvesIdentically() {
    final long headerKey;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      headerKey = build(session);
    }
    // Fresh database and session objects: nothing cached from the build survives.
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      final var reader = rtx.getStorageEngineReader();
      final GlobalValueDictionary.ReadView view = GlobalValueDictionary.readView(headerKey, reader);
      assertNotNull(view);
      assertEquals(ENTRIES + 3, view.entryCount());
      // Exercise the VIEW's own miss path, not the static materialiser: the retained buckets are a
      // view-scoped structure, so a cold reopen has to be observed through it to mean anything.
      for (int id = 1; id <= ENTRIES; id += 7) {

        // Ordering against a fixed neighbour, resolved entirely through the view.
        final int other = id == 1
            ? 2
            : 1;
        assertEquals(Integer.signum(valueOf(id).compareTo(valueOf(other))), Integer.signum(view.compareIds(id, other)),
            "cold ordering for id " + id);
      }
      // And the static materialiser agrees with the view on the same revision.
      for (int id = 1; id <= ENTRIES; id += 257) {
        assertEquals(valueOf(id), GlobalValueDictionary.value(headerKey, id, reader), "cold id " + id);
      }
    }
  }

  /**
   * Bytes ONE revision guard allocates on this JVM, right now, on the meter that measured the
   * windows.
   *
   * <p>
   * Probes {@link StorageEngineReader#getRevisionNumber()} and nothing else, so what it returns is
   * the guard and only the guard: it cannot quietly absorb an allocation belonging to the value
   * comparison and so cannot hollow out the assertion it calibrates. The caller pins that from the
   * other side too, by requiring the hot window to land ON the floor rather than merely under it.
   * </p>
   *
   * @return allocated bytes per {@code getRevisionNumber()} call; {@code 0.0} on an uninstrumented
   *         JVM
   */
  private static double revisionGuardBytes(final ThreadMXBean threads, final StorageEngineReader reader) {
    long sink = 0L;
    for (int probe = 0; probe < GUARD_PROBES; probe++) {
      sink += reader.getRevisionNumber();
    }
    final long before = threads.getCurrentThreadAllocatedBytes();
    for (int probe = 0; probe < GUARD_PROBES; probe++) {
      sink += reader.getRevisionNumber();
    }
    final long allocated = threads.getCurrentThreadAllocatedBytes() - before;
    assertTrue(sink > 0, "the guard probe must actually reach the reader, not be folded away");
    return (double) allocated / GUARD_PROBES;
  }

  @Test
  @DisplayName("the miss path no longer allocates its radix path and result records per probe")
  void missPathDoesNotAllocatePerProbe() {
    final ThreadMXBean threads = ManagementFactory.getPlatformMXBean(ThreadMXBean.class);
    Assumptions.assumeTrue(threads != null && threads.isThreadAllocatedMemorySupported(),
        "thread allocation accounting unavailable");

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final long headerKey = build(session);
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final GlobalValueDictionary.ReadView view =
            GlobalValueDictionary.readView(headerKey, rtx.getStorageEngineReader());
        assertNotNull(view);

        // DISTINCT ids throughout: compareIds short-circuits on id equality, so comparing an id
        // with itself resolves no bucket, no block and no slice — a first version of this test did
        // exactly that and reported a false 0.0 B/probe.
        // EXACT expected checksum, computed from String.compareTo before the measured window, so the
        // loop's result is pinned rather than merely "not a sentinel".
        long expected = 0L;
        for (int id = 1; id <= ENTRIES; id++) {
          expected += Integer.signum(valueOf(id).compareTo(valueOf(id == ENTRIES
              ? 1
              : id + 1)));
        }
        long checksum = 0L;
        for (int pass = 0; pass < 4; pass++) {
          for (int id = 1; id <= ENTRIES; id++) {
            checksum += Integer.signum(view.compareIds(id, id == ENTRIES
                ? 1
                : id + 1));
          }
        }

        final int passes = 6;
        final long before = threads.getCurrentThreadAllocatedBytes();
        for (int pass = 0; pass < passes; pass++) {
          for (int id = 1; id <= ENTRIES; id++) {
            checksum += Integer.signum(view.compareIds(id, id == ENTRIES
                ? 1
                : id + 1));
          }
        }
        final long allocated = threads.getCurrentThreadAllocatedBytes() - before;
        final long probes = (long) passes * ENTRIES;
        final double thrashPerProbe = (double) allocated / probes;
        assertEquals(expected * (4 + passes), checksum,
            "the comparisons must produce the interpreter's exact ordering, not merely run");

        // CONTROL on the SAME meter: the materialising path builds a String and a byte[] per id, so
        // it must dominate. Without this, a guard passing on an empty loop looks identical to one
        // passing on a genuinely allocation-free path.
        final long controlBefore = threads.getCurrentThreadAllocatedBytes();
        long controlSink = 0L;
        for (int id = 1; id <= ENTRIES; id++) {
          controlSink += GlobalValueDictionary.value(headerKey, id, rtx.getStorageEngineReader()).length();
        }
        final double controlPerProbe = (double) (threads.getCurrentThreadAllocatedBytes() - controlBefore) / ENTRIES;
        assertTrue(controlSink > 0);

        // FULLY HOT window: a working set inside both caches, so no bucket or sub-block decode can
        // hide a regression. The thrash bound is ~40 B/probe and would happily admit a 16-byte
        // per-compare wrapper; this one would not. It doubles as the thrash window's baseline, so it
        // must be driven through the SAME call in the SAME loop shape, and nothing may run between
        // the two that re-shapes how the JIT compiles them.
        final int hotIds = 200;
        for (int pass = 0; pass < 4; pass++) {
          for (int id = 1; id <= hotIds; id++) {
            checksum += Integer.signum(view.compareIds(id, id == hotIds
                ? 1
                : id + 1));
          }
        }
        final long hotBefore = threads.getCurrentThreadAllocatedBytes();
        long hotChecksum = 0L;
        for (int pass = 0; pass < passes; pass++) {
          for (int id = 1; id <= hotIds; id++) {
            hotChecksum += Integer.signum(view.compareIds(id, id == hotIds
                ? 1
                : id + 1));
          }
        }
        final long hotAllocated = threads.getCurrentThreadAllocatedBytes() - hotBefore;
        final double hotPerProbe = (double) hotAllocated / ((long) passes * hotIds);
        long hotExpected = 0L;
        for (int id = 1; id <= hotIds; id++) {
          hotExpected += Integer.signum(valueOf(id).compareTo(valueOf(id == hotIds
              ? 1
              : id + 1)));
        }
        assertEquals(hotExpected * passes, hotChecksum, "hot window must compute the same exact ordering");

        // LAST, for the reason GUARD_PROBES gives: calibrate the floor on the state the windows ran
        // in, never the other way round.
        final double guardBytes = revisionGuardBytes(threads, rtx.getStorageEngineReader());
        final double compareFloor = GUARDS_PER_COMPARE * guardBytes;
        final double missPerProbe = thrashPerProbe - hotPerProbe;
        final double hotBeyondGuards = hotPerProbe - compareFloor;

        System.out.println("[readview-miss] " + allocated + " bytes over " + probes + " probes = " + thrashPerProbe
            + " B/probe, " + missPerProbe + " B/probe net of the hot window; materialising control = " + controlPerProbe
            + " B/probe");
        System.out.println("[readview-hot] " + hotAllocated + " bytes = " + hotPerProbe + " B/probe, " + hotBeyondGuards
            + " B/probe beyond a measured " + compareFloor + " B/compare guard floor (" + guardBytes + " B/guard)");

        assertTrue(missPerProbe < SLICE_PATH_MAX_BYTES_PER_PROBE,
            "the miss path allocated " + missPerProbe + " B/probe beyond a hot comparison (" + allocated
                + " bytes over " + probes + " probes against " + hotPerProbe
                + " B/probe hot); its radix path and result records are back");
        assertTrue(controlPerProbe > SLICE_PATH_MAX_BYTES_PER_PROBE,
            "the materialising control (" + controlPerProbe + " B/probe) must sit above the slice-path bound ("
                + SLICE_PATH_MAX_BYTES_PER_PROBE + " B/probe), or this meter cannot see an allocation at all");
        assertTrue(hotBeyondGuards < 1.0, "fully-hot comparison allocated " + hotPerProbe + " B/probe against a "
            + compareFloor + " B/compare guard floor; a per-compare wrapper is back");
        // And from the other side: a floor ABOVE what a whole hot comparison costs would subtract a
        // regression away instead of the harness, leaving the assertion above true of anything.
        assertTrue(hotBeyondGuards > -1.0,
            "the measured guard floor (" + compareFloor + " B/compare) exceeds a whole hot comparison (" + hotPerProbe
                + " B/probe), so it is not calibrating this meter, it is hollowing it out");
      }
    }
  }
}
