/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.StorageEngineWriter;
import io.sirix.index.projection.ProjectionIndexMetadata.SegmentAnchor;
import io.sirix.access.DatabaseType;
import io.sirix.node.SegmentDictionaryDirectoryNode;
import io.sirix.page.ChunkedBodyConfig;
import io.sirix.page.NamePage;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * The segment-scoped dictionary lane, assembled: mint while the load runs, seal when the pages are
 * encoded, persist the anchors, and leave a database that needs no pre-pass to have been built.
 *
 * <p>
 * This is the orchestration {@code docs/SEGMENT_SCOPED_DICTIONARIES.md} describes, over five pieces
 * that are each tested on their own: {@link SegmentScopedDictionaries} (mint, per-PAGE views),
 * {@link SegmentSealController} (when a segment is done), {@link SegmentDictionarySeal} (write
 * one sealed segment through the load's own writer), {@link SegmentDictionaryAnchors} (where each
 * sealed dictionary lives) and the metadata section that persists the last of those. The lane
 * exists so a caller wires three call sites rather than five objects.
 * </p>
 *
 * <h2>Why sealing happens at the DRAIN and not from the encode listener</h2>
 *
 * The listener fires from the sequential pass that follows a flush window's join — the writer's own
 * critical section, mid-append. Writing a dictionary there would interleave a fresh page-allocating
 * write with the append pass that is walking the window. So the listener only does bookkeeping, and
 * every segment is sealed at {@link #sealAll}, which the caller invokes once every adopted page has
 * been encoded. {@link SegmentSealController#takeSealable} is therefore not consulted here; it
 * exists for the incremental regime a 100M load needs, where holding every segment's values to the
 * end is not affordable, and it is left switched off until this shape is measured.
 *
 * <h2>Why a sealed segment is released at once</h2>
 *
 * A page encoded AFTER its segment's seal would mint values the persisted dictionary does not hold,
 * and the page would carry ids nothing can resolve. {@link #sealAll} therefore releases each
 * segment's mint maps the moment its dictionary is written: a late mint is refused loudly by
 * {@link SegmentScopedDictionaries} instead of producing an unreadable page, and the values are no
 * longer held in memory a second time.
 *
 * <h2>Where a segment ends</h2>
 *
 * {@link SegmentBoundaries}, at adoption: the open segment closes once the distinct-value bytes
 * minted into it reach its budget or it spans its leaf cap, and the page being adopted opens the
 * next one. Two constants, no properties.
 *
 * <h2>Kill switch</h2>
 *
 * {@code -Dsirix.projection.segmentDict=true} arms the lane; it is OFF by default, so a load that
 * does not ask for it behaves exactly as before and every page keeps its bytes. Arming it without
 * chunk-framed bodies ({@code -Dsirix.chunkedBody.enable=true}) is refused rather than obeyed: the
 * pages would be written unreadable.
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class SegmentDictionaryLane {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentDictionaryLane.class);

  /** Arms the lane. Off by default: it gates BEHAVIOUR, never a decoder. */
  public static final String ENABLED_PROPERTY = "sirix.projection.segmentDict";

  private final SegmentScopedDictionaries dictionaries;

  private final SegmentSealController sealController = new SegmentSealController();

  private final SegmentDictionaryAnchors anchors = new SegmentDictionaryAnchors();

  private final int columns;

  private SegmentDictionaryLane(final int columns) {
    this(columns, new SegmentBoundaries());
  }

  /** For tests that need to place segment boundaries; production uses the default boundaries. */
  SegmentDictionaryLane(final int columns, final SegmentBoundaries boundaries) {
    if (columns < 0) {
      throw new IllegalArgumentException("columns must not be negative: " + columns);
    }
    this.dictionaries = new SegmentScopedDictionaries(boundaries, SegmentScopedDictionaries.noTags());
    this.columns = columns;
  }

  /** Whether the lane is armed. */
  public static boolean enabled() {
    return Boolean.getBoolean(ENABLED_PROPERTY);
  }

  /**
   * Arm the lane against {@code storageEngineWriter}, or return {@code null} when it is switched off.
   * Installs the per-page resolver factory and the encode-completion listener; the caller hands the
   * dictionaries to the builder so tag publication reaches them.
   */
  public static @Nullable SegmentDictionaryLane bind(final StorageEngineWriter storageEngineWriter, final int columns) {
    if (!enabled()) {
      return null;
    }
    requireNonNull(storageEngineWriter, "storageEngineWriter must not be null");
    if (!ChunkedBodyConfig.enabled()) {
      // A page whose values are dictionary ids can only be expanded where a reader is reachable, and
      // that is the LAZY route; the lazy route needs a chunk-framed body, and without one the reader
      // falls back to eager expansion and refuses the page outright. Converting under a monolithic
      // body writes pages nothing can read back — so the lane refuses to arm instead, here, where the
      // load has not written anything yet.
      throw new IllegalStateException("the segment dictionary lane needs chunk-framed record-page bodies"
          + " (-Dsirix.chunkedBody.enable=true): a converted page is readable only on the lazy route,"
          + " which a monolithic body cannot serve");
    }
    reserveDirectoryKeys(storageEngineWriter);
    return install(storageEngineWriter, columns, new SegmentBoundaries());
  }

  /**
   * The wiring half of {@link #bind}: hand the writer the two seams. Separated so a test can drive
   * the wiring without a resource behind it — {@link #bind} additionally decides whether the lane may
   * arm at all and reserves the directory's keys, and both of those need a real writer.
   */
  static SegmentDictionaryLane install(final StorageEngineWriter storageEngineWriter, final int columns,
      final SegmentBoundaries boundaries) {
    requireNonNull(storageEngineWriter, "storageEngineWriter must not be null");
    final SegmentDictionaryLane lane = new SegmentDictionaryLane(columns, boundaries);
    storageEngineWriter.installDocumentStringDictionaryFactory(lane::adoptPage);
    storageEngineWriter.installDocumentPageEncodedListener(lane::pageEncoded);
    // The load must be able to read back what it has already written, before any of it is sealed.
    storageEngineWriter.installLiveDocumentStringReadView(new SegmentLaneReadView(lane.dictionaries));
    return lane;
  }

  /**
   * Take the directory's key range out of circulation, once, before anything can be written.
   *
   * <p>
   * The directory lives at the fixed key {@link SegmentDictionaryDirectoryNode#DIRECTORY_KEY}, which
   * a reader looks up without being told where it is. Reserving the whole first record page keeps it
   * alone there — its record is rewritten at every commit, and sharing a page with dictionary entries
   * would copy those entries on every rewrite — and, more simply, stops the first dictionary the seal
   * writes from landing on the directory's own key.
   * </p>
   *
   * @throws IllegalStateException if the sub-trie has already handed out keys, so the directory's key
   *         is not the lane's to take: this resource's dictionaries were written by another lane, and
   *         a directory written over them would name records that mean something else
   */
  private static void reserveDirectoryKeys(final StorageEngineWriter storageEngineWriter) {
    final NamePage namePage = storageEngineWriter.getNamePage(storageEngineWriter.getActualRevisionRootPage());
    final DatabaseType databaseType = GlobalValueDictionary.databaseTypeOf(storageEngineWriter);
    namePage.createProjectionValueDictionaryTree(databaseType, storageEngineWriter, storageEngineWriter.getLog());
    final long first =
        namePage.reserveProjectionValueDictionaryKeys(databaseType, SegmentDictionaryDirectoryNode.RESERVED_KEYS);
    if (first != SegmentDictionaryDirectoryNode.DIRECTORY_KEY) {
      throw new IllegalStateException("the segment dictionary directory needs key "
          + SegmentDictionaryDirectoryNode.DIRECTORY_KEY + ", but this resource's value dictionary already reaches "
          + (first - 1) + "; one lane per resource writes the directory");
    }
  }

  /**
   * The factory's answer for a page: adopt it and hand back its view. The boundary decision comes
   * first — adopting may CLOSE the open segment and place this page in the next one — and the seal
   * controller then counts the page where the page's view says it is.
   */
  SegmentScopedDictionaries.SegmentView adoptPage(final long recordPageKey) {
    final SegmentScopedDictionaries.SegmentView view = dictionaries.adopt(recordPageKey);
    sealController.adopted(view.segment(), recordPageKey);
    return view;
  }

  /** The listener's notification: the page's bytes are produced, its values are minted. */
  void pageEncoded(final long recordPageKey) {
    sealController.encoded(dictionaries.segmentOf(recordPageKey), recordPageKey);
  }

  /** The dictionaries the builder must publish its tag map to. */
  public SegmentScopedDictionaries dictionaries() {
    return dictionaries;
  }

  /**
   * Seal every segment: write each one's values as a dictionary through the load's own writer,
   * record where it went, and release its mint maps. Call once every adopted page has been encoded:
   * {@link SegmentSealController#drainAfterFence} verifies that from the pages' own notifications
   * and refuses a segment with a page still outstanding, because a page encoded after the seal would
   * mint into a dictionary nothing will persist.
   *
   * @return the anchors to persist, empty when the lane minted nothing
   * @throws IllegalStateException if a page was adopted but not encoded, if a segment with values
   *         produced no dictionary, or if a segment minted a dictionary this seal does not cover
   */
  public SegmentAnchor[] sealAll(final StorageEngineWriter storageEngineWriter) {
    requireNonNull(storageEngineWriter, "storageEngineWriter must not be null");
    final List<SegmentAnchor> tail = sealSegments(storageEngineWriter, sealController.drainAfterFence());
    // EVERY anchor, not just this pass's. The incremental seals wrote their dictionaries during
    // earlier commits and recorded them here; returning only the tail would publish metadata naming
    // a handful of segments and leave every page of the rest resolving against nothing.
    final List<SegmentAnchor> sealed = new ArrayList<>(allSealed);
    sealed.addAll(tail);
    if (!sealed.isEmpty()) {
      writeDirectory(storageEngineWriter);
    }
    if (SEAL_DIAG) {
      // What the lane was HOLDING. The heap cost of the whole design in one number, and the only way
      // to price the 100M shape without paying half an hour for it.
      long entries = 0;
      for (final SegmentAnchor anchor : sealed) {
        entries += anchor.sealedEntryCount();
      }
      System.err.println("[seal] final: dictionaries=" + sealed.size() + " entries=" + entries + " heldValueBytes="
          + heldValueBytes + " heapUsedMB="
          + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024 * 1024));
    }
    LOGGER.debug("segment dictionary lane sealed {} (segment, column) dictionaries ({} incrementally)", sealed.size(),
        allSealed.size());
    return sealed.toArray(new SegmentAnchor[0]);
  }

  /**
   * Seal every segment the writer has FINISHED with, and release what they held.
   *
   * <p>
   * The incremental regime a 100M load needs. Holding every segment's values to the end costs one
   * budget per segment — about 64 MiB of value bytes plus its hash maps — and at a hundred segments
   * that is the whole heap: measured, a 100M load died with {@code OutOfMemoryError} at 33 GB
   * written with roughly forty-five segments live. A segment below the high-water mark can take no
   * further page (the boundaries never go back), so its dictionary is already final; writing it here
   * and dropping the maps turns an O(segments) heap cost into an O(1) one.
   * </p>
   *
   * <p>
   * Call at the COMMIT SEAM and nowhere else, for the reason {@link #sealAll} gives: a page encoded
   * after its segment's seal would mint into a dictionary nothing will persist. One segment of slack
   * covers the consumer that derives from the same rows and mints a moment later — see
   * {@link SegmentSealController#takeSealable(int)}.
   * </p>
   *
   * @return the anchors sealed by this pass, empty when nothing was ready
   */
  public SegmentAnchor[] sealCompleted(final StorageEngineWriter storageEngineWriter) {
    requireNonNull(storageEngineWriter, "storageEngineWriter must not be null");
    final IntList ready = sealController.takeSealable(SEAL_SLACK_SEGMENTS);
    if (ready.isEmpty()) {
      if (SEAL_DIAG) {
        final int high = sealController.highWaterMark();
        final StringBuilder outstanding = new StringBuilder();
        for (int segment = 0; segment <= high && segment < 8; segment++) {
          outstanding.append(segment == 0
              ? ""
              : " ").append('s').append(segment).append('=').append(sealController.outstandingIn(segment));
        }
        System.err.println("[seal] nothing sealable: highWater=" + high + " sealed=" + sealController.sealedCount()
            + " outstanding[" + outstanding + "]");
      }
      return NO_ANCHORS;
    }
    final List<SegmentAnchor> sealed = sealSegments(storageEngineWriter, ready);
    allSealed.addAll(sealed);
    if (SEAL_DIAG) {
      System.err.println("[seal] incremental: segments=" + ready + " dictionaries=" + sealed.size() + " total="
          + allSealed.size() + " heapUsedMB="
          + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024 * 1024));
    }
    // NOT the directory: it names every segment's first page and is written once, by the seal that
    // finishes the load. Rewriting it per commit would persist a table that is still growing.
    LOGGER.debug("segment dictionary lane sealed {} (segment, column) dictionaries incrementally", sealed.size());
    return sealed.toArray(new SegmentAnchor[0]);
  }

  /** Reports what each incremental pass sealed and the heap after it; {@code -Dsirix.projDiag}. */
  private static final boolean SEAL_DIAG = Boolean.getBoolean("sirix.projDiag");

  /** Segments kept live below the high-water mark; see {@link SegmentSealController#takeSealable(int)}. */
  private static final int SEAL_SLACK_SEGMENTS = 1;

  private static final SegmentAnchor[] NO_ANCHORS = new SegmentAnchor[0];

  /** Value bytes the lane held, summed as the seal reads them; diagnostics only. */
  private long heldValueBytes;

  /** Anchors sealed by the incremental passes; {@link #sealAll} publishes these beside its own. */
  private final List<SegmentAnchor> allSealed = new ArrayList<>();

  private List<SegmentAnchor> sealSegments(final StorageEngineWriter storageEngineWriter, final IntList segments) {
    final List<SegmentAnchor> sealed = new ArrayList<>();
    for (int i = 0; i < segments.size(); i++) {
      final int segment = segments.getInt(i);
      int written = 0;
      for (int column = 0; column < columns; column++) {
        // The values FIRST, and the count from the array that is actually written: reading the count
        // separately would record a bound that a mint between the two reads has already outgrown, and
        // the reader refuses a page whose recorded count exceeds the sealed one.
        final byte[][] values = dictionaries.valuesById(segment, column);
        if (values.length == 0) {
          continue;
        }
        if (SEAL_DIAG) {
          for (final byte[] value : values) {
            heldValueBytes += value.length;
          }
        }
        // The values are stored in COLLATION order with a rank table translating the mints the pages
        // carry; the ids stay arrival-order mints, which is what makes them permanent.
        final SegmentDictionarySeal.Sealed dictionary =
            SegmentDictionarySeal.write(storageEngineWriter, column, values);
        if (dictionary.wroteNothing()) {
          // The seal writes nothing only for an empty array, and this one has values: a silent skip
          // here would leave every page of the segment with ids that resolve to nothing.
          throw new IllegalStateException("segment " + segment + " column " + column + " has " + values.length
              + " values but its dictionary was not written");
        }
        anchors.seal(segment, column, dictionary.headerKey(), dictionary.entryCount());
        sealed.add(new SegmentAnchor(segment, column, dictionary.headerKey(), dictionary.entryCount()));
        written++;
      }
      // Every dictionary the segment minted must be one this loop wrote. A dictionary at a column
      // beyond `columns` would be dropped by the release below with no anchor naming it, and every
      // page that stamped ids against it would be unreadable — data lost, not merely unconverted.
      final int minted = dictionaries.dictionaryCount(segment);
      if (written != minted) {
        throw new IllegalStateException("segment " + segment + " minted " + minted + " dictionaries but the seal wrote "
            + written + " over " + columns + " column(s); a minted dictionary would be released unwritten");
      }
      dictionaries.release(segment);
    }
    return sealed;
  }

  /**
   * Write the directory at its fixed key: where every segment begins, and which dictionary serves
   * which TAG inside it.
   *
   * <p>
   * The anchors travel in the index metadata as well, but a reader that has to resolve a page's ids
   * before it knows which projection index it is looking at cannot get there. The directory is that
   * reader's entry point: one record, at a key it does not have to be told, naming the same
   * dictionaries by the page's own tag.
   * </p>
   */
  private void writeDirectory(final StorageEngineWriter storageEngineWriter) {
    final long[] starts = dictionaries.boundaries().starts();
    final Int2IntMap columnByTag = dictionaries.tags();
    final SegmentDictionaryDirectoryNode.SlotTable[] tables =
        new SegmentDictionaryDirectoryNode.SlotTable[starts.length];
    for (int segment = 0; segment < starts.length; segment++) {
      tables[segment] = slotTableFor(segment, columnByTag);
    }
    final NamePage namePage = storageEngineWriter.getNamePage(storageEngineWriter.getActualRevisionRootPage());
    final DatabaseType databaseType = GlobalValueDictionary.databaseTypeOf(storageEngineWriter);
    namePage.putProjectionValueDictionaryRecord(
        SegmentDictionaryDirectoryNode.takeOwnership(SegmentDictionaryDirectoryNode.DIRECTORY_KEY, starts, tables),
        databaseType, storageEngineWriter, storageEngineWriter.getLog());
  }

  /**
   * One segment's slot table: a slot per column that sealed a dictionary in it, carrying the tags
   * that resolve to that column. A column with no dictionary in this segment contributes no slot, so
   * a segment nothing was sealed in files as {@link SegmentDictionaryDirectoryNode.SlotTable#EMPTY}.
   */
  SegmentDictionaryDirectoryNode.SlotTable slotTableFor(final int segment, final Int2IntMap columnByTag) {
    final IntArrayList slotColumns = new IntArrayList();
    for (int column = 0; column < columns; column++) {
      if (anchors.headerKeyOf(segment, column) != SegmentDictionaryAnchors.NO_HEADER_KEY) {
        slotColumns.add(column);
      }
    }
    if (slotColumns.isEmpty()) {
      return SegmentDictionaryDirectoryNode.SlotTable.EMPTY;
    }
    final int[][] tagsBySlot = new int[slotColumns.size()][];
    final long[] headerKeys = new long[slotColumns.size()];
    final int[] entryCounts = new int[slotColumns.size()];
    for (int slot = 0; slot < slotColumns.size(); slot++) {
      final int column = slotColumns.getInt(slot);
      final IntArrayList tags = new IntArrayList();
      for (final Int2IntMap.Entry entry : columnByTag.int2IntEntrySet()) {
        if (entry.getIntValue() == column) {
          tags.add(entry.getIntKey());
        }
      }
      // The node's contract, and the reader's binary search: strictly ascending.
      final int[] tagArray = tags.toIntArray();
      Arrays.sort(tagArray);
      tagsBySlot[slot] = tagArray;
      headerKeys[slot] = anchors.headerKeyOf(segment, column);
      entryCounts[slot] = anchors.sealedEntryCountOf(segment, column);
    }
    return SegmentDictionaryDirectoryNode.SlotTable.takeOwnership(tagsBySlot, headerKeys, entryCounts);
  }

  /**
   * Stop handing the resolver to new pages and stop counting encodes. Uninstall FIRST, then let the
   * caller drain: a page created after this carries no resolver and converts nothing, which is the
   * same ordering the trie lane's release uses and for the same reason.
   */
  public void release(final @Nullable StorageEngineWriter storageEngineWriter) {
    if (storageEngineWriter != null) {
      storageEngineWriter.installDocumentStringDictionaryFactory(null);
      storageEngineWriter.installDocumentPageEncodedListener(null);
      storageEngineWriter.installLiveDocumentStringReadView(null);
    }
  }

  /** Where each sealed dictionary went (test observability). */
  SegmentDictionaryAnchors anchors() {
    return anchors;
  }

  /** Sealed {@code (segment, column)} dictionaries (test and diagnostic observability). */
  public int sealedCount() {
    return anchors.sealedCount();
  }
}
