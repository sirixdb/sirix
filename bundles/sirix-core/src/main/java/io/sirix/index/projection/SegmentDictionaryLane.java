/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.StorageEngineWriter;
import io.sirix.index.projection.ProjectionIndexMetadata.SegmentAnchor;
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
 * {@link SegmentSealController} (when a segment is done), {@link SegmentDictionaryFlusher} (write
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
 * does not ask for it behaves exactly as before and every page keeps its bytes.
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class SegmentDictionaryLane {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentDictionaryLane.class);

  /** Arms the lane. Off by default: it gates BEHAVIOUR, never a decoder. */
  public static final String ENABLED_PROPERTY = "sirix.projection.segmentDict";

  /** Per-segment admission budget for one column's values; a segment is small by construction. */
  private static final long SEGMENT_VALUE_BUDGET_BYTES = 256L << 20;

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
    final SegmentDictionaryLane lane = new SegmentDictionaryLane(columns);
    storageEngineWriter.installDocumentStringDictionaryFactory(lane::adoptPage);
    storageEngineWriter.installDocumentPageEncodedListener(lane::pageEncoded);
    return lane;
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
    final List<SegmentAnchor> sealed = new ArrayList<>();
    final IntList segments = sealController.drainAfterFence();
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
        final long headerKey = SegmentDictionaryFlusher.write(storageEngineWriter, column,
            Arrays.asList(values).iterator(), SEGMENT_VALUE_BUDGET_BYTES);
        if (headerKey == SegmentDictionaryAnchors.NO_HEADER_KEY) {
          // The flusher writes nothing only for an empty stream, and this stream has values: a silent
          // skip here would leave every page of the segment with ids that resolve to nothing.
          throw new IllegalStateException("segment " + segment + " column " + column + " has " + values.length
              + " values but its dictionary was not written");
        }
        anchors.seal(segment, column, headerKey, values.length);
        sealed.add(new SegmentAnchor(segment, column, headerKey, values.length));
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
    LOGGER.debug("segment dictionary lane sealed {} (segment, column) dictionaries", sealed.size());
    return sealed.toArray(new SegmentAnchor[0]);
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
    }
  }

  /** Sealed {@code (segment, column)} dictionaries (test and diagnostic observability). */
  public int sealedCount() {
    return anchors.sealedCount();
  }
}
