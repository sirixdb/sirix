/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.StorageEngineWriter;
import io.sirix.index.projection.ProjectionIndexMetadata.SegmentAnchor;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * The segment-scoped dictionary lane, assembled: mint while the load runs, seal when the pages are
 * encoded, persist the anchors, and leave a database that needs no pre-pass to have been built.
 *
 * <p>
 * This is the orchestration {@code docs/SEGMENT_SCOPED_DICTIONARIES.md} describes, over five pieces
 * that are each tested on their own: {@link SegmentScopedDictionaries} (mint, per-PAGE views),
 * {@link SegmentSealController} (when a segment is done), {@link SegmentDictionaryFlusher} (write one
 * sealed segment through the load's own writer), {@link SegmentDictionaryAnchors} (where each sealed
 * dictionary lives) and the metadata section that persists the last of those. The lane exists so a
 * caller wires three call sites rather than five objects.
 * </p>
 *
 * <h2>Why sealing happens at the DRAIN and not from the encode listener</h2>
 *
 * The listener fires from the sequential pass that follows a flush window's join — the writer's own
 * critical section, mid-append. Writing a dictionary there would interleave a fresh page-allocating
 * write with the append pass that is walking the window. So the listener only does bookkeeping, and
 * every segment is sealed at {@link #sealAll}, which the caller invokes once the flush pool has been
 * fenced. {@link SegmentSealController#takeSealable} is therefore not consulted here; it exists for
 * the incremental regime a 100M load needs, where holding every segment's values to the end is not
 * affordable, and it is left switched off until this shape is measured.
 *
 * <h2>Kill switch</h2>
 *
 * {@code -Dsirix.projection.segmentDict=true} arms the lane; it is OFF by default, so a load that does
 * not ask for it behaves exactly as before and every page keeps its bytes.
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class SegmentDictionaryLane {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentDictionaryLane.class);

  /** Arms the lane. Off by default: it gates BEHAVIOUR, never a decoder. */
  public static final String ENABLED_PROPERTY = "sirix.projection.segmentDict";

  /** Record pages per segment; the page trie groups 1024 leaves under one indirect page. */
  public static final String LEAVES_PER_SEGMENT_PROPERTY = "sirix.projection.segmentDict.leaves";

  private static final long DEFAULT_LEAVES_PER_SEGMENT = 1024L;

  /** Per-segment admission budget for one column's values; a segment is small by construction. */
  private static final long SEGMENT_VALUE_BUDGET_BYTES = 256L << 20;

  private final SegmentScopedDictionaries dictionaries;

  private final SegmentSealController sealController = new SegmentSealController();

  private final SegmentDictionaryAnchors anchors = new SegmentDictionaryAnchors();

  private final int columns;

  private SegmentDictionaryLane(final long leavesPerSegment, final int columns) {
    this.dictionaries = new SegmentScopedDictionaries(leavesPerSegment, SegmentScopedDictionaries.noTags());
    this.columns = columns;
  }

  /** Whether the lane is armed. */
  public static boolean enabled() {
    return Boolean.getBoolean(ENABLED_PROPERTY);
  }

  private static long leavesPerSegment() {
    final long configured = Long.getLong(LEAVES_PER_SEGMENT_PROPERTY, DEFAULT_LEAVES_PER_SEGMENT);
    if (configured <= 0 || Long.bitCount(configured) != 1) {
      throw new IllegalArgumentException(LEAVES_PER_SEGMENT_PROPERTY + " must be a positive power of two: "
          + configured);
    }
    return configured;
  }

  /**
   * Arm the lane against {@code storageEngineWriter}, or return {@code null} when it is switched off.
   * Installs the per-page resolver factory and the encode-completion listener; the caller hands the
   * dictionaries to the builder so tag publication reaches them.
   */
  public static @Nullable SegmentDictionaryLane bind(final StorageEngineWriter storageEngineWriter,
      final int columns) {
    if (!enabled()) {
      return null;
    }
    requireNonNull(storageEngineWriter, "storageEngineWriter must not be null");
    final SegmentDictionaryLane lane = new SegmentDictionaryLane(leavesPerSegment(), columns);
    storageEngineWriter.installDocumentStringDictionaryFactory(recordPageKey -> {
      lane.sealController.adopted(lane.dictionaries.segmentOf(recordPageKey));
      return lane.dictionaries.viewFor(recordPageKey);
    });
    storageEngineWriter.installDocumentPageEncodedListener(
        recordPageKey -> lane.sealController.encoded(lane.dictionaries.segmentOf(recordPageKey)));
    return lane;
  }

  /** The dictionaries the builder must publish its tag map to. */
  public SegmentScopedDictionaries dictionaries() {
    return dictionaries;
  }

  /**
   * Seal every segment: write each one's values as a dictionary through the load's own writer and
   * record where it went. Call once the flush pool has been fenced — {@link SegmentSealController#drain}
   * refuses while any page is still encoding, because sealing then would drop the values that page is
   * about to mint.
   *
   * @return the anchors to persist, empty when the lane minted nothing
   */
  public SegmentAnchor[] sealAll(final StorageEngineWriter storageEngineWriter) {
    requireNonNull(storageEngineWriter, "storageEngineWriter must not be null");
    final List<SegmentAnchor> sealed = new ArrayList<>();
    for (final long segment : sealController.drain()) {
      for (int column = 0; column < columns; column++) {
        final int entryCount = dictionaries.entryCount(segment, column);
        if (entryCount == 0) {
          continue;
        }
        final long headerKey = SegmentDictionaryFlusher.write(storageEngineWriter, column,
            dictionaries.valuesOf(segment, column), SEGMENT_VALUE_BUDGET_BYTES);
        if (headerKey == SegmentDictionaryAnchors.NO_HEADER_KEY) {
          continue;
        }
        anchors.seal(segment, column, headerKey, entryCount);
        sealed.add(new SegmentAnchor(segment, column, headerKey, entryCount));
      }
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
