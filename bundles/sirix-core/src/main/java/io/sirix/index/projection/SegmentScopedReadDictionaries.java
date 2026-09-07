/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.StorageEngineReader;
import io.sirix.node.SegmentDictionaryDirectoryNode;
import io.sirix.page.pax.GlobalStringDictionaries;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * DECODE-direction resolver for pages whose dictionaries are scoped to a SEGMENT.
 *
 * <p>
 * The twin of {@link SegmentScopedDictionaries}, and the difference from
 * {@link TrieLaneDictionaries} is one indirection: a trie-lane page's anchor IS the dictionary's
 * header key, so that resolver hands it straight to {@link GlobalValueDictionary#valueBytes}. A
 * segment-scoped page's anchor is its SEGMENT, because at page-encode time the segment's dictionary
 * had not been written and had no key yet, so this resolver translates through the
 * {@link SegmentDictionaryDirectoryNode} first.
 * </p>
 *
 * <h2>Why the directory and not the index metadata</h2>
 *
 * Everything this resolver needs is in ONE record, at a key it does not have to be told: the page's
 * own tag names a slot, the slot names the dictionary. The alternative — the projection index's
 * metadata blob plus the path summary — makes the reader ask which index it is looking at before it
 * can resolve a page's values, which it cannot know from the page; needs a path-summary walk per
 * transaction to turn field paths back into tags; and keys dictionaries by an index's OWN column
 * numbering, so two projection indexes on one resource name different dictionaries by the same
 * {@code (segment, column)} and a page records nothing that tells them apart. The directory is
 * keyed by tag, which is what the page carries.
 *
 *
 * <h2>The two refusals, and why both are needed</h2>
 *
 * <ul>
 * <li><b>Unsealed segment.</b> A page can outlive the crash that stopped its segment being sealed —
 * the pages are durable, the dictionary is not. Its ids resolve against nothing, so the page keeps
 * whatever the caller falls back to; it must never resolve against a DIFFERENT segment's
 * dictionary, which is what an anchor table lookup returning "some key" would do.</li>
 * <li><b>An id past what the page saw.</b> Identical in spirit to the trie lane's rule: an id above
 * the count the page recorded is one the page could not have written. Refusing it is what stops a
 * later, larger dictionary answering from a part the page never saw. For a segment dictionary this
 * is belt and braces — a sealed segment never grows again — but the rule costs nothing and the
 * invariant it depends on is the pipeline's, not this class's.</li>
 * </ul>
 *
 * <p>
 * One instance belongs to one transaction and does not outlive it: it holds a
 * {@link StorageEngineReader}. A page or a cache may hold the VALUE this produces, never this
 * object.
 * </p>
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class SegmentScopedReadDictionaries implements GlobalStringDictionaries {

  /**
   * How a resolved id becomes bytes. Exists so the translation and refusal rules can be tested
   * without a committed dictionary; production passes {@link GlobalValueDictionary#valueBytes}.
   */
  @FunctionalInterface
  public interface ValueReader {
    byte @Nullable [] read(long headerKey, int id, StorageEngineReader reader);
  }

  private final StorageEngineReader reader;

  private final SegmentDictionaryDirectoryNode directory;

  /** Every tag the directory covers in any segment; the answer {@link #hasDictionary} needs. */
  private final IntSet coveredTags;

  private final ValueReader values;

  public SegmentScopedReadDictionaries(final StorageEngineReader reader,
      final SegmentDictionaryDirectoryNode directory) {
    this(reader, directory, GlobalValueDictionary::valueBytes);
  }

  SegmentScopedReadDictionaries(final StorageEngineReader reader, final SegmentDictionaryDirectoryNode directory,
      final ValueReader values) {
    this.reader = reader;
    this.directory = requireNonNull(directory, "directory must not be null");
    this.values = requireNonNull(values, "values must not be null");
    this.coveredTags = coveredTagsOf(directory);
  }

  /**
   * The union of every segment's tags. A tag is asked about without a segment — the encoder asks once
   * per tag per page, before the page's anchor is read — so the union is the only honest answer, and
   * a tag covered in one segment but not another still resolves correctly because {@link #accepts}
   * looks the tag up in the page's OWN segment.
   */
  private static IntSet coveredTagsOf(final SegmentDictionaryDirectoryNode directory) {
    final IntOpenHashSet tags = new IntOpenHashSet();
    for (int segment = 0; segment < directory.segmentCount(); segment++) {
      final SegmentDictionaryDirectoryNode.SlotTable table = directory.slots(segment);
      for (int slot = 0; slot < table.slotCount(); slot++) {
        for (final int tag : table.tags(slot)) {
          tags.add(tag);
        }
      }
    }
    return tags;
  }

  @Override
  public boolean hasDictionary(final int tag) {
    return coveredTags.contains(tag);
  }

  @Override
  public boolean accepts(final int tag, final long dictionaryKey, final int recordedEntryCount) {
    return slotFor(tag, dictionaryKey, recordedEntryCount) >= 0;
  }

  @Override
  public int idOf(final int tag, final byte[] value, final int offset, final int length) {
    return ID_ABSENT; // decode-direction only; minting is the writer's job
  }

  @Override
  public byte @Nullable [] valueOf(final int tag, final long dictionaryKey, final int recordedEntryCount,
      final int id) {
    if (id <= 0 || id > recordedEntryCount) {
      return null;
    }
    final int slot = slotFor(tag, dictionaryKey, recordedEntryCount);
    if (slot < 0) {
      return null;
    }
    final long headerKey = directory.headerKey((int) segmentOf(dictionaryKey), slot);
    if (headerKey == SegmentDictionaryAnchors.NO_HEADER_KEY) {
      return null;
    }
    return values.read(headerKey, id, reader);
  }

  /**
   * The slot serving {@code tag} in the segment the page anchors to, or {@code -1} when the page
   * cannot be resolved: an anchor outside the directory (a page whose segment was never sealed — the
   * pages are durable, the dictionary is not), a tag the segment does not cover, or a sealed count
   * below what the page recorded.
   */
  private int slotFor(final int tag, final long dictionaryKey, final int recordedEntryCount) {
    if (dictionaryKey <= 0 || recordedEntryCount < 0) {
      return -1;
    }
    final long segment = segmentOf(dictionaryKey);
    if (segment >= directory.segmentCount()) {
      return -1;
    }
    final SegmentDictionaryDirectoryNode.SlotTable table = directory.slots((int) segment);
    final int slot = table.slotOfTag(tag);
    if (slot < 0) {
      return -1;
    }
    return table.entryCount(slot) >= recordedEntryCount
        ? slot
        : -1;
  }

  /** Slots the directory files for {@code segment} (test observability). */
  int slotCountOf(final int segment) {
    return segment < directory.segmentCount()
        ? directory.slots(segment).slotCount()
        : 0;
  }

  /** The page's anchor is its segment plus one, because 0 is the "no dictionary" sentinel. */
  private static long segmentOf(final long dictionaryKey) {
    return dictionaryKey - 1;
  }

  @Override
  public long dictionaryKey(final int tag) {
    return 0L; // a segment anchor belongs to a PAGE; this resolver never mints one
  }

  @Override
  public int dictionaryEntryCount(final int tag) {
    return 0; // likewise: the count that matters is the one the page recorded
  }
}
