/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.StorageEngineReader;
import io.sirix.page.pax.GlobalStringDictionaries;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
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
 * had not been written and had no key yet, so this resolver translates through
 * {@link SegmentDictionaryAnchors} first.
 * </p>
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

  private final Int2IntMap columnByTag;

  private final SegmentDictionaryAnchors anchors;

  private final ValueReader values;

  public SegmentScopedReadDictionaries(final StorageEngineReader reader, final Int2IntMap columnByTag,
      final SegmentDictionaryAnchors anchors) {
    this(reader, columnByTag, anchors, GlobalValueDictionary::valueBytes);
  }

  SegmentScopedReadDictionaries(final StorageEngineReader reader, final Int2IntMap columnByTag,
      final SegmentDictionaryAnchors anchors, final ValueReader values) {
    this.reader = reader;
    this.columnByTag = requireNonNull(columnByTag, "columnByTag must not be null");
    this.anchors = requireNonNull(anchors, "anchors must not be null");
    this.values = requireNonNull(values, "values must not be null");
  }

  @Override
  public boolean hasDictionary(final int tag) {
    return columnByTag.containsKey(tag);
  }

  @Override
  public boolean accepts(final int tag, final long dictionaryKey, final int recordedEntryCount) {
    if (!columnByTag.containsKey(tag) || dictionaryKey <= 0 || recordedEntryCount < 0) {
      return false;
    }
    return anchors.accepts(dictionaryKey - 1, columnByTag.get(tag), recordedEntryCount);
  }

  @Override
  public int idOf(final int tag, final byte[] value, final int offset, final int length) {
    return ID_ABSENT; // decode-direction only; minting is the writer's job
  }

  @Override
  public byte @Nullable [] valueOf(final int tag, final long dictionaryKey, final int recordedEntryCount,
      final int id) {
    if (id <= 0 || id > recordedEntryCount || !accepts(tag, dictionaryKey, recordedEntryCount)) {
      return null;
    }
    final long headerKey = anchors.headerKeyOf(dictionaryKey - 1, columnByTag.get(tag));
    if (headerKey == SegmentDictionaryAnchors.NO_HEADER_KEY) {
      return null;
    }
    return values.read(headerKey, id, reader);
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
