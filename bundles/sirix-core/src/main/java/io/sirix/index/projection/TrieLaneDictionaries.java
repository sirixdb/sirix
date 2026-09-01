/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.StorageEngineReader;
import io.sirix.node.ValueDictionaryHeaderNode;
import io.sirix.page.pax.GlobalStringDictionaries;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ByteMap;
import it.unimi.dsi.fastutil.ints.Int2ByteOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * Resolves a trie-lane tag's ids against the resource-wide dictionaries the projection anchors name.
 *
 * <p>
 * One instance belongs to one transaction and does not outlive it: it holds a
 * {@link StorageEngineReader} and the {@link GlobalValueDictionary.ReadView}s it opens, and F1 of
 * the cache review applies unchanged — a page or a cache may hold the VALUE this produces, never
 * this object or the reader inside it.
 * </p>
 *
 * <h2>Resolve in ascending id order</h2>
 *
 * A dictionary point read is <b>417 ns at a random id and 75 ns at a sequential one</b>. A leaf
 * holds only about six distinct ids per tag and expansion resolves a whole tag at once, so a caller
 * that batches a tag's ids and walks them ascending pays the sequential price. This class cannot
 * enforce that — it answers one id at a time — but it is why the interface says so.
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class TrieLaneDictionaries implements GlobalStringDictionaries {

  private final StorageEngineReader reader;

  /** Tag (path node key) to the dictionary header key the projection recorded for it. */
  private final Int2LongMap anchors;

  /** Views opened so far, one per tag; a view is revision-bound and cheap to keep for a trx. */
  private final Int2ObjectMap<GlobalValueDictionary.ReadView> views = new Int2ObjectOpenHashMap<>();

  /**
   * Per tag: 0 unchecked, 1 accepted, 2 refused. A byte map rather than {@code Boolean} so a refusal
   * is remembered without boxing on a path a page walk touches once per tag.
   */
  private final Int2ByteMap accepted = new Int2ByteOpenHashMap();

  /**
   * @param reader the transaction's reader; the caller guarantees it outlives this instance
   * @param anchors tag to dictionary header key, for the tags that have one
   */
  public TrieLaneDictionaries(final StorageEngineReader reader, final Int2LongMap anchors) {
    this.reader = Objects.requireNonNull(reader, "reader must not be null");
    this.anchors = Objects.requireNonNull(anchors, "anchors must not be null");
  }

  @Override
  public boolean hasDictionary(final int tag) {
    return anchors.containsKey(tag);
  }

  @Override
  public boolean accepts(final int tag, final long dictionaryKey, final int recordedEntryCount) {
    final byte previous = accepted.get(tag);
    if (previous != 0) {
      return previous == 1;
    }
    final boolean verdict = check(tag, dictionaryKey, recordedEntryCount);
    accepted.put(tag, (byte) (verdict ? 1 : 2));
    return verdict;
  }

  private boolean check(final int tag, final long dictionaryKey, final int recordedEntryCount) {
    if (dictionaryKey <= 0L || recordedEntryCount < 0) {
      return false;
    }
    // The page must name the dictionary this tag actually resolves against. A page naming some other
    // dictionary is not a page we can read: its ids were minted by a different ranking.
    if (anchors.get(tag) != dictionaryKey) {
      return false;
    }
    final GlobalValueDictionary.ReadView view = viewOf(tag);
    if (view == null) {
      return false;
    }
    // A rank-ordered dictionary only ever APPENDS in collation order, so it cannot shrink and ids
    // 1..recordedEntryCount keep their values as it grows. A live count BELOW the recorded one is
    // therefore not a stale page, it is a different dictionary under a reused key -- and resolving
    // against it would return plausible wrong values rather than fail.
    return view.entryCount() >= recordedEntryCount;
  }

  @Override
  public int idOf(final int tag, final byte[] value, final int offset, final int length) {
    Objects.checkFromIndexSize(offset, length, value.length);
    final long anchor = anchors.getOrDefault(tag, 0L);
    if (anchor <= 0L) {
      return ID_ABSENT;
    }
    final int id = GlobalValueDictionary.probe(anchor, value, offset, length, reader);
    // ID_UNKNOWN means "the dictionary could not be read", which is not the same as "the value is
    // not there" -- but for the ENCODE direction both mean the same thing: this tag cannot be
    // written as ids, and the caller keeps its bytes.
    return id > 0 ? id : ID_ABSENT;
  }

  @Override
  public byte @Nullable [] valueOf(final int tag, final int id) {
    if (id <= 0) {
      return null;
    }
    final long anchor = anchors.getOrDefault(tag, 0L);
    if (anchor <= 0L) {
      return null;
    }
    return GlobalValueDictionary.valueBytes(anchor, id, reader);
  }

  @Override
  public long dictionaryKey(final int tag) {
    return anchors.getOrDefault(tag, 0L);
  }

  @Override
  public int dictionaryEntryCount(final int tag) {
    final GlobalValueDictionary.ReadView view = viewOf(tag);
    return view == null ? 0 : view.entryCount();
  }

  private GlobalValueDictionary.@Nullable ReadView viewOf(final int tag) {
    final GlobalValueDictionary.ReadView cached = views.get(tag);
    if (cached != null) {
      return cached;
    }
    final long anchor = anchors.getOrDefault(tag, 0L);
    if (anchor <= 0L) {
      return null;
    }
    final GlobalValueDictionary.ReadView view = GlobalValueDictionary.readView(anchor, reader);
    if (view != null) {
      views.put(tag, view);
    }
    return view;
  }

  /** Anchors keyed by tag, for a caller that has the projection's column anchors and its paths. */
  public static Int2LongMap anchorsOf(final int[] tags, final long[] headerKeys) {
    if (tags.length != headerKeys.length) {
      throw new IllegalArgumentException(
          "tags and header keys must be index-aligned: " + tags.length + " vs " + headerKeys.length);
    }
    final Int2LongMap anchors = new Int2LongOpenHashMap(tags.length);
    for (int i = 0; i < tags.length; i++) {
      if (headerKeys[i] > 0L) {
        anchors.put(tags[i], headerKeys[i]);
      }
    }
    return anchors;
  }

  @Override
  public String toString() {
    return "TrieLaneDictionaries[tags=" + anchors.keySet() + ']';
  }

  /** Only so a caller can log what a refusal was about; never part of a decision. */
  public String describe(final int tag) {
    final ValueDictionaryHeaderNode header =
        GlobalValueDictionary.header(anchors.getOrDefault(tag, 0L), reader);
    return header == null
        ? "tag " + tag + " has no readable dictionary"
        : "tag " + tag + " -> dictionary " + anchors.get(tag) + " with " + header.getEntryCount() + " entries";
  }

  /** The empty resolver: every tag keeps its bytes. */
  public static final GlobalStringDictionaries NONE = new GlobalStringDictionaries() {
    @Override
    public boolean hasDictionary(final int tag) {
      return false;
    }

    @Override
    public boolean accepts(final int tag, final long dictionaryKey, final int recordedEntryCount) {
      return false;
    }

    @Override
    public int idOf(final int tag, final byte[] value, final int offset, final int length) {
      return ID_ABSENT;
    }

    @Override
    public byte @Nullable [] valueOf(final int tag, final int id) {
      return null;
    }

    @Override
    public long dictionaryKey(final int tag) {
      return 0L;
    }

    @Override
    public int dictionaryEntryCount(final int tag) {
      return 0;
    }
  };

}
