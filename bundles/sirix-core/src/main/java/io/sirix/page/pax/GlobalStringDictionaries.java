/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import org.jspecify.annotations.Nullable;

/**
 * Resolves a tag's string values against a resource-wide dictionary, in both directions.
 *
 * <h2>Why the page cannot do this itself</h2>
 *
 * A {@link StringRegion} tag whose column has a resource-wide rank-ordered dictionary stores its
 * per-tag dictionary as IDS rather than value bytes — which is the whole of the trie-lane lever:
 * 97.9 % of a converted tag's region bytes are the value bytes it no longer keeps. But the page
 * layer cannot reach that dictionary. {@code deserializePage} is handed a
 * {@code ResourceConfiguration} and no reader, and giving it one would not help: the dictionary's
 * records live in the {@code NamePage} sub-trie, so a record page's decode would recurse into page
 * decodes of its own.
 *
 * <p>
 * So the resolver arrives the way the FSST symbol table already does — from the component that
 * holds a reader, handed to the page after it is decoded and before anything asks for a value.
 * {@code NodeStorageEngineReader} resolves the FSST table from the {@code NamePage}, caches it by
 * id and injects it into the record; this is the same seam for the same reason, and page expansion
 * being lazy is what makes "after decode" early enough.
 * </p>
 *
 * <h2>Resolve in id order</h2>
 *
 * A dictionary point read costs <b>417 ns at a random id and 75 ns at a sequential one</b> — a 320×
 * spread that is a property of block residency, not of the lookup. A leaf holds only about six
 * distinct ids per converted tag and expansion resolves a whole tag at once, so an implementation
 * that batches a tag's ids and walks them ASCENDING pays the sequential price. That is a constraint
 * on callers, not an optimisation: resolving a tag's ids in arbitrary order gives up 320× for
 * nothing.
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public interface GlobalStringDictionaries {

  /** Answer of {@link #idOf} when the dictionary provably does not hold the value. */
  int ID_ABSENT = 0;

  /**
   * Whether this tag's values are stored as dictionary ids rather than bytes.
   *
   * <p>
   * Asked once per tag per page, never per value: a tag is converted or it is not, and the answer
   * cannot change within a revision.
   * </p>
   *
   * @param tag the region's tag — a path node key under the pathNodeKey-tagged layout
   */
  boolean hasDictionary(int tag);

  /**
   * Bind to the dictionary a PAGE names, or refuse — the temporal-validity check.
   *
   * <p>
   * Called ONCE per tag per page, before any {@link #valueOf}, with the anchor the page recorded. It
   * is the check the anchor exists for, and it does not live in the parse: parse can only compare the
   * recorded count against the ids in THAT LEAF (about six of them), which is true for essentially
   * any corrupt value. Only a resolver holds the live dictionary and can ask the question that
   * matters.
   * </p>
   *
   * <p>
   * Refuse when the named dictionary is unreadable, when it is not the one this tag resolves against,
   * or when its live entry count is BELOW the recorded one. That last case is a different dictionary
   * under a reused key: a rank-ordered dictionary only ever appends, so it cannot shrink, and a
   * smaller live count means the key was reused by something else. Ids resolved against it would be
   * plausible and wrong.
   * </p>
   *
   * <p>
   * A refusal is not an error to swallow. The caller has a page whose values it cannot read, and
   * substituting anything — empty strings, the bytes at that id, the current dictionary's answer —
   * turns an unreadable page into a wrong one.
   * </p>
   *
   * @param tag the region tag
   * @param dictionaryKey the dictionary node key the page recorded
   * @param recordedEntryCount the dictionary's entry count when the page was written
   * @return whether ids under {@code tag} may now be resolved
   */
  boolean accepts(int tag, long dictionaryKey, int recordedEntryCount);

  /**
   * The id {@code value} is stored under for {@code tag}, for the ENCODE direction.
   *
   * <p>
   * Returns {@link #ID_ABSENT} when the dictionary does not hold the value, which a caller must treat
   * as "this tag cannot be written as ids on this page" rather than minting anything: the dictionary
   * is complete before the load begins, so a miss means the writer and the pre-pass disagree about
   * the value set, and an id no reader can resolve is worse than the bytes.
   * </p>
   */
  int idOf(int tag, byte[] value, int offset, int length);

  /**
   * The bytes stored under {@code id} for {@code tag}, for the DECODE direction.
   *
   * <p>
   * <b>It takes the page's anchor and performs {@link #accepts} itself.</b> Not as a convenience — an
   * ordering requirement that a caller must remember is the same shape as a check that lives only in
   * prose, which is how the temporal-validity rule went missing the first time. Passing the anchor is
   * the only way to ask for a value, so the check cannot be skipped by forgetting it.
   * </p>
   *
   * <p>
   * {@code null} when the id cannot be resolved — a refused anchor, a torn or absent dictionary —
   * which callers must surface rather than substitute, because an empty string and an unresolvable id
   * are different answers and only one of them is a value.
   * </p>
   */
  byte @Nullable [] valueOf(int tag, long dictionaryKey, int recordedEntryCount, int id);

  /**
   * The node key of the dictionary {@code tag} resolves against, so the page can NAME it.
   *
   * <p>
   * Written into the region beside the ids, and the reason the trie lane is safe at all. A dictionary
   * is a function of (resource, generation), not of the page, and a rank rebuild REASSIGNS every id —
   * so a copy-on-write leaf written against one generation and still reachable after the next would
   * resolve its ids against the wrong dictionary and return plausible wrong values for a page nobody
   * touched. Naming the dictionary makes resolution a function of the page again, which is the
   * property that lets FSST pages cache their symbol table safely.
   * </p>
   */
  long dictionaryKey(int tag);

  /**
   * The dictionary's entry count at encode time — the freshness half of the anchor.
   *
   * <p>
   * A rank-ordered dictionary only ever APPENDS in collation order, so ids {@code 1..n} keep their
   * values as it grows: a live count at least this one means every id the page stores is still the
   * value it stored. A SMALLER live count is a different dictionary under a reused key and must be
   * refused rather than resolved. It does not by itself exclude a rebuild that lands on the same key
   * with at least as many entries — the key changing on rebuild is what closes that, and this is the
   * second line rather than the first.
   * </p>
   */
  int dictionaryEntryCount(int tag);
}
