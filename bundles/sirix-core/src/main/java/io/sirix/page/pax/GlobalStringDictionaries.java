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
 * records live in the {@code NamePage} sub-trie, so a record page's decode would recurse into
 * page decodes of its own.
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
   * The id {@code value} is stored under for {@code tag}, for the ENCODE direction.
   *
   * <p>
   * Returns {@link #ID_ABSENT} when the dictionary does not hold the value, which a caller must
   * treat as "this tag cannot be written as ids on this page" rather than minting anything: the
   * dictionary is complete before the load begins, so a miss means the writer and the pre-pass
   * disagree about the value set, and an id no reader can resolve is worse than the bytes.
   * </p>
   */
  int idOf(int tag, byte[] value, int offset, int length);

  /**
   * The bytes stored under {@code id} for {@code tag}, for the DECODE direction.
   *
   * <p>
   * {@code null} when the id cannot be resolved — a torn or absent dictionary — which callers must
   * surface rather than substitute, because an empty string and an unresolvable id are different
   * answers and only one of them is a value.
   * </p>
   */
  byte @Nullable [] valueOf(int tag, int id);
}
