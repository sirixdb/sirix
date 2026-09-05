/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.page.pax.GlobalStringDictionaries;

import java.nio.charset.StandardCharsets;

import static java.util.Objects.requireNonNull;

/**
 * The projection build's view of ONE {@code (segment, column)} dictionary — the same dictionary the
 * document pages of that segment mint into.
 *
 * <p>
 * A projection row holds the bytes of the document node it was extracted from, so the two sides are
 * interning the same value set. Sharing one dictionary is therefore not an optimisation but the
 * absence of a duplicate: the strings are written once, by the seal, and both a document page and a
 * projection leaf store the same id for the same value. A projection leaf then carries no dictionary
 * of its own at all, which is where a per-leaf dictionary's bytes go.
 * </p>
 *
 * <p>
 * One instance per (segment, column) per leaf conversion; it holds no state beyond the two indices,
 * so a caller may make one per leaf without thinking about it.
 * </p>
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
final class SegmentColumnEncoder implements GlobalValueDictionaryEncoder {

  private final SegmentScopedDictionaries dictionaries;

  private final int segment;

  private final int column;

  SegmentColumnEncoder(final SegmentScopedDictionaries dictionaries, final int segment, final int column) {
    this.dictionaries = requireNonNull(dictionaries, "dictionaries must not be null");
    if (segment < 0) {
      throw new IllegalArgumentException("segment must not be negative: " + segment);
    }
    if (column < 0) {
      throw new IllegalArgumentException("column must not be negative: " + column);
    }
    this.segment = segment;
    this.column = column;
  }

  @Override
  public int intern(final byte[] source, final int offset, final int length) {
    final int id = dictionaries.idIn(segment, column, source, offset, length);
    if (id == GlobalStringDictionaries.ID_ABSENT) {
      // The seal cannot persist a value this long, so an id for it would name nothing. The caller
      // converts a whole column at a time and cannot fall back per value, so this is refused rather
      // than silently written as an absent cell.
      throw new IllegalStateException("segment " + segment + " column " + column + " was asked to intern a "
          + length + "-byte value, above what a dictionary entry can hold");
    }
    return id;
  }

  @Override
  public int intern(final String value) {
    final byte[] utf8 = requireNonNull(value, "value must not be null").getBytes(StandardCharsets.UTF_8);
    return intern(utf8, 0, utf8.length);
  }

  /** The segment whose dictionary this encoder mints into. */
  int segment() {
    return segment;
  }
}
