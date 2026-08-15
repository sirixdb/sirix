package io.sirix.query.json;

import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.json.JsonItem;

/**
 * A JSON string value that a fast path computed rather than read from a node.
 *
 * <p>
 * Why this exists rather than a plain {@link Str}: brackit's serializer distinguishes the two.
 * Inside a sequence it writes a bare {@code Atomic} with {@code toString()} but routes an
 * {@code Atomic} that is also a {@link JsonItem} through its JSON writer, which quotes it. Every
 * string a query reads out of a SirixDB resource arrives as {@link AtomicStrJsonDBItem} — a
 * {@code JsonItem} — so a kernel that answers, say, {@code min(EventDate)} with a plain {@code Str}
 * would serialize {@code 2013-07-02} where the interpreter serializes {@code "2013-07-02"}. The
 * value would be right and the output still different, which is precisely what the differential
 * suites exist to prevent.
 *
 * <p>
 * Unlike {@link AtomicStrJsonDBItem} this carries no node identity, because a computed extremum has
 * none: it is a value, not a position in a document.
 */
public final class ComputedStrJsonItem extends Str implements JsonItem {

  /**
   * @param value the string value; must not be {@code null}
   */
  public ComputedStrJsonItem(final String value) {
    super(value);
  }
}
