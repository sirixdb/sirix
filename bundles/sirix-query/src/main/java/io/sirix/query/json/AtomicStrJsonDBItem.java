package io.sirix.query.json;

import io.brackit.query.atomic.AbstractTimeInstant;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.Str;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.StructuredDBItem;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;

import static java.util.Objects.requireNonNull;

/**
 * A string read from a Sirix document, keeping its node identity.
 *
 * <p>Sealed with one marker subclass, {@link FieldNameJsonDBItem}, for an object field NAME: under
 * record fusion one node holds a field's name and its value, so the node key alone cannot tell
 * which of the two an item stands for when temporal navigation re-reads it in another revision. A
 * type rather than a flag keeps every string value at its existing instance size (the fields are
 * densely packed, so one more boolean would add 8 bytes per materialized string); all methods are
 * {@code final}, so call sites stay monomorphic for values.
 */
public sealed class AtomicStrJsonDBItem extends Str implements JsonDBItem, StructuredDBItem<JsonNodeReadOnlyTrx>
    permits FieldNameJsonDBItem {

  /** Sirix {@link JsonNodeReadOnlyTrx}. */
  private final JsonNodeReadOnlyTrx rtx;

  /** Immutable per-field memo. Non-fixed layouts retain the general parser. */
  private final long epochMillis;
  private @Nullable DateTime dateTime;

  /** Sirix node key. */
  private final long nodeKey;

  /** Collection this node is part of. */
  private final JsonDBCollection collection;

  /**
   * Constructor.
   *
   * @param rtx {@link JsonNodeReadOnlyTrx} for providing reading access to the underlying node
   * @param collection {@link JsonDBCollection} reference
   * @param string the atomic string value delegate
   */
  public AtomicStrJsonDBItem(final JsonNodeReadOnlyTrx rtx, final JsonDBCollection collection, final String string) {
    this(rtx, collection, string, StoredDateTimeParser.NOT_FIXED_UTC);
  }

  AtomicStrJsonDBItem(final JsonNodeReadOnlyTrx rtx, final JsonDBCollection collection, final byte[] utf8) {
    this(rtx, collection, new String(requireNonNull(utf8), StandardCharsets.UTF_8),
        StoredDateTimeParser.epochMillis(utf8, 0, utf8.length));
  }

  AtomicStrJsonDBItem(final JsonNodeReadOnlyTrx rtx, final JsonDBCollection collection, final String string,
      final long epochMillis) {
    super(string);
    this.epochMillis = epochMillis;
    this.collection = requireNonNull(collection);
    this.rtx = requireNonNull(rtx);
    nodeKey = this.rtx.getNodeKey();
  }

  /** Epoch millis for fixed UTC input, or {@link StoredDateTimeParser#NOT_FIXED_UTC}. */
  public final long epochMillis() {
    return epochMillis;
  }

  /** Parse once for this immutable string snapshot; never memoize an error. */
  public final DateTime dateTime() {
    DateTime parsed = dateTime;
    if (parsed == null) {
      final String text = stringValue();
      if (epochMillis == StoredDateTimeParser.NOT_FIXED_UTC) {
        parsed = new DateTime(text);
      } else {
        final short year = (short) ((text.charAt(0) - '0') * 1000 + (text.charAt(1) - '0') * 100
            + (text.charAt(2) - '0') * 10 + text.charAt(3) - '0');
        parsed = new DateTime(year, (byte) digits2(text, 5), (byte) digits2(text, 8), (byte) digits2(text, 11),
            (byte) digits2(text, 14), digits2(text, 17) * 1_000_000, AbstractTimeInstant.UTC_TIMEZONE);
      }
      dateTime = parsed;
    }
    return parsed;
  }

  private static int digits2(final String text, final int offset) {
    return (text.charAt(offset) - '0') * 10 + text.charAt(offset + 1) - '0';
  }

  private void moveRtx() {
    rtx.moveTo(nodeKey);
  }

  @Override
  public final JsonResourceSession getResourceSession() {
    return rtx.getResourceSession();
  }

  @Override
  public final JsonNodeReadOnlyTrx getTrx() {
    moveRtx();

    return rtx;
  }

  @Override
  public final JsonDBCollection getCollection() {
    return collection;
  }

  @Override
  public final long getNodeKey() {
    return nodeKey;
  }
}
