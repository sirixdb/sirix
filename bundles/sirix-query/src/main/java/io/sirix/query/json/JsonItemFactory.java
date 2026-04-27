package io.sirix.query.json;

import io.brackit.query.atomic.Bool;
import io.brackit.query.atomic.Dbl;
import io.brackit.query.atomic.Dec;
import io.brackit.query.atomic.Flt;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Int64;
import io.brackit.query.jdm.json.JsonItem;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.node.NodeKind;

import java.math.BigDecimal;

public final class JsonItemFactory {
  public JsonItemFactory() {}

  /**
   * Build a name-mode item that always represents the OBJECT_KEY (or fused OBJECT_NAMED_*)
   * field NAME as a string, regardless of whether the underlying record is fused.
   *
   * <p>The generic {@link #getSequence(JsonNodeReadOnlyTrx, JsonDBCollection)} method returns
   * the VALUE for fused records — callers that iterate {@code obj.names()} (e.g. {@code
   * bit:fields($obj)}) must use this method instead so the iteration yields field names.
   */
  public JsonItem getNameSequence(final JsonNodeReadOnlyTrx rtx, final JsonDBCollection collection) {
    final NodeKind kind = rtx.getKind();
    if (kind.playsObjectKeyRole()) {
      return new AtomicStrJsonDBItem(rtx, collection, rtx.getName().getLocalName());
    }
    throw new IllegalStateException("getNameSequence called on a non-object-key node: " + kind);
  }

  public JsonItem getSequence(final JsonNodeReadOnlyTrx rtx, final JsonDBCollection collection) {
    switch (rtx.getKind()) {
      case ARRAY:
        return new JsonDBArray(rtx, collection);
      case OBJECT:
        return new JsonDBObject(rtx, collection);
      // iter#32 Phase 2: fused structural OBJECT_NAMED_OBJECT/ARRAY play the OBJECT/ARRAY
      // role under fusion — return the matching DB view anchored at the fused record.
      case OBJECT_NAMED_OBJECT:
        return new JsonDBObject(rtx, collection);
      case OBJECT_NAMED_ARRAY:
        return new JsonDBArray(rtx, collection);
      // iter#31 Option B: fused OBJECT_NAMED_* is a LEAF carrying BOTH name and inline
      // primitive value. When the factory is asked to turn the current cursor position
      // into a JsonItem, it's being asked for the VALUE — the caller has already resolved
      // the name (via filter axes or direct lookup). Return the inline primitive as its
      // typed JSON atomic item.
      case OBJECT_NAMED_STRING:
        return new AtomicStrJsonDBItem(rtx, collection, rtx.getValue());
      case OBJECT_NAMED_BOOLEAN:
        return new AtomicBooleanJsonDBItem(rtx, collection, new Bool(rtx.getBooleanValue()));
      case OBJECT_NAMED_NULL:
        return new AtomicNullJsonDBItem(rtx, collection);
      case OBJECT_NAMED_NUMBER: {
        final Number fusedNumber = rtx.getNumberValue();
        if (fusedNumber instanceof Integer) {
          return new NumericJsonDBItem(rtx, collection, new Int32(fusedNumber.intValue()));
        } else if (fusedNumber instanceof Long) {
          return new NumericJsonDBItem(rtx, collection, new Int64(fusedNumber.longValue()));
        } else if (fusedNumber instanceof Float) {
          return new NumericJsonDBItem(rtx, collection, new Flt(fusedNumber.floatValue()));
        } else if (fusedNumber instanceof Double) {
          return new NumericJsonDBItem(rtx, collection, new Dbl(fusedNumber.doubleValue()));
        } else if (fusedNumber instanceof BigDecimal bd) {
          return new NumericJsonDBItem(rtx, collection, new Dec(bd));
        }
        throw new AssertionError();
      }
      case STRING_VALUE:
        return new AtomicStrJsonDBItem(rtx, collection, rtx.getValue());
      case BOOLEAN_VALUE:
        return new AtomicBooleanJsonDBItem(rtx, collection, new Bool(rtx.getBooleanValue()));
      case NULL_VALUE:
        return new AtomicNullJsonDBItem(rtx, collection);
      case NUMBER_VALUE:
        final Number number = rtx.getNumberValue();

        if (number instanceof Integer) {
          return new NumericJsonDBItem(rtx, collection, new Int32(number.intValue()));
        } else if (number instanceof Long) {
          return new NumericJsonDBItem(rtx, collection, new Int64(number.longValue()));
        } else if (number instanceof Float) {
          return new NumericJsonDBItem(rtx, collection, new Flt(number.floatValue()));
        } else if (number instanceof Double) {
          return new NumericJsonDBItem(rtx, collection, new Dbl(number.doubleValue()));
        } else if (number instanceof BigDecimal) {
          return new NumericJsonDBItem(rtx, collection, new Dec((BigDecimal) number));
        }
        // $CASES-OMITTED$
      default:
        throw new AssertionError();
    }
  }
}
