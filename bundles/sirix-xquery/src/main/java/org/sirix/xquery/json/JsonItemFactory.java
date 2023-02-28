package org.sirix.xquery.json;

import org.brackit.xquery.atomic.*;
import org.brackit.xquery.jdm.json.JsonItem;
import org.sirix.api.json.JsonNodeReadOnlyTrx;

import java.math.BigDecimal;

public final class JsonItemFactory {
  public JsonItemFactory() {}

  public JsonItem getSequence(final JsonNodeReadOnlyTrx rtx, final JsonDBCollection collection) {
    switch (rtx.getKind()) {
      case ARRAY:
        return new JsonDBArray(rtx, collection);
      case OBJECT:
        return new JsonDBObject(rtx, collection);
      case OBJECT_KEY:
        return new AtomicStrJsonDBItem(rtx, collection, rtx.getName().getLocalName());
      case STRING_VALUE:
      case OBJECT_STRING_VALUE:
        return new AtomicStrJsonDBItem(rtx, collection, rtx.getValue());
      case BOOLEAN_VALUE:
      case OBJECT_BOOLEAN_VALUE:
        return new AtomicBooleanJsonDBItem(rtx, collection, new Bool(rtx.getBooleanValue()));
      case OBJECT_NULL_VALUE:
      case NULL_VALUE:
        return new AtomicNullJsonDBItem(rtx, collection);
      case OBJECT_NUMBER_VALUE:
      case NUMBER_VALUE:
        final Number number = rtx.getNumberValue();

        if (number instanceof Integer) {
          return new NumericJsonDBItem(rtx, collection, new Int32(number.intValue()));
        } else if (number instanceof Long) {
          return new NumericJsonDBItem(rtx, collection, new Int64(number.intValue()));
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
