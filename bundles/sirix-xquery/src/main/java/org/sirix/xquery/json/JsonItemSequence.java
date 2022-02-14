package org.sirix.xquery.json;

import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.Bool;
import org.brackit.xquery.atomic.Dbl;
import org.brackit.xquery.atomic.Dec;
import org.brackit.xquery.atomic.Flt;
import org.brackit.xquery.atomic.Int;
import org.brackit.xquery.atomic.Int32;
import org.brackit.xquery.atomic.Int64;
import org.brackit.xquery.atomic.Null;
import org.brackit.xquery.atomic.Numeric;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.util.ExprUtil;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Sequence;
import org.sirix.api.json.JsonNodeTrx;

final class JsonItemSequence {

  void insert(Sequence value, JsonNodeTrx trx, final long nodeKey) {
    final Item item = ExprUtil.asItem(value);

    if (value instanceof Atomic) {
      if (value instanceof Str) {
        final var str = ((Str) value).stringValue();
        if (trx.getNodeKey() == nodeKey) {
          trx.insertStringValueAsFirstChild(str);
        } else {
          trx.insertStringValueAsRightSibling(str);
        }
      } else if (value instanceof Null) {
        if (trx.getNodeKey() == nodeKey) {
          trx.insertNullValueAsFirstChild();
        } else {
          trx.insertNullValueAsRightSibling();
        }
      } else if (value instanceof Numeric) {
        if (value instanceof Int) {
          if (trx.getNodeKey() == nodeKey) {
            trx.insertNumberValueAsFirstChild(((Int) value).intValue());
          } else {
            trx.insertNumberValueAsRightSibling(((Int) value).intValue());
          }
        } else if (value instanceof Int32) {
          if (trx.getNodeKey() == nodeKey) {
            trx.insertNumberValueAsFirstChild(((Int32) value).intValue());
          } else {
            trx.insertNumberValueAsRightSibling(((Int32) value).intValue());
          }
        } else if (value instanceof Int64) {
          if (trx.getNodeKey() == nodeKey) {
            trx.insertNumberValueAsFirstChild(((Int64) value).longValue());
          } else {
            trx.insertNumberValueAsRightSibling(((Int64) value).longValue());
          }
        } else if (value instanceof Flt) {
          if (trx.getNodeKey() == nodeKey) {
            trx.insertNumberValueAsFirstChild(((Flt) value).floatValue());
          } else {
            trx.insertNumberValueAsRightSibling(((Flt) value).floatValue());
          }
        } else if (value instanceof Dbl) {
          if (trx.getNodeKey() == nodeKey) {
            trx.insertNumberValueAsFirstChild(((Dbl) value).doubleValue());
          } else {
            trx.insertNumberValueAsRightSibling(((Dbl) value).doubleValue());
          }
        } else if (value instanceof Dec) {
          if (trx.getNodeKey() == nodeKey) {
            trx.insertNumberValueAsFirstChild(((Dec) value).decimalValue());
          } else {
            trx.insertNumberValueAsRightSibling(((Dec) value).decimalValue());
          }
        }
      } else if (value instanceof Bool) {
        if (trx.getNodeKey() == nodeKey) {
          trx.insertBooleanValueAsFirstChild(value.booleanValue());
        } else {
          trx.insertBooleanValueAsRightSibling(value.booleanValue());
        }
      }
    } else {
      if (trx.getNodeKey() == nodeKey) {
        trx.insertSubtreeAsFirstChild(item, JsonNodeTrx.Commit.NO);
      } else {
        trx.insertSubtreeAsRightSibling(item, JsonNodeTrx.Commit.NO);
      }
    }
  }
}