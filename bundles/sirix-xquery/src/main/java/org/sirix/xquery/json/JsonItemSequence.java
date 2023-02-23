package org.sirix.xquery.json;

import org.brackit.xquery.atomic.*;
import org.brackit.xquery.jdm.Item;
import org.brackit.xquery.jdm.Sequence;
import org.brackit.xquery.util.ExprUtil;
import org.sirix.api.json.JsonNodeTrx;

final class JsonItemSequence {

  void insert(Sequence value, JsonNodeTrx trx, final long nodeKey) {
    final Item item = ExprUtil.asItem(value);

    if (value instanceof Atomic) {
      switch (value) {
        case Str str1 -> {
          final var str = str1.stringValue();
          if (trx.getNodeKey() == nodeKey) {
            trx.insertStringValueAsFirstChild(str);
          } else {
            trx.insertStringValueAsRightSibling(str);
          }
        }
        case Null aNull -> {
          if (trx.getNodeKey() == nodeKey) {
            trx.insertNullValueAsFirstChild();
          } else {
            trx.insertNullValueAsRightSibling();
          }
        }
        case Numeric numeric -> {
          switch (value) {
            case Int anInt -> {
              if (trx.getNodeKey() == nodeKey) {
                trx.insertNumberValueAsFirstChild(((Int) value).intValue());
              } else {
                trx.insertNumberValueAsRightSibling(((Int) value).intValue());
              }
            }
            case Int32 int32 -> {
              if (trx.getNodeKey() == nodeKey) {
                trx.insertNumberValueAsFirstChild(((Int32) value).intValue());
              } else {
                trx.insertNumberValueAsRightSibling(((Int32) value).intValue());
              }
            }
            case Int64 int64 -> {
              if (trx.getNodeKey() == nodeKey) {
                trx.insertNumberValueAsFirstChild(((Int64) value).longValue());
              } else {
                trx.insertNumberValueAsRightSibling(((Int64) value).longValue());
              }
            }
            case Flt flt -> {
              if (trx.getNodeKey() == nodeKey) {
                trx.insertNumberValueAsFirstChild(((Flt) value).floatValue());
              } else {
                trx.insertNumberValueAsRightSibling(((Flt) value).floatValue());
              }
            }
            case Dbl dbl -> {
              if (trx.getNodeKey() == nodeKey) {
                trx.insertNumberValueAsFirstChild(((Dbl) value).doubleValue());
              } else {
                trx.insertNumberValueAsRightSibling(((Dbl) value).doubleValue());
              }
            }
            case Dec dec -> {
              if (trx.getNodeKey() == nodeKey) {
                trx.insertNumberValueAsFirstChild(((Dec) value).decimalValue());
              } else {
                trx.insertNumberValueAsRightSibling(((Dec) value).decimalValue());
              }
            }
            case null, default -> {
            }
          }
        }
        case Bool bool -> {
          if (trx.getNodeKey() == nodeKey) {
            trx.insertBooleanValueAsFirstChild(value.booleanValue());
          } else {
            trx.insertBooleanValueAsRightSibling(value.booleanValue());
          }
        }
        case null, default -> {
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