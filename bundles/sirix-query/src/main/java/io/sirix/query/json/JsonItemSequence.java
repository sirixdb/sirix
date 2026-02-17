package io.sirix.query.json;

import io.brackit.query.atomic.Bool;
import io.brackit.query.atomic.Dec;
import io.brackit.query.atomic.Dbl;
import io.brackit.query.atomic.Flt;
import io.brackit.query.atomic.Int;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.Null;
import io.brackit.query.atomic.Numeric;
import io.brackit.query.atomic.Str;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.json.Array;
import io.brackit.query.jdm.json.Object;
import io.brackit.query.util.ExprUtil;
import io.sirix.access.trx.node.json.objectvalue.ArrayValue;
import io.sirix.access.trx.node.json.objectvalue.BooleanValue;
import io.sirix.access.trx.node.json.objectvalue.NullValue;
import io.sirix.access.trx.node.json.objectvalue.NumberValue;
import io.sirix.access.trx.node.json.objectvalue.ObjectValue;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.node.NodeKind;

/**
 * Utility class for JSON item operations.
 */
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

  /**
   * Replace the value at the current transaction position with a new value. Handles different node
   * types (object values, array elements).
   *
   * @param wtx the write transaction positioned at the node to replace
   * @param newValue the new value to replace with
   * @param collection the collection for creating new items
   */
  static void replaceValue(JsonNodeTrx wtx, Sequence newValue, JsonDBCollection collection) {
    final NodeKind kind = wtx.getKind();

    // For object value nodes (STRING_VALUE, BOOLEAN_VALUE, etc.),
    // we need to replace the value directly
    if (isObjectValueNode(kind)) {
      replaceObjectValue(wtx, newValue);
    } else {
      // For array elements or object/array replacement
      replaceArrayElement(wtx, newValue);
    }
  }

  private static boolean isObjectValueNode(NodeKind kind) {
    return kind == NodeKind.OBJECT_STRING_VALUE || kind == NodeKind.OBJECT_NUMBER_VALUE
        || kind == NodeKind.OBJECT_BOOLEAN_VALUE || kind == NodeKind.OBJECT_NULL_VALUE;
  }

  /**
   * Replace an object value node. If the new value has the same type as the current node, performs an
   * in-place update to preserve node identity (important for bitemporal semantics). If types differ,
   * navigates to the parent OBJECT_KEY and performs a full replacement.
   */
  private static void replaceObjectValue(JsonNodeTrx wtx, Sequence newValue) {
    final NodeKind currentKind = wtx.getKind();

    // Check if we can do an in-place update (same type)
    if (currentKind == NodeKind.OBJECT_STRING_VALUE && newValue instanceof Str str) {
      wtx.setStringValue(str.stringValue());
      return;
    }
    if (currentKind == NodeKind.OBJECT_NUMBER_VALUE && newValue instanceof Numeric) {
      setNumericValue(wtx, newValue);
      return;
    }
    if (currentKind == NodeKind.OBJECT_BOOLEAN_VALUE && newValue instanceof Bool bool) {
      wtx.setBooleanValue(bool.booleanValue());
      return;
    }
    if (currentKind == NodeKind.OBJECT_NULL_VALUE && newValue instanceof Null) {
      // Null to null - no change needed
      return;
    }

    // Types differ - need to navigate to parent OBJECT_KEY and do full replacement
    wtx.moveToParent();
    doReplaceObjectRecordValue(wtx, newValue);
  }

  /**
   * Performs in-place update of a numeric value.
   */
  private static void setNumericValue(JsonNodeTrx wtx, Sequence newValue) {
    switch (newValue) {
      case Int anInt -> wtx.setNumberValue(anInt.intValue());
      case Int32 int32 -> wtx.setNumberValue(int32.intValue());
      case Int64 int64 -> wtx.setNumberValue(int64.longValue());
      case Flt flt -> wtx.setNumberValue(flt.floatValue());
      case Dbl dbl -> wtx.setNumberValue(dbl.doubleValue());
      case Dec dec -> wtx.setNumberValue(dec.decimalValue());
      default -> {
      }
    }
  }

  /**
   * Performs a full object record value replacement (delete + insert). Must be positioned at
   * OBJECT_KEY node.
   */
  private static void doReplaceObjectRecordValue(JsonNodeTrx wtx, Sequence newValue) {
    if (newValue instanceof Array) {
      wtx.replaceObjectRecordValue(new ArrayValue());
      insertSubtree(newValue, wtx);
    } else if (newValue instanceof Object) {
      wtx.replaceObjectRecordValue(new ObjectValue());
      insertSubtree(newValue, wtx);
    } else if (newValue instanceof Str str) {
      wtx.replaceObjectRecordValue(new StringValue(str.stringValue()));
    } else if (newValue instanceof Null) {
      wtx.replaceObjectRecordValue(new NullValue());
    } else if (newValue instanceof Bool bool) {
      wtx.replaceObjectRecordValue(new BooleanValue(bool.booleanValue()));
    } else if (newValue instanceof Numeric) {
      replaceWithNumeric(wtx, newValue);
    }
  }

  private static void replaceWithNumeric(JsonNodeTrx wtx, Sequence newValue) {
    switch (newValue) {
      case Int anInt -> wtx.replaceObjectRecordValue(new NumberValue(anInt.intValue()));
      case Int32 int32 -> wtx.replaceObjectRecordValue(new NumberValue(int32.intValue()));
      case Int64 int64 -> wtx.replaceObjectRecordValue(new NumberValue(int64.longValue()));
      case Flt flt -> wtx.replaceObjectRecordValue(new NumberValue(flt.floatValue()));
      case Dbl dbl -> wtx.replaceObjectRecordValue(new NumberValue(dbl.doubleValue()));
      case Dec dec -> wtx.replaceObjectRecordValue(new NumberValue(dec.decimalValue()));
      default -> {
      }
    }
  }

  /**
   * Replace an array element. If the new value has the same type as the current node, performs an
   * in-place update to preserve node identity (important for bitemporal semantics). If types differ,
   * deletes the old node and inserts a new one at the same position.
   */
  private static void replaceArrayElement(JsonNodeTrx wtx, Sequence newValue) {
    final NodeKind currentKind = wtx.getKind();

    // Check if we can do an in-place update (same type)
    if (currentKind == NodeKind.STRING_VALUE && newValue instanceof Str str) {
      wtx.setStringValue(str.stringValue());
      return;
    }
    if (currentKind == NodeKind.NUMBER_VALUE && newValue instanceof Numeric) {
      setNumericValue(wtx, newValue);
      return;
    }
    if (currentKind == NodeKind.BOOLEAN_VALUE && newValue instanceof Bool bool) {
      wtx.setBooleanValue(bool.booleanValue());
      return;
    }
    if (currentKind == NodeKind.NULL_VALUE && newValue instanceof Null) {
      // Null to null - no change needed
      return;
    }

    // Types differ - delete old and insert new at same position
    final long leftSiblingKey = wtx.getLeftSiblingKey();
    final long parentKey = wtx.getParentKey();

    wtx.remove();

    if (leftSiblingKey != -1) {
      wtx.moveTo(leftSiblingKey);
      insertAsRightSibling(wtx, newValue);
    } else {
      wtx.moveTo(parentKey);
      insertAsFirstChild(wtx, newValue);
    }
  }

  private static void insertSubtree(Sequence value, JsonNodeTrx trx) {
    final Item item = ExprUtil.asItem(value);
    trx.insertSubtreeAsLastChild(item);
  }

  private static void insertAsRightSibling(JsonNodeTrx wtx, Sequence newValue) {
    final Item item = ExprUtil.asItem(newValue);
    wtx.insertSubtreeAsRightSibling(item);
  }

  private static void insertAsFirstChild(JsonNodeTrx wtx, Sequence newValue) {
    final Item item = ExprUtil.asItem(newValue);
    wtx.insertSubtreeAsFirstChild(item);
  }
}
