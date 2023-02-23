package org.sirix.index;

import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.*;
import org.brackit.xquery.expr.Cast;
import org.brackit.xquery.jdm.Type;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixRuntimeException;
import org.sirix.utils.Calc;

/**
 * 
 * @author Sebastian Baechle
 * 
 */
public final class AtomicUtil {

  // public static Field map(Type type) throws DocumentException {
  // if (!type.isBuiltin()) {
  // throw new DocumentException("%s is not a built-in type", type);
  // }
  // if (type.instanceOf(Type.STR)) {
  // return Field.STRING;
  // }
  // if (type.isNumeric()) {
  // if (type.instanceOf(Type.DBL)) {
  // return Field.DOUBLE;
  // }
  // if (type.instanceOf(Type.FLO)) {
  // return Field.FLOAT;
  // }
  // if (type.instanceOf(Type.INT)) {
  // return Field.INTEGER;
  // }
  // if (type.instanceOf(Type.LON)) {
  // return Field.LONG;
  // }
  // if (type.instanceOf(Type.INR)) {
  // return Field.BIGDECIMAL;
  // }
  // if (type.instanceOf(Type.DEC)) {
  // return Field.BIGDECIMAL;
  // }
  // }
  // throw new DocumentException("Unsupported type: %s", type);
  // }

  public static byte[] toBytes(Atomic atomic, Type type) throws SirixException {
    if (atomic == null) {
      return null;
    }
    return toBytes(toType(atomic, type));
  }

  public static byte[] toBytes(Atomic atomic) {
    if (atomic == null) {
      return null;
    }
    Type type = atomic.type();

    if (!type.isBuiltin()) {
      throw new SirixRuntimeException("%s is not a built-in type", type);
    }
    if (type.instanceOf(Type.STR)) {
      return Calc.fromString(atomic.stringValue());
    }
    if (type.instanceOf(Type.BOOL)) {
      return atomic.booleanValue() ? new byte[] { (byte) 1 } : new byte[] { (byte) 0 };
    }
    if (type.isNumeric()) {
      if (type.instanceOf(Type.DBL)) {
        return Calc.fromDouble(((Numeric) atomic).doubleValue());
      }
      if (type.instanceOf(Type.FLO)) {
        return Calc.fromFloat(((Numeric) atomic).floatValue());
      }
      if (type.instanceOf(Type.INT)) {
        return Calc.fromInt(((Numeric) atomic).intValue());
      }
      if (type.instanceOf(Type.LON)) {
        return Calc.fromLong(((Numeric) atomic).longValue());
      }
      if (type.instanceOf(Type.INR)) {
        return Calc.fromBigDecimal(((Numeric) atomic).decimalValue());
      }
      if (type.instanceOf(Type.DEC)) {
        return Calc.fromBigDecimal(((Numeric) atomic).decimalValue());
      }
    }
    throw new SirixRuntimeException("Unsupported type: %s", type);
  }

  public static Atomic fromBytes(byte[] b, Type type) {
    if (!type.isBuiltin()) {
      throw new SirixRuntimeException("%s is not a built-in type", type);
    }
    if (type.instanceOf(Type.STR)) {
      return new Str(Calc.toString(b));
    }
    if (type.instanceOf(Type.BOOL)) {
      return new Bool(b[0] == 1);
    }
    if (type.isNumeric()) {
      if (type.instanceOf(Type.DBL)) {
        return new Dbl(Calc.toDouble(b));
      }
      if (type.instanceOf(Type.FLO)) {
        return new Flt(Calc.toFloat(b));
      }
      if (type.instanceOf(Type.INT)) {
        return new Int32(Calc.toInt(b));
      }
      if (type.instanceOf(Type.LON)) {
        return new Int64(Calc.toLong(b));
      }
      if (type.instanceOf(Type.INR)) {
        return new Int(Calc.toBigDecimal(b));
      }
      if (type.instanceOf(Type.DEC)) {
        return new Dec(Calc.toBigDecimal(b));
      }
    }
    throw new SirixRuntimeException("Unsupported type: %s", type);
  }

  public static Atomic toType(Atomic atomic, Type type) {
    try {
      return Cast.cast(null, atomic, type);
    } catch (final QueryException e) {
      throw new SirixRuntimeException(e);
    }
  }
}
