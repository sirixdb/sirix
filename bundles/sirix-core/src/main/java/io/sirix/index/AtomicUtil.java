package io.sirix.index;

import io.brackit.query.QueryException;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.Bool;
import io.brackit.query.atomic.Dbl;
import io.brackit.query.atomic.Dec;
import io.brackit.query.atomic.Flt;
import io.brackit.query.atomic.Int;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.Numeric;
import io.brackit.query.atomic.Str;
import io.brackit.query.expr.Cast;
import io.brackit.query.jdm.Type;
import io.sirix.exception.SirixException;
import io.sirix.exception.SirixRuntimeException;
import io.sirix.utils.Calc;
import java.math.BigDecimal;
import java.math.BigInteger;
import static java.util.Objects.requireNonNull;

/**
 * 
 * @author Sebastian Baechle
 * 
 */
public final class AtomicUtil {

  public static Atomic fromNumber(final Number number) {
    return switch (requireNonNull(number)) {
      case Integer value -> new Int32(value);
      case Long value -> new Int64(value);
      case Float value -> new Flt(value);
      case Double value -> new Dbl(value);
      case BigDecimal value -> new Dec(value);
      case BigInteger value -> new Dec(new BigDecimal(value));
      default -> throw new IllegalArgumentException("Unsupported numeric representation: " + number.getClass());
    };
  }

  public static Atomic fromNumericString(final String value) {
    try {
      final BigDecimal number = new BigDecimal(requireNonNull(value));
      return number.signum() == 0 && value.startsWith("-")
          ? new Dbl(-0.0d)
          : new Dec(number);
    } catch (final NumberFormatException e) {
      return new Dbl(Double.parseDouble(value));
    }
  }

  public static boolean isExactIntegerProbe(final Atomic value) {
    if (value instanceof Int32 number) {
      final int probe = number.intValue();
      return probe >= -16_777_215 && probe <= 16_777_215;
    }
    if (value instanceof Int64 number) {
      final long probe = number.longValue();
      return probe >= -16_777_215 && probe <= 16_777_215;
    }
    return value instanceof Numeric number && value.type().instanceOf(Type.INR)
        && number.cmp(new Int32(-16_777_215)) >= 0 && number.cmp(new Int32(16_777_215)) <= 0;
  }

  public static Atomic toIndexType(final Atomic value, final IndexDef definition) {
    requireNonNull(value);
    requireNonNull(definition);
    final Type type = definition.getContentType();
    if (type.isNumeric()) {
      if (!(value instanceof Numeric)) {
        definition.markNonNumericValue();
      } else if (!type.instanceOf(Type.INR) && !value.type().instanceOf(type)) {
        definition.markIncompleteNumericCoverage();
      }
    }
    try {
      if (value instanceof Numeric number && type.instanceOf(Type.INR)) {
        final Atomic converted = toType(value, type);
        if (converted != value && number.decimalValue().compareTo(((Numeric) converted).decimalValue()) != 0) {
          throw new SirixRuntimeException("Numeric value is not exactly representable as %s", type);
        }
        return converted;
      }
      return type == Type.STR
          ? (value instanceof Str ? value : new Str(sourceString(value)))
          : toType(value instanceof Numeric ? new Str(sourceString(value)) : value, type);
    } catch (final SirixRuntimeException | NumberFormatException e) {
      definition.markIncompleteNumericCoverage();
      throw new SirixRuntimeException(e);
    }
  }

  private static String sourceString(final Atomic value) {
    if (value instanceof Dbl number) {
      return Double.toString(number.doubleValue());
    }
    if (value instanceof Flt number) {
      return Float.toString(number.floatValue());
    }
    if (value instanceof Dec number) {
      return number.decimalValue().toString();
    }
    return value.stringValue();
  }

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
      return atomic.booleanValue()
          ? new byte[] {(byte) 1}
          : new byte[] {(byte) 0};
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
    // The instant family uses the same order-preserving codec as the canonical CAS key encoding.
    if (InstantKeyCodec.isInstantType(type)) {
      return InstantKeyCodec.toBytes(atomic, type);
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
    if (InstantKeyCodec.isInstantType(type)) {
      return InstantKeyCodec.decode(b, 0, b.length, type);
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
