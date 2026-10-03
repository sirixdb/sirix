package io.sirix.query.compiler.expression;

import io.brackit.query.QueryContext;
import io.brackit.query.Tuple;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.Bits;
import io.brackit.query.compiler.translator.Binding;
import io.brackit.query.compiler.translator.VariableTable;
import io.sirix.query.json.BasicJsonDBStore;
import java.util.Arrays;
import java.util.Objects;

/** Checks scalar inputs without traversing containers or resolving unbound defaults. */
public final class ConjunctInputs {
  private static final Object UNBOUND = new Object();
  private final QNm[] names;
  private final boolean[] defaults;
  private final int[] positions;
  private final boolean nativeStore;

  public ConjunctInputs(final QNm[] inputs, final QNm[] captured, final QNm[] defaultNames, final VariableTable table,
      final boolean nativeStore) {
    this.nativeStore = nativeStore;
    Objects.requireNonNull(inputs);
    Objects.requireNonNull(captured);
    names = Arrays.copyOf(inputs, inputs.length + captured.length);
    System.arraycopy(captured, 0, names, inputs.length, captured.length);
    Objects.requireNonNull(defaultNames);
    defaults = new boolean[names.length];
    positions = new int[names.length];
    Arrays.fill(positions, -1);
    for (final QNm name : defaultNames) {
      Objects.requireNonNull(name);
    }
    final Binding[] bindings = table == null
        ? null
        : table.bound();
    for (int i = 0; i < names.length; i++) {
      Objects.requireNonNull(names[i]);
      for (final QNm name : defaultNames) {
        if (i < inputs.length && !name.equals(Bits.FS_DOT) && names[i].equals(name)) {
          defaults[i] = true;
          break;
        }
      }
      if (i >= inputs.length) {
        positions[i] = -2;
      }
      if (i >= inputs.length && table != null) {
        for (final Binding binding : bindings) {
          if (names[i].equals(binding.getName())) {
            final int index = i;
            table.resolve(names[i], position -> positions[index] = position);
            break;
          }
        }
      }
    }
  }

  boolean admit(final QueryContext context, final Tuple tuple) {
    // Only the final stock store guarantees freshly opened document views. A caller's
    // provider may return an already exposed object with changing lazy field values.
    if (nativeStore && !(context.getJsonItemStore() instanceof BasicJsonDBStore)) {
      return false;
    }
    for (int i = 0; i < names.length; i++) {
      if (!repeatable(value(context, tuple, i), i)) {
        return false;
      }
    }
    return true;
  }

  private Object value(final QueryContext context, final Tuple tuple, final int index) {
    if (positions[index] >= 0) {
      return tuple.get(positions[index]);
    }
    if (positions[index] == -2) {
      return UNBOUND;
    }
    final QNm name = names[index];
    if (name.equals(Bits.FS_DOT)) {
      return UNBOUND;
    }
    return context.isBound(name)
        ? context.resolve(name)
        : UNBOUND;
  }

  private boolean repeatable(final Object value, final int index) {
    return value == UNBOUND
        ? defaults[index]
        : value == null || value instanceof Atomic;
  }
}
