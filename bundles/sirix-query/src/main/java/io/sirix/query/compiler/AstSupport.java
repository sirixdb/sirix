package io.sirix.query.compiler;

import io.brackit.query.compiler.AST;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/** Own array annotations when copying plans; expose their contents in diagnostics. */
public final class AstSupport {
  // AST.getProperties() exposes an unmodifiable view and throws when the backing map is null.
  // A constant handle avoids exceptions and wrapper allocations while inspecting our own copies.
  private static final VarHandle PROPERTIES;

  static {
    try {
      PROPERTIES = MethodHandles.privateLookupIn(AST.class, MethodHandles.lookup())
                                .findVarHandle(AST.class, "properties", Map.class);
    } catch (final ReflectiveOperationException exception) {
      throw new ExceptionInInitializerError(exception);
    }
  }

  private AstSupport() {
    throw new AssertionError("no instances");
  }

  /** Brackit's tree copy owns property maps but retains their array values. */
  public static AST copyTree(final AST source) {
    final AST copy = Objects.requireNonNull(source).copyTree();
    detachArrays(copy, false);
    return copy;
  }

  /** Render boolean annotations by value without modifying the executable plan. */
  public static String toJSON(final AST source) {
    final AST copy = Objects.requireNonNull(source).copyTree();
    detachArrays(copy, true);
    return copy.toJSON();
  }

  private static void detachArrays(final AST node, final boolean diagnostic) {
    node.setValue(copyArray(node.getValue()));
    final Map<String, Object> properties = properties(node);
    if (properties != null && !properties.isEmpty()) {
      if (diagnostic) {
        properties.replaceAll((name, value) -> value instanceof boolean[] flags
            ? Arrays.toString(flags)
            : copyArray(value));
      } else {
        properties.replaceAll((name, value) -> copyArray(value));
      }
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      detachArrays(node.getChild(i), diagnostic);
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> properties(final AST node) {
    return (Map<String, Object>) PROPERTIES.get(node);
  }

  private static Object copyArray(final Object value) {
    if (value == null || !value.getClass().isArray())
      return value;
    if (value instanceof Object[] array) {
      final Object[] copy = array.clone();
      for (int i = 0; i < copy.length; i++)
        copy[i] = copyArray(copy[i]);
      return copy;
    }
    if (value instanceof boolean[] array)
      return array.clone();
    if (value instanceof int[] array)
      return array.clone();
    if (value instanceof long[] array)
      return array.clone();
    if (value instanceof double[] array)
      return array.clone();
    if (value instanceof byte[] array)
      return array.clone();
    if (value instanceof float[] array)
      return array.clone();
    if (value instanceof short[] array)
      return array.clone();
    return ((char[]) value).clone();
  }
}
