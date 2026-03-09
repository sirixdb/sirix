package io.sirix.utils;

/**
 * Lightweight replacement for {@code com.google.common.base.Preconditions}.
 * Provides zero-allocation argument and state checking methods.
 *
 * <p>Thread-safety: all methods are stateless and thread-safe.
 */
public final class Preconditions {

  private Preconditions() {
    // utility class
  }

  /**
   * Ensures the truth of an expression involving one or more parameters.
   *
   * @param expression a boolean expression
   * @throws IllegalArgumentException if {@code expression} is false
   */
  public static void checkArgument(final boolean expression) {
    if (!expression) {
      throw new IllegalArgumentException();
    }
  }

  /**
   * Ensures the truth of an expression involving one or more parameters.
   *
   * @param expression a boolean expression
   * @param errorMessage the exception message to use if the check fails
   * @throws IllegalArgumentException if {@code expression} is false
   */
  public static void checkArgument(final boolean expression, final Object errorMessage) {
    if (!expression) {
      throw new IllegalArgumentException(String.valueOf(errorMessage));
    }
  }

  /**
   * Ensures the truth of an expression involving one or more parameters.
   *
   * @param expression a boolean expression
   * @param errorMessageTemplate a template for the exception message, using {@code %s} placeholders
   * @param errorMessageArgs the arguments to substitute into the message template
   * @throws IllegalArgumentException if {@code expression} is false
   */
  public static void checkArgument(final boolean expression, final String errorMessageTemplate,
      final Object... errorMessageArgs) {
    if (!expression) {
      throw new IllegalArgumentException(format(errorMessageTemplate, errorMessageArgs));
    }
  }

  /**
   * Ensures the truth of an expression involving the state of the calling instance.
   *
   * @param expression a boolean expression
   * @throws IllegalStateException if {@code expression} is false
   */
  public static void checkState(final boolean expression) {
    if (!expression) {
      throw new IllegalStateException();
    }
  }

  /**
   * Ensures the truth of an expression involving the state of the calling instance.
   *
   * @param expression a boolean expression
   * @param errorMessage the exception message to use if the check fails
   * @throws IllegalStateException if {@code expression} is false
   */
  public static void checkState(final boolean expression, final Object errorMessage) {
    if (!expression) {
      throw new IllegalStateException(String.valueOf(errorMessage));
    }
  }

  /**
   * Ensures that an object reference is not null.
   *
   * @param reference an object reference
   * @param <T> the type of the reference
   * @return the non-null reference
   * @throws NullPointerException if {@code reference} is null
   */
  public static <T> T checkNotNull(final T reference) {
    if (reference == null) {
      throw new NullPointerException();
    }
    return reference;
  }

  /**
   * Ensures that an object reference is not null.
   *
   * @param reference an object reference
   * @param errorMessage the exception message to use if the check fails
   * @param <T> the type of the reference
   * @return the non-null reference
   * @throws NullPointerException if {@code reference} is null
   */
  public static <T> T checkNotNull(final T reference, final Object errorMessage) {
    if (reference == null) {
      throw new NullPointerException(String.valueOf(errorMessage));
    }
    return reference;
  }

  /**
   * Simple string formatting that replaces {@code %s} placeholders with arguments.
   * Avoids {@link String#format} overhead for hot paths.
   */
  private static String format(final String template, final Object... args) {
    if (args == null || args.length == 0) {
      return template;
    }
    final StringBuilder sb = new StringBuilder(template.length() + 16 * args.length);
    int argIdx = 0;
    int templateIdx = 0;
    while (templateIdx < template.length()) {
      final int placeholderIdx = template.indexOf("%s", templateIdx);
      if (placeholderIdx == -1 || argIdx >= args.length) {
        sb.append(template, templateIdx, template.length());
        break;
      }
      sb.append(template, templateIdx, placeholderIdx);
      sb.append(args[argIdx++]);
      templateIdx = placeholderIdx + 2;
    }
    // Append any remaining args
    while (argIdx < args.length) {
      sb.append(" [").append(args[argIdx++]).append(']');
    }
    return sb.toString();
  }
}
